//! Media download support for WhatsApp messages.
//!
//! Requires the `download` feature to be enabled. Provides [`Client::download`]
//! for fetching and decrypting media from WhatsApp's CDN.

use std::{
    io::{Cursor, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::anyhow;
use wacore::{
    download::{
        DownloadRequest, DownloadUtils, Downloadable, MediaConnection as CoreMediaConnection,
        MediaDecryption,
    },
    iq::mediaconn::MediaConnSpec,
};

use crate::{Client, error::ClientError};

/// Pre-allocated buffer cap for in-memory downloads (64 MB).
const DOWNLOAD_PREALLOC_CAP: u64 = 64 * 1024 * 1024;

/// Number of retry attempts after a media auth error (401/403).
const MEDIA_AUTH_REFRESH_RETRY_ATTEMPTS: usize = 1;

/// Cached media connection with TTL-aware expiry.
#[derive(Debug, Clone)]
pub(crate) struct MediaConn {
    pub(crate) auth: String,
    pub(crate) ttl: u64,
    pub(crate) auth_ttl: Option<u64>,
    pub(crate) hosts: Vec<wacore::iq::mediaconn::MediaConnHost>,
    pub(crate) fetched_at: Instant,
}

impl MediaConn {
    /// Whether the connection info has expired.
    pub(crate) fn is_expired(&self) -> bool {
        let effective_ttl = self.auth_ttl.map_or(self.ttl, |at| self.ttl.min(at));
        self.fetched_at.elapsed() > Duration::from_secs(effective_ttl)
    }
}

impl From<&MediaConn> for CoreMediaConnection {
    fn from(conn: &MediaConn) -> Self {
        CoreMediaConnection {
            hosts: conn
                .hosts
                .iter()
                .map(|h| wacore::download::MediaHost {
                    hostname: h.hostname.clone(),
                })
                .collect(),
            auth: conn.auth.clone(),
        }
    }
}

/// Owned download parameters for re-downloading media without the original
/// message in hand. Implements [`Downloadable`] so it can be passed to
/// [`Client::download`].
#[derive(Debug, Clone)]
pub struct DownloadParams {
    /// CDN direct path (e.g. `/mms/image/...`).
    pub direct_path: PathBuf,
    /// E2E media key for decryption. `None` for unencrypted media.
    pub media_key: Option<Vec<u8>>,
    /// SHA-256 hash of the plaintext file.
    pub file_sha256: Vec<u8>,
    /// SHA-256 hash of the encrypted file (used as URL token).
    pub file_enc_sha256: Option<Vec<u8>>,
    /// Declared plaintext file length in bytes.
    pub file_length: u64,
    /// Media type (image, video, audio, etc.).
    pub media_type: wacore::download::MediaType,
}

impl DownloadParams {
    /// Create params for encrypted media.
    #[must_use]
    pub fn encrypted<P: AsRef<Path>>(
        direct_path: P,
        media_key: &[u8],
        file_sha256: &[u8],
        file_enc_sha256: &[u8],
        file_length: u64,
        media_type: wacore::download::MediaType,
    ) -> Self {
        Self {
            direct_path: direct_path.as_ref().to_path_buf(),
            media_key: Some(media_key.to_vec()),
            file_sha256: file_sha256.to_vec(),
            file_enc_sha256: Some(file_enc_sha256.to_vec()),
            file_length,
            media_type,
        }
    }

    /// Create params for unencrypted media (e.g. newsletter images).
    #[must_use]
    pub fn plaintext<P: AsRef<Path>>(
        direct_path: P,
        file_sha256: &[u8],
        file_length: u64,
        media_type: wacore::download::MediaType,
    ) -> Self {
        Self {
            direct_path: direct_path.as_ref().to_path_buf(),
            media_key: None,
            file_sha256: file_sha256.to_vec(),
            file_enc_sha256: None,
            file_length,
            media_type,
        }
    }
}

impl Downloadable for DownloadParams {
    fn direct_path(&self) -> Option<&str> {
        self.direct_path.as_path().to_str()
    }

    fn media_key(&self) -> Option<&[u8]> {
        self.media_key.as_deref()
    }

    fn file_enc_sha256(&self) -> Option<&[u8]> {
        self.file_enc_sha256.as_deref()
    }

    fn file_sha256(&self) -> Option<&[u8]> {
        Some(&self.file_sha256)
    }

    fn file_length(&self) -> Option<u64> {
        Some(self.file_length)
    }

    fn app_info(&self) -> wacore::download::MediaType {
        self.media_type
    }
}

#[derive(Debug)]
enum DownloadRequestError {
    Auth,
    NotFound,
    Other(anyhow::Error),
}

fn is_media_auth_error(status_code: u16) -> bool {
    matches!(status_code, 401 | 403)
}

impl Client {
    /// Get or fetch a fresh media connection, using cached value when valid.
    pub(crate) async fn refresh_media_conn(&self, force: bool) -> crate::Result<MediaConn> {
        {
            let guard = self.inner.media_conn.read().await;
            if !force
                && let Some(conn) = &*guard
                && !conn.is_expired()
            {
                return Ok(conn.clone());
            }
        }

        let response = self
            .inner
            .handle
            .send_iq(MediaConnSpec::new())
            .await
            .map_err(|e| ClientError::Internal(format!("media conn IQ failed: {e}")))?;

        let new_conn = MediaConn {
            auth: response.auth,
            ttl: response.ttl,
            auth_ttl: response.auth_ttl,
            hosts: response.hosts,
            fetched_at: Instant::now(),
        };

        let mut write_guard = self.inner.media_conn.write().await;
        *write_guard = Some(new_conn.clone());

        Ok(new_conn)
    }

    /// Invalidate the cached media connection to force a fresh IQ on next use.
    pub(crate) async fn invalidate_media_conn(&self) {
        *self.inner.media_conn.write().await = None;
    }

    /// Prepare download requests from a downloadable with optional media conn refresh.
    async fn prepare_requests<D: Downloadable + Sized>(
        &self,
        downloadable: &D,
        force_refresh: bool,
    ) -> crate::Result<Vec<DownloadRequest>> {
        let media_conn = self.refresh_media_conn(force_refresh).await?;
        let core_conn = CoreMediaConnection::from(&media_conn);

        DownloadUtils::prepare_download_requests(downloadable, &core_conn)
            .map_err(|e| ClientError::Internal(format!("failed to prepare download URLs: {e}")))
    }

    /// Execute a single HTTP download request and decrypt the response.
    async fn execute_download_request(
        &self,
        request: &DownloadRequest,
    ) -> Result<Vec<u8>, DownloadRequestError> {
        let client = &self.inner.http_client;

        let response = client
            .get(&request.url)
            .send()
            .await
            .map_err(|e| DownloadRequestError::Other(e.into()))?;

        let status = response.status().as_u16();
        if status >= 300 {
            if is_media_auth_error(status) {
                return Err(DownloadRequestError::Auth);
            }
            if matches!(status, 404 | 410) {
                return Err(DownloadRequestError::NotFound);
            }
            return Err(DownloadRequestError::Other(anyhow::anyhow!(
                "download failed with HTTP status: {status}"
            )));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|e| DownloadRequestError::Other(e.into()))?;

        match &request.decryption {
            MediaDecryption::Encrypted {
                media_key,
                media_type,
            } => DownloadUtils::verify_and_decrypt(&bytes, media_key, *media_type)
                .map_err(|e| DownloadRequestError::Other(e.into())),
            MediaDecryption::Plaintext { file_sha256 } => {
                let mut output = Vec::new();
                DownloadUtils::copy_and_validate_plaintext_to_writer(
                    Cursor::new(bytes),
                    file_sha256,
                    &mut output,
                )
                .map_err(DownloadRequestError::Other)?;
                Ok(output)
            }
        }
    }

    /// Download and decrypt media from WhatsApp's CDN into memory.
    ///
    /// Only needed when you need the plaintext bytes (processing, transcoding,
    /// re-upload). To forward existing media unchanged, reuse the original
    /// message's CDN fields directly.
    pub async fn download<D: Downloadable + Sized>(
        &self,
        downloadable: &D,
    ) -> crate::Result<Vec<u8>> {
        let cap = downloadable
            .file_length()
            .unwrap_or(0)
            .min(DOWNLOAD_PREALLOC_CAP) as usize;
        let _ = cap; // Reserved for future streaming prealloc

        let mut force_refresh = false;
        let mut last_err = None;

        for attempt in 0..=MEDIA_AUTH_REFRESH_RETRY_ATTEMPTS {
            let requests = self.prepare_requests(downloadable, force_refresh).await?;
            let mut retry_with_fresh_auth = false;

            for request in &requests {
                match self.execute_download_request(request).await {
                    Ok(data) => return Ok(data),
                    Err(DownloadRequestError::Auth | DownloadRequestError::NotFound)
                        if attempt == 0 =>
                    {
                        self.invalidate_media_conn().await;
                        force_refresh = true;
                        retry_with_fresh_auth = true;
                        break;
                    }
                    Err(DownloadRequestError::Auth | DownloadRequestError::NotFound) => {
                        return Err(ClientError::Internal(
                            "media download auth failed after retry".into(),
                        ));
                    }
                    Err(DownloadRequestError::Other(e)) => {
                        tracing::warn!(
                            "Failed to download from {}: {e}. Trying next host.",
                            request.url,
                        );
                        last_err = Some(e);
                    }
                }
            }

            if !retry_with_fresh_auth {
                break;
            }
        }

        match last_err {
            Some(e) => Err(ClientError::Internal(format!("{e}"))),
            None => Err(ClientError::Internal(
                "failed to download from all available media hosts".into(),
            )),
        }
    }

    /// Download and decrypt media, writing the result to a file.
    ///
    /// Returns the number of bytes written.
    pub async fn download_to_file<D: Downloadable + Sized, P: AsRef<Path>>(
        &self,
        downloadable: &D,
        path: P,
    ) -> crate::Result<u64> {
        let data = self.download(downloadable).await?;
        let path = path.as_ref();

        if let Some(parent) = path.parent() {
            async_fs::create_dir_all(parent)
                .await
                .map_err(|e| ClientError::Internal(format!("failed to create directory: {e}")))?;
        }

        async_fs::write(path, &data)
            .await
            .map_err(|e| ClientError::Internal(format!("failed to write file: {e}")))?;

        Ok(data.len() as u64)
    }

    /// Download and decrypt media, streaming to a writer.
    ///
    /// Memory usage is proportional to the CDN chunk size (~40 KB) rather than
    /// the full file size. Returns the number of bytes written.
    pub async fn download_to_writer<D: Downloadable + Sized, W: Write + Seek + Send + 'static>(
        &self,
        downloadable: &D,
        mut writer: W,
    ) -> crate::Result<u64> {
        let mut force_refresh = false;
        let mut last_err = None;

        for attempt in 0..=MEDIA_AUTH_REFRESH_RETRY_ATTEMPTS {
            let requests = self.prepare_requests(downloadable, force_refresh).await?;
            let mut retry_with_fresh_auth = false;

            for request in &requests {
                writer
                    .seek(SeekFrom::Start(0))
                    .map_err(|e| ClientError::Internal(format!("seek failed: {e}")))?;

                match self
                    .streaming_download_to_writer(request, &mut writer)
                    .await
                {
                    Ok(bytes_written) => return Ok(bytes_written),
                    Err(DownloadRequestError::Auth | DownloadRequestError::NotFound)
                        if attempt == 0 =>
                    {
                        self.invalidate_media_conn().await;
                        force_refresh = true;
                        retry_with_fresh_auth = true;
                        break;
                    }
                    Err(DownloadRequestError::Auth | DownloadRequestError::NotFound) => {
                        return Err(ClientError::Internal(
                            "media stream download auth failed after retry".into(),
                        ));
                    }
                    Err(DownloadRequestError::Other(e)) => {
                        tracing::warn!(
                            "Failed to stream-download from {}: {e}. Trying next host.",
                            request.url,
                        );
                        last_err = Some(e);
                    }
                }
            }

            if !retry_with_fresh_auth {
                break;
            }
        }

        match last_err {
            Some(e) => Err(ClientError::Internal(format!("{e}"))),
            None => Err(ClientError::Internal(
                "failed to stream-download from all available media hosts".into(),
            )),
        }
    }

    /// Download a single request and stream-decrypt to the writer.
    async fn streaming_download_to_writer<W: Write>(
        &self,
        request: &DownloadRequest,
        writer: &mut W,
    ) -> Result<u64, DownloadRequestError> {
        let client = &self.inner.http_client;

        let response = client
            .get(&request.url)
            .send()
            .await
            .map_err(|e| DownloadRequestError::Other(e.into()))?;

        let status = response.status().as_u16();
        if status >= 300 {
            if is_media_auth_error(status) {
                return Err(DownloadRequestError::Auth);
            }
            if matches!(status, 404 | 410) {
                return Err(DownloadRequestError::NotFound);
            }
            return Err(DownloadRequestError::Other(anyhow!(
                "stream download failed with HTTP status: {status}"
            )));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|e| DownloadRequestError::Other(e.into()))?;

        match &request.decryption {
            MediaDecryption::Encrypted {
                media_key,
                media_type,
            } => {
                let cursor = Cursor::new(bytes);
                DownloadUtils::decrypt_stream_to_writer(cursor, media_key, *media_type, writer)
                    .map_err(DownloadRequestError::Other)
            }
            MediaDecryption::Plaintext { file_sha256 } => {
                let cursor = Cursor::new(bytes);
                DownloadUtils::copy_and_validate_plaintext_to_writer(cursor, file_sha256, writer)
                    .map_err(DownloadRequestError::Other)
            }
        }
    }
}
