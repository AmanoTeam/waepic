pub mod auth;
pub mod handlers;
pub mod messages;
pub mod pair;
pub mod signal_adapter;
pub mod updates;

use std::{fmt, sync::Arc};

use async_lock::{Mutex, RwLock};
use chrono::Utc;
use buffa::message::Message as _;
use wacore::{
    libsignal::store::record_helpers as wacore_record, pair_code::PairCodeState,
    store::SignalStoreCache,
};
use wacore_binary::{Jid, JidExt, Server};
use waepic_connection::{Connection, ConnectionHandle, ConnectionRunner, RawEvent};
use waepic_session::Session;

use crate::{
    Result,
    config::ClientConfiguration,
    error::ClientError,
    peer::{Chat, Group, Newsletter, OtherChat, User},
};

pub use updates::UpdateStream;

/// The main WhatsApp client handle.
#[derive(Clone)]
pub struct Client {
    pub(crate) inner: Arc<ClientInner>,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client").finish_non_exhaustive()
    }
}

pub(crate) struct ClientInner {
    pub(crate) handle: ConnectionHandle,
    pub(crate) session: Arc<dyn Session>,
    pub(crate) config: ClientConfiguration,
    pub(crate) raw_tx: Option<async_broadcast::Sender<RawEvent>>,
    /// Device state (identity key, registration ID, prekeys, etc.).
    pub(crate) device: Arc<RwLock<wacore::store::Device>>,
    /// Signal protocol state cache (sessions, identities, sender keys).
    #[allow(dead_code)]
    pub(crate) signal_cache: Arc<SignalStoreCache>,
    /// Pair-code authentication state machine.
    pub(crate) pair_code_state: Mutex<PairCodeState>,
}

impl Client {
    /// Create a new `Client` with an existing connection handle.
    pub fn new(
        handle: ConnectionHandle,
        session: Arc<dyn Session>,
        config: ClientConfiguration,
    ) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                handle,
                session,
                config,
                raw_tx: None,
                device: Arc::new(RwLock::new(wacore::store::Device::new())),
                signal_cache: Arc::new(SignalStoreCache::new()),
                pair_code_state: Mutex::new(PairCodeState::Idle),
            }),
        }
    }

    /// Create a new `Client` by establishing a connection.
    #[tracing::instrument(skip(session))]
    pub fn connect(
        session: Arc<dyn Session>,
        config: ClientConfiguration,
    ) -> (Self, ConnectionRunner) {
        let (runner, event_tx, handle) =
            Connection::new(session.clone(), config.connection.clone());

        let client = Self {
            inner: Arc::new(ClientInner {
                handle,
                session,
                config,
                raw_tx: Some(event_tx),
                device: Arc::new(RwLock::new(wacore::store::Device::new())),
                signal_cache: Arc::new(SignalStoreCache::new()),
                pair_code_state: Mutex::new(PairCodeState::Idle),
            }),
        };

        (client, runner)
    }

    /// Load device state from the session backend, or keep the fresh device if none exists.
    ///
    /// Must be called after `connect()` and before any E2E operations.
    /// The device is loaded into the client's `Arc<RwLock<Device>>`.
    pub async fn load_or_create_device(&self) -> Result<()> {
        let loaded = self
            .inner
            .session
            .load()
            .await
            .map_err(|e| ClientError::Internal(format!("failed to load device: {e}")))?;
        let mut device = self.inner.device.write().await;
        if let Some(stored) = loaded {
            *device = stored;
        }
        // If no stored device, the fresh Device::new() created in connect() is used.

        // Ensure the signed prekey is stored in the SignalStore backend.
        // libsignal reads signed prekeys via backend.load_signed_prekey(),
        // not from the Device struct, so we must persist it here.
        if self
            .inner
            .session
            .load_signed_prekey(device.signed_pre_key_id)
            .await
            .map_err(|e| ClientError::Internal(format!("failed to check signed prekey: {e}")))?
            .is_none()
        {
            let structure = wacore_record::new_signed_pre_key_record(
                device.signed_pre_key_id,
                &device.signed_pre_key,
                device.signed_pre_key_signature,
                Utc::now(),
            );
            let encoded = structure.encode_to_vec();
            self.inner
                .session
                .store_signed_prekey(device.signed_pre_key_id, &encoded)
                .await
                .map_err(|e| {
                    ClientError::Internal(format!("failed to store signed prekey: {e}"))
                })?;
        }

        Ok(())
    }

    /// Map a JID to the appropriate [`Chat`] variant based on its server type.
    pub fn chat(&self, jid: Jid) -> Chat {
        match jid.server() {
            Server::Pn | Server::Lid => Chat::User(User::new(jid, self.clone())),
            Server::Group => Chat::Group(Group::new(jid, self.clone())),
            Server::Newsletter => Chat::Newsletter(Newsletter::new(jid, self.clone())),
            _ => Chat::Other(OtherChat::new(jid, self.clone())),
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn disconnect(&self) -> Result<()> {
        self.inner.handle.disconnect().await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn logout(&self) -> Result<()> {
        let device = self.inner.device.read().await;
        if let Some(pn) = &device.pn {
            use wacore::iq::devices::RemoveCompanionDeviceSpec;
            if let Err(e) = self
                .inner
                .handle
                .send_iq(RemoveCompanionDeviceSpec::new(pn))
                .await
            {
                tracing::warn!("failed to send logout IQ: {e}");
            }
        }
        drop(device);

        self.disconnect().await
    }
}
