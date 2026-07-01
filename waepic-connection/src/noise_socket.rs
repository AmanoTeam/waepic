//! NoiseSocket: encrypt/decrypt frames using [`wacore::noise::NoiseCipher`].

use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use async_lock::Mutex;
use bytes::{Bytes, BytesMut};
use wacore::noise::NoiseCipher;

use crate::{Result, error::ConnectionError};

/// Encrypts and frames a plaintext payload for sending.
///
/// Returns the framed ciphertext ready to pass to `transport.send()`.
pub fn encrypt_and_frame(cipher: &NoiseCipher, counter: u32, plaintext: &[u8]) -> Result<Bytes> {
    let mut buffer = BytesMut::from(plaintext);
    cipher
        .encrypt_in_place_with_counter(counter, &mut buffer)
        .map_err(|e| ConnectionError::Encrypt(format!("encrypt failed: {e}")))?;

    let mut framed = BytesMut::new();
    wacore::framing::encode_frame_into(&buffer, None, &mut framed)
        .map_err(|e| ConnectionError::Protocol(format!("frame encode failed: {e}")))?;

    Ok(framed.freeze())
}

/// Decrypts a received frame in-place.
///
/// On success, `ciphertext` is replaced with the decrypted plaintext.
pub fn decrypt_frame(cipher: &NoiseCipher, counter: u32, ciphertext: &mut BytesMut) -> Result<()> {
    cipher
        .decrypt_in_place_with_counter(counter, ciphertext)
        .map_err(|e| ConnectionError::Decrypt(format!("decrypt failed: {e}")))
}

/// A Noise socket that handles per-direction encryption/decryption counters.
pub struct NoiseSocket {
    read_cipher: NoiseCipher,
    read_counter: AtomicU32,
    write_cipher: NoiseCipher,
    write_counter: Mutex<u32>,
}

impl NoiseSocket {
    /// Create a new NoiseSocket from the handshake-derived cipher pair.
    pub fn new(write_cipher: NoiseCipher, read_cipher: NoiseCipher) -> Self {
        Self {
            read_cipher,
            read_counter: AtomicU32::new(0),
            write_cipher,
            write_counter: Mutex::new(0),
        }
    }

    /// Encrypt and frame a plaintext payload for sending.
    pub async fn encrypt_and_send(
        &self,
        transport: &Arc<dyn wacore::net::Transport>,
        plaintext: Bytes,
    ) -> Result<()> {
        let mut counter_guard = self.write_counter.lock().await;
        let counter = *counter_guard;

        // Refuse to wrap the counter: reusing an AES-GCM nonce is catastrophic.
        if counter == u32::MAX {
            return Err(ConnectionError::Encrypt("write counter exhausted".into()));
        }

        let framed = encrypt_and_frame(&self.write_cipher, counter, &plaintext)?;

        transport
            .send(framed)
            .await
            .map_err(|e| ConnectionError::Socket(format!("transport send failed: {e}")))?;

        *counter_guard = counter.wrapping_add(1);
        Ok(())
    }

    /// Decrypt a received frame in-place.
    pub fn decrypt_frame(&self, ciphertext: &mut BytesMut) -> Result<()> {
        let counter = self
            .read_counter
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |c| c.checked_add(1))
            .map_err(|_| ConnectionError::Decrypt("read counter exhausted".into()))?;

        decrypt_frame(&self.read_cipher, counter, ciphertext)
    }

    /// Get a reference to the read cipher.
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn read_cipher(&self) -> &NoiseCipher {
        &self.read_cipher
    }

    /// Get a reference to the write cipher.
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn write_cipher(&self) -> &NoiseCipher {
        &self.write_cipher
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let key = [0x42u8; 32];
        let write_cipher = NoiseCipher::new(&key).expect("32-byte key should be valid");
        let read_cipher = NoiseCipher::new(&key).expect("32-byte key should be valid");

        let plaintext = b"hello world";
        let framed =
            encrypt_and_frame(&write_cipher, 0, plaintext).expect("encrypt should succeed");

        // Strip the 3-byte frame length prefix
        let mut ciphertext = BytesMut::from(&framed[3..]);
        decrypt_frame(&read_cipher, 0, &mut ciphertext).expect("decrypt should succeed");

        assert_eq!(&ciphertext[..], plaintext);
    }

    #[test]
    fn test_counter_exhaustion_detected() {
        let key = [0x42u8; 32];
        let write_cipher = NoiseCipher::new(&key).expect("32-byte key should be valid");
        let read_cipher = NoiseCipher::new(&key).expect("32-byte key should be valid");

        let socket = NoiseSocket::new(write_cipher, read_cipher);
        socket.read_counter.store(u32::MAX, Ordering::SeqCst);

        let mut buf = BytesMut::from(&b"test"[..]);
        let err = socket
            .decrypt_frame(&mut buf)
            .expect_err("exhausted counter must error");
        assert!(err.to_string().contains("read counter exhausted"));
    }
}
