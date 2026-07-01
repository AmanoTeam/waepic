//! Error types for the connection layer.

use std::io;

/// Errors that can occur in the WebSocket transport, Noise handshake, or frame
/// processing.
#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    /// Failed to encrypt a frame for sending.
    #[error("encrypt failed: {0}")]
    Encrypt(String),
    /// Failed to decrypt a received frame.
    #[error("decrypt failed: {0}")]
    Decrypt(String),
    /// Underlying socket/transport error.
    #[error("socket error: {0}")]
    Socket(String),
    /// Protocol-level error (framing, node parsing, etc.).
    #[error("protocol error: {0}")]
    Protocol(String),
    /// Operation attempted while not connected.
    #[error("not connected")]
    NotConnected,
    /// I/O error.
    #[error(transparent)]
    Io(#[from] io::Error),
}
