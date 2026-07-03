//! Error types for all client operations.

use waepic_connection::ConnectionError;

/// The unified error type for client operations.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// A connection-layer error occurred.
    #[error("connection error: {0}")]
    Connection(#[from] ConnectionError),
    /// The client is not connected to the WhatsApp server.
    #[error("not connected to WhatsApp server")]
    NotConnected,
    /// The client is not logged in.
    #[error("not logged in")]
    NotLoggedIn,
    /// A connection was already established.
    #[error("already connected")]
    AlreadyConnected,
    /// A socket-level error occurred.
    #[error("socket error: {0}")]
    Socket(String),
    /// Encryption or sending failed.
    #[error("encryption/send error: {0}")]
    EncryptSend(String),
    /// An IQ (Info Query) request failed.
    #[error("IQ error")]
    Iq(#[from] IqError),
    /// An authentication error occurred.
    #[error("authentication error")]
    Auth(#[from] AuthError),
    /// A message send error occurred.
    #[error("send error")]
    Send(#[from] SendError),
    /// A protocol-level error occurred.
    #[error("protocol error: {0}")]
    Protocol(String),
    /// An I/O error occurred.
    #[error("IO error")]
    Io(#[from] std::io::Error),
    /// An internal error occurred.
    #[error("internal error: {0}")]
    Internal(String),
}

/// Authentication errors during pairing or login.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    /// The client has not paired with any device yet.
    #[error("not paired with any device")]
    NotPaired,
    /// Pairing failed with the given reason.
    #[error("pairing failed: {0}")]
    PairFailed(String),
    /// The pair code entered was invalid.
    #[error("invalid pair code")]
    PairCodeInvalid,
    /// The QR code expired before scanning.
    #[error("QR code expired")]
    QrExpired,
    /// The QR code timed out waiting for a scan.
    #[error("QR code timeout")]
    QrTimeout,
}

/// Errors from IQ (Info Query) request/response cycles.
#[derive(Debug, thiserror::Error)]
pub enum IqError {
    /// The IQ request timed out with no response.
    #[error("IQ timeout")]
    Timeout,
    /// The server returned an error response.
    #[error("server error: {0}")]
    ServerError(String),
    /// The server response did not match the expected format.
    #[error("unexpected response: {0}")]
    UnexpectedResponse(String),
    /// Failed to parse the server response.
    #[error("parse error: {0}")]
    ParseError(String),
}

/// Errors from sending messages or performing message operations.
#[derive(Debug, thiserror::Error)]
pub enum SendError {
    /// The request was invalid (e.g. empty message, bad JID).
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    /// Encryption failed for the outgoing message.
    #[error("encryption failed: {0}")]
    EncryptionFailed(String),
    /// An IQ request failed during the send operation.
    #[error("IQ failed")]
    IqFailed(#[from] IqError),
}
