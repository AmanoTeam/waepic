use std::io;

/// Error type for session operations.
#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    /// The requested chat or contact was not found.
    #[error("not found")]
    NotFound,
    /// An I/O error occurred during storage operations.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    /// An internal session error.
    #[error("internal error: {0}")]
    Internal(String),
}
