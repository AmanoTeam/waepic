//! # waepic-session
//!
//! Session storage combining protocol-level persistence ([`Backend`]) with
//! chat and contact caching ([`Session`]).
//!
//! ## Overview
//!
//! [`Session`] extends [`Backend`] so a single value serves both protocol
//! persistence and chat/contact caching. The included [`MemorySession`]
//! wraps [`wacore::store::InMemoryBackend`] and adds in-memory chat storage.
//!
//! ## Quick start
//!
//! ```ignore
//! use waepic_session::{MemorySession, Session};
//! use std::sync::Arc;
//!
//! let session = Arc::new(MemorySession::new());
//! ```
//!
//! [`Session`]: session::Session
//! [`Backend`]: session::Backend
//! [`MemorySession`]: memory::MemorySession

#![deny(clippy::all)]

/// Cached chat record type.
pub mod chat;
/// Error types for session operations.
pub mod error;
/// In-memory session storage implementation.
pub mod memory;
/// Session storage trait and backend re-export.
pub mod session;

/// Re-export of the cached chat entry type.
pub use chat::ChatEntry;
/// Re-export of the session error type.
pub use error::SessionError;
/// Re-export of the in-memory session implementation.
pub use memory::MemorySession;
/// Re-export of the session trait and backend trait.
pub use session::{Backend, Session};

/// Convenient [`Result`] alias for session operations.
pub type Result<T> = std::result::Result<T, SessionError>;
