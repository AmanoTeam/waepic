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

pub mod chat;
pub mod error;
pub mod memory;
pub mod session;

pub use chat::ChatEntry;
pub use error::SessionError;
pub use memory::MemorySession;
pub use session::{Backend, Session};

/// Convenient [`Result`] alias for session operations.
pub type Result<T> = std::result::Result<T, SessionError>;
