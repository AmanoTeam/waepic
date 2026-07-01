//! # waepic-session
//!
//! Session storage for chat and contact caching, separate from wacore's
//! protocol-level `Backend` trait.
//!
//! ## Overview
//!
//! `waepic-session` provides a [`Session`] trait for caching chat metadata
//! (JIDs, names, types) and contact lists. The included [`MemorySession`]
//! is suitable for testing and simple use cases; production applications
//! should implement [`Session`] with persistent storage.
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
//! [`MemorySession`]: memory::MemorySession

#![deny(clippy::all)]

pub mod chat;
pub mod error;
pub mod memory;
pub mod session;

pub use chat::ChatEntry;
pub use error::SessionError;
pub use memory::MemorySession;
pub use session::Session;

/// Convenient [`Result`] alias for session operations.
pub type Result<T> = std::result::Result<T, SessionError>;
