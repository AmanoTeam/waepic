//! # waepic-client
//!
//! A high-level Rust client library for WhatsApp Web, built on top of
//! [`wacore`] and [`wacore_binary`].
//!
//! ## Overview
//!
//! waepic-client provides a ergonomic API for connecting to WhatsApp's
//! WebSocket servers, pairing devices via QR code or phone number, sending
//! and receiving messages, and handling real-time updates such as receipts,
//! presence, and history sync.
//!
//! ## Quick start
//!
//! ```ignore
//! use std::sync::Arc;
//!
//! use waepic_client::{Client, ClientConfiguration, RawEvent};
//! use waepic_session::MemorySession;
//!
//! # async fn main() -> waepic_client::error::Result<()> {
//! let backend = /* obtain a wacore Backend */;
//! let session = Arc::new(MemorySession::new());
//! let config = ClientConfiguration::default();
//!
//! let (client, raw_rx) = Client::connect(backend, session, config);
//! let mut updates = client.stream_updates(raw_rx);
//!
//! while let Some(update) = updates.next().await {
//!     println!("Update: {update:?}");
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Modules
//!
//! - [`client`] - The main [`Client`] struct and its API methods.
//! - [`config`] - Client configuration (device properties, reconnect settings).
//! - [`connection`] - WebSocket transport, Noise handshake, and read loop.
//! - [`error`] - Error types for all waepic operations.
//! - [`types`] - High-level types: [`Chat`], [`Message`], [`Update`], and more.
//!
//! [`Client`]: client::Client
//! [`Chat`]: types::Chat
//! [`Message`]: types::Message
//! [`Update`]: types::Update

#![deny(clippy::all)]

/// The main [`Client`] handle and its API methods.
pub mod client;
/// Client configuration: device properties, reconnect behavior, WebSocket URL.
pub mod config;
/// Error types for all client operations.
pub mod error;
/// High-level message types: [`Message`], [`InputMessage`], and [`MessageInfo`].
pub mod message;
/// Types relating to WhatsApp chats: users, groups, newsletters, and more.
pub mod peer;
/// Event types emitted by the client: [`Update`] and supporting structs.
pub mod update;

pub use client::Client;
pub use config::ClientConfiguration;
pub use error::{AuthError, ClientError, IqError, SendError};
pub use message::{InputMessage, Message, MessageInfo};
pub use peer::{Chat, Group, Jid, JidExt, Newsletter, OtherChat, Server, User};
pub use update::Update;

/// Convenient [`Result`] alias for client operations.
pub type Result<T> = std::result::Result<T, ClientError>;
