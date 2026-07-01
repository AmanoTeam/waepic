//! WebSocket transport for the WhatsApp multidevice protocol.
//!
//! This crate provides the connection layer used by [`waepic-client`].
//! It handles WebSocket transport, Noise XX handshake, frame encryption/decryption,
//! and the read loop with keepalive and auto-reconnect.
//!
//! Most users will not use this crate directly. Instead, use [`waepic-client`]'s
//! [`Client`] which wraps the connection handle.
//!
//! [`waepic-client`]: https://docs.rs/waepic-client
//! [`Client`]: waepic_client::Client

#![deny(clippy::all)]

pub mod connection;
pub mod error;
pub mod frame;
pub mod noise_socket;
pub mod transport;

pub use error::ConnectionError;

/// A `Result` alias for the connection layer.
pub type Result<T> = std::result::Result<T, ConnectionError>;
