//! WebSocket transport for the WhatsApp multidevice protocol.
//!
//! This crate provides the connection layer used by the `waepic-client` crate.
//! It handles WebSocket transport, Noise XX handshake, frame encryption/decryption,
//! and the read loop with keepalive and auto-reconnect.
//!
//! Most users will not use this crate directly. Instead, use `waepic-client`'s
//! `Client` which wraps the connection handle.

#![deny(clippy::all, clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::must_use_candidate,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::doc_markdown,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::redundant_pub_crate,
    clippy::needless_pass_by_value
)]

#[cfg(not(any(feature = "tokio", feature = "smol")))]
compile_error!("no async runtime was enabled, you must choose one of: tokio, smol");

/// Connection lifecycle: WebSocket transport, Noise handshake, read loop,
/// keepalive, and auto-reconnect.
pub mod connection;
/// Error types for the connection layer.
pub mod error;
/// Frame processing: decryption, node parsing, IQ routing, and keepalive.
pub mod frame;
/// Noise socket for encrypting and decrypting frames.
pub mod noise_socket;
/// WebSocket transport factory with pluggable TLS.
pub mod transport;

/// Re-export of connection types.
pub use connection::{Connection, ConnectionConfig, ConnectionHandle, ConnectionRunner, RawEvent};
/// Re-export of the connection error type.
pub use error::ConnectionError;
/// Re-export of the Noise socket type.
pub use noise_socket::NoiseSocket;
/// Re-export of the WebSocket transport factory.
pub use transport::WebSocketTransportFactory;

/// A `Result` alias for the connection layer.
pub type Result<T> = std::result::Result<T, ConnectionError>;
