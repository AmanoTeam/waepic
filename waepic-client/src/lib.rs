//! # waepic-client
//!
//! A high-level Rust client library for WhatsApp Web, built on top of
//! [`wacore`] and [`wacore_binary`].
//!
//! ## Modules
//!
//! - [`client`] - The main [`Client`] struct and its API methods.
//! - [`config`] - Client configuration (device properties, reconnect settings).
//! - [`error`] - Error types for all waepic operations.
//! - [`message`] - [`Message`], [`InputMessage`], and [`MessageInfo`].
//! - [`peer`] - [`Chat`], [`User`], [`Group`], [`Newsletter`], and more.
//! - [`update`] - [`Update`] enum and supporting event types.
//!
//! [`Client`]: client::Client
//! [`Chat`]: peer::Chat
//! [`Message`]: message::Message
//! [`InputMessage`]: message::InputMessage
//! [`MessageInfo`]: message::MessageInfo
//! [`Update`]: update::Update

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

/// Re-export of the main client handle, update stream, and pairing types.
pub use client::{
    Client, UpdateStream,
    auth::LoginStatus,
    messages::RevokeType,
    pair::{PairEvent, PairEventStream},
};
/// Re-export of the client configuration type.
pub use config::ClientConfiguration;
/// Re-export of all error types.
pub use error::{AuthError, ClientError, IqError, SendError};
/// Re-export of message types.
pub use message::{InputMessage, Message, MessageInfo};
/// Re-export of peer types and JID utilities.
pub use peer::{Chat, Group, Jid, JidExt, Newsletter, OtherChat, Server, User};
/// Re-export of the update event enum.
pub use update::Update;

/// Convenient [`Result`] alias for client operations.
pub type Result<T> = std::result::Result<T, ClientError>;

/// Re-export of [`wacore`].
pub use wacore;
/// Re-export of [`wacore-binary`].
pub use wacore_binary;
/// Re-export of [`waproto`].
pub use waproto;
