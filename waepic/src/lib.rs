#![deny(clippy::all)]
#![allow(ambiguous_glob_reexports)]

//! Umbrella crate that re-exports waepic-client, waepic-connection, and waepic-session.

/// Re-exports all public items from the client crate.
pub use waepic_client::*;
/// Re-exports all public items from the connection crate.
pub use waepic_connection::*;
/// Re-exports all public items from the session crate.
pub use waepic_session::*;
