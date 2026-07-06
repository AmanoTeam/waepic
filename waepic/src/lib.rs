#![deny(clippy::all, clippy::pedantic)]
#![allow(ambiguous_glob_reexports)]
#![allow(
    clippy::module_name_repetitions,
    clippy::must_use_candidate,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::doc_markdown,
    clippy::similar_names,
    clippy::needless_pass_by_value
)]

//! Umbrella crate that re-exports waepic-client, waepic-connection, and waepic-session.

/// Re-exports all public items from the client crate.
pub use waepic_client::*;
/// Re-exports all public items from the connection crate.
pub use waepic_connection::*;
/// Re-exports all public items from the session crate.
pub use waepic_session::*;
