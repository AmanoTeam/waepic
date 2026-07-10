//! Session storage trait.

use std::pin::Pin;

use wacore_binary::Jid;

use crate::{ChatEntry, Result};

/// Re-export of the protocol-level persistence backend trait.
pub use wacore::store::traits::Backend;

/// Session storage trait for peer/chat caching.
///
/// Extends [`Backend`] so a single session value serves both protocol-level
/// persistence and chat/contact caching.
pub trait Session: Backend {
    /// Look up a cached chat by JID.
    fn get_chat(
        &self,
        jid: &Jid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<ChatEntry>>> + Send + '_>>;

    /// Cache or update a chat entry.
    fn cache_chat(&self, chat: &ChatEntry)
    -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;

    /// Return all cached chats.
    fn get_chats(&self) -> Pin<Box<dyn Future<Output = Result<Vec<ChatEntry>>> + Send + '_>>;

    /// Remove a chat from the cache.
    fn remove_chat(&self, jid: &Jid) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;

    /// Whether the given JID is a known contact.
    fn is_contact(&self, jid: &Jid) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + '_>>;

    /// Clear the stored device from the session.
    ///
    /// Called when the server removes the companion (e.g. user logged out from
    /// phone). Forces the next connection to create a fresh device and show
    /// QR/pairing flow instead of reconnecting with a stale device.
    fn clear_device(&self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}
