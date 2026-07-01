//! Session storage trait.

use std::pin::Pin;

use wacore_binary::Jid;

use crate::{ChatEntry, Result};

/// Session storage trait for peer/chat caching.
pub trait Session: Send + Sync {
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
}
