//! In-memory storage implementation.

use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
};

use async_lock::RwLock;
use wacore_binary::Jid;

use crate::{ChatEntry, Result, Session};

/// In-memory session storage.
///
/// Uses [`RwLock`] internally for concurrent access. All data is lost
/// when the process exits. For persistent storage, implement [`Session`]
/// with a database backend.
pub struct MemorySession {
    chats: RwLock<HashMap<String, ChatEntry>>,
    contacts: RwLock<HashSet<String>>,
}

impl MemorySession {
    /// Create a new empty `MemorySession`.
    pub fn new() -> Self {
        Self {
            chats: RwLock::new(HashMap::new()),
            contacts: RwLock::new(HashSet::new()),
        }
    }

    /// Add a contact JID to the contacts set.
    pub async fn add_contact(&self, jid: &Jid) {
        self.contacts.write().await.insert(jid.to_string());
    }
}

impl Default for MemorySession {
    fn default() -> Self {
        Self::new()
    }
}

impl Session for MemorySession {
    fn get_chat(
        &self,
        jid: &Jid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<ChatEntry>>> + Send + '_>> {
        let jid_str = jid.to_string();

        Box::pin(async move { Ok(self.chats.read().await.get(&jid_str).cloned()) })
    }

    fn cache_chat(
        &self,
        chat: &ChatEntry,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let chat = chat.clone();

        Box::pin(async move {
            self.chats.write().await.insert(chat.jid.to_string(), chat);
            Ok(())
        })
    }

    fn get_chats(&self) -> Pin<Box<dyn Future<Output = Result<Vec<ChatEntry>>> + Send + '_>> {
        Box::pin(async move { Ok(self.chats.read().await.values().cloned().collect()) })
    }

    fn remove_chat(&self, jid: &Jid) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let jid_str = jid.to_string();

        Box::pin(async move {
            self.chats.write().await.remove(&jid_str);
            Ok(())
        })
    }

    fn is_contact(&self, jid: &Jid) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + '_>> {
        let jid_str = jid.to_string();

        Box::pin(async move { Ok(self.contacts.read().await.contains(&jid_str)) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chat::tests::{test_chat_entry, test_jid};

    #[compio::test]
    async fn test_cache_and_get_chat() {
        let session = MemorySession::new();
        let entry = test_chat_entry();
        session.cache_chat(&entry).await.unwrap();

        let result = session.get_chat(&test_jid()).await.unwrap();
        assert!(result.is_some());

        let cached = result.unwrap();
        assert_eq!(cached.name, Some("Test User".to_string()));
        assert_eq!(cached.kind, "user");
    }

    #[compio::test]
    async fn test_get_chat_nonexistent() {
        let session = MemorySession::new();

        let result = session.get_chat(&test_jid()).await.unwrap();
        assert!(result.is_none());
    }

    #[compio::test]
    async fn test_is_contact_unknown() {
        let session = MemorySession::new();

        let result = session.is_contact(&test_jid()).await.unwrap();
        assert!(!result);
    }

    #[compio::test]
    async fn test_is_contact_after_add() {
        let session = MemorySession::new();
        session.add_contact(&test_jid()).await;

        let result = session.is_contact(&test_jid()).await.unwrap();
        assert!(result);
    }

    #[compio::test]
    async fn test_remove_chat() {
        let session = MemorySession::new();
        let entry = test_chat_entry();

        session.cache_chat(&entry).await.unwrap();
        assert!(session.get_chat(&test_jid()).await.unwrap().is_some());

        session.remove_chat(&test_jid()).await.unwrap();
        assert!(session.get_chat(&test_jid()).await.unwrap().is_none());
    }

    #[compio::test]
    async fn test_get_chats() {
        let session = MemorySession::new();
        let entry = test_chat_entry();
        session.cache_chat(&entry).await.unwrap();

        let chats = session.get_chats().await.unwrap();
        assert_eq!(chats.len(), 1);
        assert_eq!(chats[0].name, Some("Test User".into()));
    }
}
