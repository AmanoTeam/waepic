//! In-memory storage implementation.

use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
};

use async_lock::RwLock;
use async_trait::async_trait;
use bytes::Bytes;
use wacore::{
    appstate::hash::HashState,
    store::{
        InMemoryBackend,
        error::Result as StoreResult,
        traits::{
            AppStateSyncKey, AppSyncStore, DeviceListRecord, DeviceStore, LidPnMappingEntry,
            MsgSecretEntry, MsgSecretStore, ProtocolStore, SignalStore, TcTokenEntry,
        },
    },
};
use wacore_binary::Jid;

use crate::{ChatEntry, Result, Session};

/// In-memory session storage.
///
/// Wraps [`InMemoryBackend`] for protocol-level persistence and adds
/// chat/contact caching. All data is lost when the process exits.
pub struct MemorySession {
    backend: InMemoryBackend,
    chats: RwLock<HashMap<String, ChatEntry>>,
    contacts: RwLock<HashSet<String>>,
}

impl MemorySession {
    /// Create a new empty `MemorySession`.
    pub fn new() -> Self {
        Self {
            backend: InMemoryBackend::new(),
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

#[async_trait]
impl SignalStore for MemorySession {
    async fn put_identity(&self, address: &str, key: [u8; 32]) -> StoreResult<()> {
        self.backend.put_identity(address, key).await
    }

    async fn load_identity(&self, address: &str) -> StoreResult<Option<[u8; 32]>> {
        self.backend.load_identity(address).await
    }

    async fn delete_identity(&self, address: &str) -> StoreResult<()> {
        self.backend.delete_identity(address).await
    }

    async fn get_session(&self, address: &str) -> StoreResult<Option<Bytes>> {
        self.backend.get_session(address).await
    }

    async fn put_session(&self, address: &str, session: &[u8]) -> StoreResult<()> {
        self.backend.put_session(address, session).await
    }

    async fn delete_session(&self, address: &str) -> StoreResult<()> {
        self.backend.delete_session(address).await
    }

    async fn store_prekey(&self, id: u32, record: &[u8], uploaded: bool) -> StoreResult<()> {
        self.backend.store_prekey(id, record, uploaded).await
    }

    async fn load_prekey(&self, id: u32) -> StoreResult<Option<Bytes>> {
        self.backend.load_prekey(id).await
    }

    async fn remove_prekey(&self, id: u32) -> StoreResult<()> {
        self.backend.remove_prekey(id).await
    }

    async fn get_max_prekey_id(&self) -> StoreResult<u32> {
        self.backend.get_max_prekey_id().await
    }

    async fn store_signed_prekey(&self, id: u32, record: &[u8]) -> StoreResult<()> {
        self.backend.store_signed_prekey(id, record).await
    }

    async fn load_signed_prekey(&self, id: u32) -> StoreResult<Option<Vec<u8>>> {
        self.backend.load_signed_prekey(id).await
    }

    async fn load_all_signed_prekeys(&self) -> StoreResult<Vec<(u32, Vec<u8>)>> {
        self.backend.load_all_signed_prekeys().await
    }

    async fn remove_signed_prekey(&self, id: u32) -> StoreResult<()> {
        self.backend.remove_signed_prekey(id).await
    }

    async fn put_sender_key(&self, address: &str, record: &[u8]) -> StoreResult<()> {
        self.backend.put_sender_key(address, record).await
    }

    async fn get_sender_key(&self, address: &str) -> StoreResult<Option<Vec<u8>>> {
        self.backend.get_sender_key(address).await
    }

    async fn delete_sender_key(&self, address: &str) -> StoreResult<()> {
        self.backend.delete_sender_key(address).await
    }

    async fn mark_prekeys_uploaded(&self, ids: &[u32]) -> StoreResult<()> {
        self.backend.mark_prekeys_uploaded(ids).await
    }
}

#[async_trait]
impl MsgSecretStore for MemorySession {
    async fn put_msg_secrets(&self, entries: Vec<MsgSecretEntry>) -> StoreResult<usize> {
        self.backend.put_msg_secrets(entries).await
    }

    async fn get_msg_secret(
        &self,
        chat: &str,
        sender: &str,
        msg_id: &str,
    ) -> StoreResult<Option<Vec<u8>>> {
        self.backend.get_msg_secret(chat, sender, msg_id).await
    }

    async fn get_msg_secret_with_ts(
        &self,
        chat: &str,
        sender: &str,
        msg_id: &str,
    ) -> StoreResult<Option<(Vec<u8>, i64)>> {
        self.backend.get_msg_secret_with_ts(chat, sender, msg_id).await
    }

    async fn delete_expired_msg_secrets(&self, cutoff_timestamp: i64) -> StoreResult<u32> {
        self.backend.delete_expired_msg_secrets(cutoff_timestamp).await
    }
}

#[async_trait]
impl AppSyncStore for MemorySession {
    async fn get_sync_key(&self, key_id: &[u8]) -> StoreResult<Option<AppStateSyncKey>> {
        self.backend.get_sync_key(key_id).await
    }

    async fn set_sync_key(&self, key_id: &[u8], key: AppStateSyncKey) -> StoreResult<()> {
        self.backend.set_sync_key(key_id, key).await
    }

    async fn get_version(&self, name: &str) -> StoreResult<HashState> {
        self.backend.get_version(name).await
    }

    async fn set_version(&self, name: &str, state: HashState) -> StoreResult<()> {
        self.backend.set_version(name, state).await
    }

    async fn put_mutation_macs(
        &self,
        name: &str,
        version: u64,
        mutations: &[wacore::appstate::processor::AppStateMutationMAC],
    ) -> StoreResult<()> {
        self.backend
            .put_mutation_macs(name, version, mutations)
            .await
    }

    async fn get_mutation_mac(&self, name: &str, index_mac: &[u8]) -> StoreResult<Option<Vec<u8>>> {
        self.backend.get_mutation_mac(name, index_mac).await
    }

    async fn delete_mutation_macs(&self, name: &str, index_macs: &[Vec<u8>]) -> StoreResult<()> {
        self.backend.delete_mutation_macs(name, index_macs).await
    }

    async fn clear_mutation_macs(&self, name: &str) -> StoreResult<()> {
        self.backend.clear_mutation_macs(name).await
    }

    async fn get_latest_sync_key_id(&self) -> StoreResult<Option<Vec<u8>>> {
        self.backend.get_latest_sync_key_id().await
    }
}

#[async_trait]
impl ProtocolStore for MemorySession {
    async fn get_sender_key_devices(&self, group_jid: &str) -> StoreResult<Vec<(String, bool)>> {
        self.backend.get_sender_key_devices(group_jid).await
    }

    async fn set_sender_key_status(
        &self,
        group_jid: &str,
        entries: &[(&str, bool)],
    ) -> StoreResult<()> {
        self.backend.set_sender_key_status(group_jid, entries).await
    }

    async fn clear_sender_key_devices(&self, group_jid: &str) -> StoreResult<()> {
        self.backend.clear_sender_key_devices(group_jid).await
    }

    async fn clear_all_sender_key_devices(&self) -> StoreResult<()> {
        self.backend.clear_all_sender_key_devices().await
    }

    async fn delete_sender_key_device_rows(&self, device_jids: &[&str]) -> StoreResult<()> {
        self.backend
            .delete_sender_key_device_rows(device_jids)
            .await
    }

    async fn get_lid_mapping(&self, lid: &str) -> StoreResult<Option<LidPnMappingEntry>> {
        self.backend.get_lid_mapping(lid).await
    }

    async fn get_pn_mapping(&self, phone: &str) -> StoreResult<Option<LidPnMappingEntry>> {
        self.backend.get_pn_mapping(phone).await
    }

    async fn put_lid_mapping(&self, entry: &LidPnMappingEntry) -> StoreResult<()> {
        self.backend.put_lid_mapping(entry).await
    }

    async fn get_all_lid_mappings(&self) -> StoreResult<Vec<LidPnMappingEntry>> {
        self.backend.get_all_lid_mappings().await
    }

    async fn save_base_key(
        &self,
        address: &str,
        message_id: &str,
        base_key: &[u8],
    ) -> StoreResult<()> {
        self.backend
            .save_base_key(address, message_id, base_key)
            .await
    }

    async fn has_same_base_key(
        &self,
        address: &str,
        message_id: &str,
        current_base_key: &[u8],
    ) -> StoreResult<bool> {
        self.backend
            .has_same_base_key(address, message_id, current_base_key)
            .await
    }

    async fn delete_base_key(&self, address: &str, message_id: &str) -> StoreResult<()> {
        self.backend.delete_base_key(address, message_id).await
    }

    async fn update_device_list(&self, record: DeviceListRecord) -> StoreResult<()> {
        self.backend.update_device_list(record).await
    }

    async fn get_devices(&self, user: &str) -> StoreResult<Option<DeviceListRecord>> {
        self.backend.get_devices(user).await
    }

    async fn delete_devices(&self, user: &str) -> StoreResult<()> {
        self.backend.delete_devices(user).await
    }

    async fn get_tc_token(&self, jid: &str) -> StoreResult<Option<TcTokenEntry>> {
        self.backend.get_tc_token(jid).await
    }

    async fn put_tc_token(&self, jid: &str, entry: &TcTokenEntry) -> StoreResult<()> {
        self.backend.put_tc_token(jid, entry).await
    }

    async fn delete_tc_token(&self, jid: &str) -> StoreResult<()> {
        self.backend.delete_tc_token(jid).await
    }

    async fn get_all_tc_token_jids(&self) -> StoreResult<Vec<String>> {
        self.backend.get_all_tc_token_jids().await
    }

    async fn delete_expired_tc_tokens(&self, token_cutoff: i64, sender_cutoff: i64) -> StoreResult<u32> {
        self.backend
            .delete_expired_tc_tokens(token_cutoff, sender_cutoff)
            .await
    }

    async fn store_sent_message(
        &self,
        chat_jid: &str,
        message_id: &str,
        payload: &[u8],
    ) -> StoreResult<()> {
        self.backend
            .store_sent_message(chat_jid, message_id, payload)
            .await
    }

    async fn take_sent_message(
        &self,
        chat_jid: &str,
        message_id: &str,
    ) -> StoreResult<Option<Vec<u8>>> {
        self.backend.take_sent_message(chat_jid, message_id).await
    }

    async fn delete_expired_sent_messages(&self, cutoff_timestamp: i64) -> StoreResult<u32> {
        self.backend
            .delete_expired_sent_messages(cutoff_timestamp)
            .await
    }
}

#[async_trait]
impl DeviceStore for MemorySession {
    async fn save(&self, device: &wacore::store::Device) -> StoreResult<()> {
        self.backend.save(device).await
    }

    async fn load(&self) -> StoreResult<Option<wacore::store::Device>> {
        self.backend.load().await
    }

    async fn exists(&self) -> StoreResult<bool> {
        self.backend.exists().await
    }

    async fn create(&self) -> StoreResult<i32> {
        self.backend.create().await
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
    async fn cache_and_get_chat() {
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
    async fn get_chat_nonexistent() {
        let session = MemorySession::new();

        let result = session.get_chat(&test_jid()).await.unwrap();
        assert!(result.is_none());
    }

    #[compio::test]
    async fn is_contact_unknown() {
        let session = MemorySession::new();

        let result = session.is_contact(&test_jid()).await.unwrap();
        assert!(!result);
    }

    #[compio::test]
    async fn is_contact_after_add() {
        let session = MemorySession::new();
        session.add_contact(&test_jid()).await;

        let result = session.is_contact(&test_jid()).await.unwrap();
        assert!(result);
    }

    #[compio::test]
    async fn remove_chat() {
        let session = MemorySession::new();
        let entry = test_chat_entry();

        session.cache_chat(&entry).await.unwrap();
        assert!(session.get_chat(&test_jid()).await.unwrap().is_some());

        session.remove_chat(&test_jid()).await.unwrap();
        assert!(session.get_chat(&test_jid()).await.unwrap().is_none());
    }

    #[compio::test]
    async fn get_chats() {
        let session = MemorySession::new();
        let entry = test_chat_entry();
        session.cache_chat(&entry).await.unwrap();

        let chats = session.get_chats().await.unwrap();
        assert_eq!(chats.len(), 1);
        assert_eq!(chats[0].name, Some("Test User".into()));
    }
}
