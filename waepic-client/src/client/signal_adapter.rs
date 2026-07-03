//! Signal protocol store adapter.

use std::{error::Error, sync::Arc};

use async_lock::RwLock;
use async_trait::async_trait;
use buffa::message::Message as _;
use wacore::{
    libsignal::{
        protocol::{
            Direction, IdentityChange, IdentityKey, IdentityKeyPair, IdentityKeyStore, PreKeyId,
            PreKeyRecord, PreKeyStore, ProtocolAddress, SessionRecord, SessionStore,
            SignalProtocolError, SignedPreKeyId, SignedPreKeyRecord, SignedPreKeyStore,
        },
        store::{record_helpers as wacore_record, sender_key_name::SenderKeyName},
    },
    store::{SignalStoreCache, traits::Backend},
};
use waproto::whatsapp::{PreKeyRecordStructure, SignedPreKeyRecordStructure};

fn signal_err<E>(context: &'static str) -> impl FnOnce(E) -> SignalProtocolError
where
    E: Into<Box<dyn Error + Send + Sync + 'static>>,
{
    move |e| SignalProtocolError::BackendError(context, e.into())
}

/// Shared state for all five sub-adapters.
#[derive(Clone)]
struct SharedDevice {
    device: Arc<RwLock<wacore::store::Device>>,
    cache: Arc<SignalStoreCache>,
    backend: Arc<dyn Backend>,
}

#[derive(Clone)]
pub struct SessionAdapter(SharedDevice);
#[derive(Clone)]
pub struct IdentityAdapter(SharedDevice);
#[derive(Clone)]
pub struct PreKeyAdapter(SharedDevice);
#[derive(Clone)]
pub struct SignedPreKeyAdapter(SharedDevice);
#[derive(Clone)]
pub struct SenderKeyAdapter(SharedDevice);

impl SenderKeyAdapter {
    pub fn new(
        device: Arc<RwLock<wacore::store::Device>>,
        cache: Arc<SignalStoreCache>,
        backend: Arc<dyn Backend>,
    ) -> Self {
        Self(SharedDevice {
            device,
            cache,
            backend,
        })
    }
}

#[derive(Clone)]
pub struct SignalProtocolStoreAdapter {
    pub session_store: SessionAdapter,
    pub identity_store: IdentityAdapter,
    pub pre_key_store: PreKeyAdapter,
    pub signed_pre_key_store: SignedPreKeyAdapter,
    pub sender_key_store: SenderKeyAdapter,
}

impl SignalProtocolStoreAdapter {
    pub fn new(
        device: Arc<RwLock<wacore::store::Device>>,
        cache: Arc<SignalStoreCache>,
        backend: Arc<dyn Backend>,
    ) -> Self {
        let shared = SharedDevice {
            device,
            cache,
            backend,
        };
        Self {
            session_store: SessionAdapter(shared.clone()),
            identity_store: IdentityAdapter(shared.clone()),
            pre_key_store: PreKeyAdapter(shared.clone()),
            signed_pre_key_store: SignedPreKeyAdapter(shared.clone()),
            sender_key_store: SenderKeyAdapter(shared),
        }
    }
}

#[async_trait]
impl SessionStore for SessionAdapter {
    async fn load_session(
        &self,
        address: &ProtocolAddress,
    ) -> Result<Option<SessionRecord>, SignalProtocolError> {
        let _device = self.0.device.read().await;
        self.0
            .cache
            .get_session(address, &*self.0.backend)
            .await
            .map_err(signal_err("backend"))
    }

    async fn has_session(&self, address: &ProtocolAddress) -> Result<bool, SignalProtocolError> {
        let _device = self.0.device.read().await;
        self.0
            .cache
            .has_session(address, &*self.0.backend)
            .await
            .map_err(signal_err("backend"))
    }

    async fn store_session(
        &mut self,
        address: &ProtocolAddress,
        record: SessionRecord,
    ) -> Result<(), SignalProtocolError> {
        self.0.cache.put_session(address, record).await;
        Ok(())
    }
}

#[async_trait]
impl IdentityKeyStore for IdentityAdapter {
    async fn get_identity_key_pair(&self) -> Result<IdentityKeyPair, SignalProtocolError> {
        let device = self.0.device.read().await;
        let public = device.identity_key.public_key;
        let private = device.identity_key.private_key.clone();

        Ok(IdentityKeyPair::new(IdentityKey::new(public), private))
    }

    async fn get_local_registration_id(&self) -> Result<u32, SignalProtocolError> {
        let device = self.0.device.read().await;
        Ok(device.registration_id)
    }

    async fn save_identity(
        &mut self,
        address: &ProtocolAddress,
        identity: &IdentityKey,
    ) -> Result<IdentityChange, SignalProtocolError> {
        let existing_identity = self.get_identity(address).await?;
        self.0
            .cache
            .put_identity(address, identity.public_key().public_key_bytes())
            .await;

        match existing_identity {
            None => Ok(IdentityChange::NewOrUnchanged),
            Some(existing) if &existing == identity => Ok(IdentityChange::NewOrUnchanged),
            Some(_) => Ok(IdentityChange::ReplacedExisting),
        }
    }

    async fn is_trusted_identity(
        &self,
        _address: &ProtocolAddress,
        _identity: &IdentityKey,
        _direction: Direction,
    ) -> Result<bool, SignalProtocolError> {
        Ok(true)
    }

    async fn get_identity(
        &self,
        address: &ProtocolAddress,
    ) -> Result<Option<IdentityKey>, SignalProtocolError> {
        let _device = self.0.device.read().await;
        match self
            .0
            .cache
            .get_identity(address, &*self.0.backend)
            .await
            .map_err(signal_err("get_identity"))?
        {
            Some(data) if !data.is_empty() => {
                let public_key =
                    wacore::libsignal::protocol::PublicKey::from_djb_public_key_bytes(&data)?;
                Ok(Some(IdentityKey::new(public_key)))
            }
            _ => Ok(None),
        }
    }
}

#[async_trait]
impl PreKeyStore for PreKeyAdapter {
    async fn get_pre_key(&self, prekey_id: PreKeyId) -> Result<PreKeyRecord, SignalProtocolError> {
        let _device = self.0.device.read().await;
        let id: u32 = prekey_id.into();
        let bytes = self
            .0
            .backend
            .load_prekey(id)
            .await
            .map_err(signal_err("backend"))?
            .ok_or(SignalProtocolError::InvalidPreKeyId)?;

        let structure = PreKeyRecordStructure::decode_from_slice(bytes.as_ref())
            .map_err(|e| SignalProtocolError::InvalidArgument(format!("decode prekey: {e}")))?;
        wacore_record::prekey_structure_to_record(structure)
    }

    async fn save_pre_key(
        &mut self,
        prekey_id: PreKeyId,
        record: &PreKeyRecord,
    ) -> Result<(), SignalProtocolError> {
        let _device = self.0.device.read().await;
        let structure = wacore_record::prekey_record_to_structure(record)?;
        let encoded = structure.encode_to_vec();

        self.0
            .backend
            .store_prekey(prekey_id.into(), &encoded, false)
            .await
            .map_err(signal_err("backend"))
    }

    async fn remove_pre_key(&mut self, prekey_id: PreKeyId) -> Result<(), SignalProtocolError> {
        self.0
            .backend
            .remove_prekey(prekey_id.into())
            .await
            .map_err(signal_err("backend"))
    }
}

#[async_trait]
impl SignedPreKeyStore for SignedPreKeyAdapter {
    async fn get_signed_pre_key(
        &self,
        signed_prekey_id: SignedPreKeyId,
    ) -> Result<SignedPreKeyRecord, SignalProtocolError> {
        let _device = self.0.device.read().await;
        let id: u32 = signed_prekey_id.into();
        let bytes = self
            .0
            .backend
            .load_signed_prekey(id)
            .await
            .map_err(signal_err("backend"))?
            .ok_or(SignalProtocolError::InvalidSignedPreKeyId)?;

        let structure = SignedPreKeyRecordStructure::decode_from_slice(bytes.as_slice()).map_err(|e| {
            SignalProtocolError::InvalidArgument(format!("decode signed prekey: {e}"))
        })?;
        wacore_record::signed_prekey_structure_to_record(structure)
    }

    async fn save_signed_pre_key(
        &mut self,
        _id: SignedPreKeyId,
        _record: &SignedPreKeyRecord,
    ) -> Result<(), SignalProtocolError> {
        Ok(())
    }
}

#[async_trait]
impl wacore::libsignal::protocol::SenderKeyStore for SenderKeyAdapter {
    async fn store_sender_key(
        &mut self,
        sender_key_name: &SenderKeyName,
        record: wacore::libsignal::protocol::SenderKeyRecord,
    ) -> wacore::libsignal::protocol::error::Result<()> {
        self.0.cache.put_sender_key(sender_key_name, record).await;
        Ok(())
    }

    async fn load_sender_key(
        &self,
        sender_key_name: &SenderKeyName,
    ) -> wacore::libsignal::protocol::error::Result<
        Option<wacore::libsignal::protocol::SenderKeyRecord>,
    > {
        let _device = self.0.device.read().await;
        self.0
            .cache
            .get_sender_key(sender_key_name, &*self.0.backend)
            .await
            .map(|opt| opt.map(|arc| (*arc).clone()))
            .map_err(signal_err("backend"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wacore::store::in_memory::InMemoryBackend;

    const PREKEY_ID: u32 = 7777;

    #[compio::test]
    async fn adapter_identity_key_pair_is_accessible() {
        let backend: Arc<dyn Backend> = Arc::new(InMemoryBackend::new());
        let device = Arc::new(RwLock::new(wacore::store::Device::new()));
        let cache = Arc::new(SignalStoreCache::new());
        let adapter = SignalProtocolStoreAdapter::new(device, cache, backend);

        let key_pair = adapter
            .identity_store
            .get_identity_key_pair()
            .await
            .unwrap();
        assert!(key_pair.public_key().public_key_bytes().len() == 32);
    }

    #[compio::test]
    async fn adapter_registration_id_is_accessible() {
        let backend: Arc<dyn Backend> = Arc::new(InMemoryBackend::new());
        let device = Arc::new(RwLock::new(wacore::store::Device::new()));
        let cache = Arc::new(SignalStoreCache::new());
        let adapter = SignalProtocolStoreAdapter::new(device, cache, backend);

        let reg_id = adapter
            .identity_store
            .get_local_registration_id()
            .await
            .unwrap();
        assert!(reg_id > 0);
    }

    #[compio::test]
    async fn adapter_is_trusted_identity_always_true() {
        let backend: Arc<dyn Backend> = Arc::new(InMemoryBackend::new());
        let device = Arc::new(RwLock::new(wacore::store::Device::new()));
        let cache = Arc::new(SignalStoreCache::new());
        let adapter = SignalProtocolStoreAdapter::new(device, cache, backend);

        let addr = ProtocolAddress::new("test".to_string(), 1.into());
        let key_pair = adapter
            .identity_store
            .get_identity_key_pair()
            .await
            .unwrap();
        let identity = IdentityKey::new(*key_pair.public_key());
        assert!(
            adapter
                .identity_store
                .is_trusted_identity(&addr, &identity, Direction::Receiving)
                .await
                .unwrap()
        );
    }

    #[compio::test]
    async fn remove_pre_key_deletes_immediately() {
        let backend: Arc<dyn Backend> = Arc::new(InMemoryBackend::new());
        let structure = PreKeyRecordStructure::default();
        let encoded = structure.encode_to_vec();
        backend
            .store_prekey(PREKEY_ID, &encoded, false)
            .await
            .unwrap();

        let device = Arc::new(RwLock::new(wacore::store::Device::new()));
        let cache = Arc::new(SignalStoreCache::new());

        let mut adapter = SignalProtocolStoreAdapter::new(device, cache.clone(), backend.clone());
        adapter
            .pre_key_store
            .remove_pre_key(PREKEY_ID.into())
            .await
            .unwrap();

        assert!(backend.load_prekey(PREKEY_ID).await.unwrap().is_none());
    }
}
