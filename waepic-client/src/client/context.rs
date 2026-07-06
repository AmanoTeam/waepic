//! Runtime-agnostic executor and SendContextResolver for E2E encryption.

use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc, thread, time::Duration};

use anyhow::anyhow;
use async_trait::async_trait;
use futures_timer::Delay;
use wacore::{
    client::context::{GroupInfo, SendContextResolver},
    iq::{
        prekeys::{PreKeyBundle, PreKeyFetchReason, PreKeyFetchSpec},
        usync::{DeviceListResponse, DeviceListSpec},
    },
    libsignal::protocol::PreKeyBundle as LibsignalPreKeyBundle,
    runtime::{AbortHandle, Runtime},
    types::message::AddressingMode,
};
use wacore_binary::Jid;

use crate::{Client, client};

/// Runtime-agnostic executor for wacore's async operations.
///
/// Uses `async_global_executor` for spawning, `futures_timer` for sleep,
/// and `std::thread::spawn` for blocking work.
#[derive(Clone, Debug)]
pub struct RuntimeHandle;

impl RuntimeHandle {
    /// Create a new runtime handle.
    pub fn new() -> Self {
        Self
    }
}

impl Default for RuntimeHandle {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Runtime for RuntimeHandle {
    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) -> AbortHandle {
        let handle = async_global_executor::spawn(future);
        AbortHandle::new(move || {
            drop(handle);
        })
    }

    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(Delay::new(duration))
    }

    fn spawn_blocking(
        &self,
        f: Box<dyn FnOnce() + Send + 'static>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        thread::spawn(move || {
            f();
        });
        Box::pin(std::future::ready(()))
    }

    fn yield_now(&self) -> Option<Pin<Box<dyn Future<Output = ()> + Send>>> {
        None
    }
}

#[async_trait]
impl SendContextResolver for Client {
    async fn resolve_devices(&self, jids: &[Jid]) -> Result<Vec<Jid>, anyhow::Error> {
        let jids_vec: Vec<Jid> = jids.to_vec();
        let sid = client::messages::generate_message_id();
        let spec = DeviceListSpec::new(jids_vec, sid);

        let response: DeviceListResponse = self
            .inner
            .handle
            .send_iq(spec)
            .await
            .map_err(|e| anyhow::anyhow!("device list IQ failed: {e}"))?;

        let mut device_jids = Vec::new();
        for user_list in &response.device_lists {
            let user_jid = &user_list.user;
            for d in &user_list.devices {
                let mut jid = user_jid.clone();
                jid.device = d.device;
                device_jids.push(jid);
            }
        }

        Ok(device_jids)
    }

    async fn fetch_prekeys(
        &self,
        jids: &[Jid],
    ) -> Result<HashMap<Jid, LibsignalPreKeyBundle>, anyhow::Error> {
        let spec = PreKeyFetchSpec::new(jids.to_vec());
        let bundles: HashMap<Jid, PreKeyBundle> = self
            .inner
            .handle
            .send_iq(spec)
            .await
            .map_err(|e| anyhow::anyhow!("prekey fetch IQ failed: {e}"))?;

        Ok(bundles)
    }

    async fn fetch_prekeys_for_identity_check(
        &self,
        jids: &[Jid],
    ) -> Result<HashMap<Jid, LibsignalPreKeyBundle>, anyhow::Error> {
        let spec = PreKeyFetchSpec::with_reason(jids.to_vec(), PreKeyFetchReason::Identity);
        let bundles: HashMap<Jid, PreKeyBundle> = self
            .inner
            .handle
            .send_iq(spec)
            .await
            .map_err(|e| anyhow!("prekey fetch (identity) IQ failed: {e}"))?;

        Ok(bundles)
    }

    async fn resolve_group_info(&self, jid: &Jid) -> Result<Arc<GroupInfo>, anyhow::Error> {
        let _ = jid;
        Ok(Arc::new(GroupInfo::new(vec![], AddressingMode::Pn)))
    }

    async fn get_lid_for_phone(&self, phone_user: &str) -> Option<wacore_binary::CompactString> {
        let _ = phone_user;
        None
    }

    fn on_local_identity_change(&self, jid: &Jid) {
        let _ = jid;
    }
}
