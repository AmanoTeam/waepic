//! QR pairing types, event stream, and protocol logic.
//!
//! Provides [`PairEvent`], [`PairEventStream`], and the internal
//! `run_qr_pairing` function that drives the full QR pairing flow.

use std::{sync::Arc, time::Duration};

use chrono::Utc;
use futures_timer::Delay;
use futures_util::future::{Either, select};
use prost::Message as _;
use rand::{SeedableRng, rngs::StdRng};
use wacore::{
    companion_reg::companion_web_client_type_for_props,
    iq::passive::PassiveModeSpec,
    iq::prekeys::{PreKeyCountSpec, PreKeyUploadSpec},
    libsignal::protocol::KeyPair,
    libsignal::store::record_helpers as wacore_record,
    pair::{DeviceState, PairUtils},
};
use wacore_binary::{Jid, Node, NodeContent};

use waepic_connection::{ConnectionHandle, RawEvent};
use waepic_session::Session;

use crate::{
    Client, Result,
    config::ClientConfiguration,
    error::{AuthError, ClientError},
};

/// Minimum number of one-time prekeys the server should have.
const MIN_PREKEY_COUNT: usize = 100;

/// Events emitted during the QR pairing flow.
#[derive(Debug)]
pub enum PairEvent {
    /// A QR code to display, with its timeout in seconds.
    QrCode {
        /// The QR code data string.
        code: String,
        /// How long this QR code is valid, in seconds.
        timeout: u64,
    },
    /// Pairing completed successfully.
    Success,
    /// Pairing failed with an error.
    Error(ClientError),
}

/// A stream of [`PairEvent`] values produced by the QR pairing flow.
///
/// Created by [`Client::request_qr_pairing`].
/// Call [`recv`](Self::recv) to await the next event.
#[derive(Debug)]
pub struct PairEventStream {
    rx: async_channel::Receiver<PairEvent>,
}

impl PairEventStream {
    /// Create a new stream from a receiver.
    pub(crate) fn new(rx: async_channel::Receiver<PairEvent>) -> Self {
        Self { rx }
    }

    /// Receive the next event, or `None` if the stream has ended.
    pub async fn recv(&mut self) -> Option<PairEvent> {
        self.rx.recv().await.ok()
    }
}

impl Client {
    /// Request QR code pairing with the WhatsApp server.
    ///
    /// Returns a [`PairEventStream`] to receive pairing events and a future
    /// that drives the pairing flow. The caller must spawn the future on an
    /// async runtime.
    #[tracing::instrument(skip(self))]
    pub async fn request_qr_pairing(&self) -> Result<(PairEventStream, impl Future<Output = ()>)> {
        let device = self
            .inner
            .session
            .load()
            .await
            .map_err(|e| ClientError::Internal(format!("failed to load device: {e}")))?
            .ok_or(ClientError::NotLoggedIn)?;

        let raw_rx = self
            .inner
            .raw_tx
            .as_ref()
            .ok_or(ClientError::NotConnected)?
            .new_receiver();

        let (tx, rx) = async_channel::bounded(4);
        let stream = PairEventStream::new(rx);

        let handle = self.inner.handle.clone();
        let session = Arc::clone(&self.inner.session);
        let config = self.inner.config.clone();

        let future = async move {
            if let Err(e) = run_qr_pairing(handle, session, config, device, tx, raw_rx).await {
                tracing::error!("qr pairing failed: {e:#}");
            }
        };

        Ok((stream, future))
    }
}

async fn run_qr_pairing(
    handle: ConnectionHandle,
    session: Arc<dyn Session>,
    config: ClientConfiguration,
    device: wacore::store::Device,
    tx: async_channel::Sender<PairEvent>,
    mut raw_rx: async_broadcast::Receiver<RawEvent>,
) -> Result<()> {
    tracing::debug!("waiting for server-initiated pair-device iq");

    let refs = loop {
        match raw_rx.recv().await {
            Ok(RawEvent::Node(node)) if is_pair_device_node(&node) => {
                tracing::debug!("received server-initiated pair-device iq");
                match extract_pair_device_refs(&node) {
                    Some(r) => {
                        tracing::debug!(ref_count = r.len(), "extracted pairing refs");
                        break r;
                    }
                    None => {
                        tracing::warn!("pair-device iq had no valid refs, waiting for next");
                        continue;
                    }
                }
            }
            Ok(RawEvent::Disconnected) => {
                return Err(ClientError::NotConnected);
            }
            Err(_) => {
                return Err(ClientError::NotConnected);
            }
            _ => {}
        }
    };

    let client_type = companion_web_client_type_for_props(&device.device_props);
    let device_state = DeviceState {
        identity_key: device.identity_key.clone(),
        noise_key: device.noise_key.clone(),
        adv_secret_key: device.adv_secret_key,
    };

    let codes: Vec<String> = refs
        .iter()
        .map(|r| PairUtils::make_qr_data(&device_state, r, client_type))
        .collect();

    for (i, code) in codes.iter().enumerate() {
        if is_already_paired(&session).await {
            tracing::debug!("already paired, stopping qr rotation");
            let _ = tx.send(PairEvent::Success).await;
            return Ok(());
        }

        let timeout_secs = if i == 0 { 60u64 } else { 20u64 };
        tracing::debug!(qr_index = i, timeout = timeout_secs, "emitting qr code");

        if tx
            .send(PairEvent::QrCode {
                code: code.clone(),
                timeout: timeout_secs,
            })
            .await
            .is_err()
        {
            tracing::debug!("pair event receiver dropped, stopping qr pairing");
            return Ok(());
        }

        let deadline = Delay::new(Duration::from_secs(timeout_secs));
        futures_util::pin_mut!(deadline);

        loop {
            let deadline_fut = Box::pin(&mut deadline);
            let recv_fut = Box::pin(raw_rx.recv());

            match select(deadline_fut, recv_fut).await {
                Either::Left(_) => {
                    tracing::debug!(qr_index = i, "qr code timed out");
                    break;
                }
                Either::Right((event, _)) => match event {
                    Ok(RawEvent::Node(node)) => {
                        if is_pair_success_node(&node) {
                            tracing::debug!("received pair-success node");

                            match handle_pair_success(&handle, &session, &device, &node, &config)
                                .await
                            {
                                Ok(()) => {
                                    let _ = tx.send(PairEvent::Success).await;
                                    return Ok(());
                                }
                                Err(e) => {
                                    tracing::error!("pair success handling failed: {e}");
                                    let _ = tx.send(PairEvent::Error(e)).await;

                                    return Ok(());
                                }
                            }
                        }
                    }
                    Ok(RawEvent::Disconnected) => {
                        tracing::warn!("disconnected during qr pairing");
                        let _ = tx.send(PairEvent::Error(ClientError::NotConnected)).await;

                        return Ok(());
                    }
                    Ok(RawEvent::Error(e)) => {
                        tracing::error!("raw event error during qr pairing: {e}");
                    }
                    Err(_) => {
                        tracing::debug!("raw event stream ended during qr pairing");
                        let _ = tx.send(PairEvent::Error(ClientError::NotConnected)).await;

                        return Ok(());
                    }
                    _ => {}
                },
            }
        }
    }

    tracing::debug!("all qr codes expired");
    let _ = tx
        .send(PairEvent::Error(ClientError::Auth(AuthError::QrExpired)))
        .await;
    Ok(())
}

/// Check if the node is a `<pair-device>` IQ from the server (type="set").
fn is_pair_device_node(node: &Node) -> bool {
    if node.tag != "iq" {
        return false;
    }
    let is_set = node
        .attrs
        .get("type")
        .map(|v| v.as_str() == "set")
        .unwrap_or(false);
    if !is_set {
        return false;
    }
    node.children()
        .is_some_and(|children| children.iter().any(|c| c.tag == "pair-device"))
}

/// Extract pairing refs from a server-initiated `<pair-device>` IQ.
fn extract_pair_device_refs(node: &Node) -> Option<Vec<String>> {
    let children = node.children()?;
    let pair_device = children.iter().find(|c| c.tag == "pair-device")?;
    let refs: Vec<String> = pair_device
        .children()
        .map(|chs| {
            chs.iter()
                .filter(|c| c.tag == "ref")
                .filter_map(|c| match &c.content {
                    Some(NodeContent::Bytes(b)) => Some(b.as_slice()),
                    _ => None,
                })
                .filter_map(|b| std::str::from_utf8(b).ok())
                .map(|s| s.to_string())
                .collect()
        })
        .unwrap_or_default();
    if refs.is_empty() { None } else { Some(refs) }
}

/// Check if the node is a `<pair-success>` IQ from the server.
///
/// The server sends pair-success as a `type="set"` IQ (server-initiated),
/// not `type="get"`.
fn is_pair_success_node(node: &Node) -> bool {
    if node.tag != "iq" {
        return false;
    }

    let is_set = node
        .attrs
        .get("type")
        .map(|v| v.as_str() == "set")
        .unwrap_or(false);
    if !is_set {
        return false;
    }

    node.children()
        .is_some_and(|children| children.iter().any(|c| c.tag == "pair-success"))
}

/// Handle a `<pair-success>` IQ from the server.
async fn handle_pair_success(
    handle: &ConnectionHandle,
    session: &Arc<dyn Session>,
    device: &wacore::store::Device,
    node: &Node,
    _config: &ClientConfiguration,
) -> Result<()> {
    let req_id = node
        .attrs
        .get("id")
        .map(|v| v.as_str().to_string())
        .ok_or_else(|| ClientError::Protocol("pair-success missing id attribute".into()))?;
    let children = node
        .children()
        .ok_or_else(|| ClientError::Protocol("pair-success has no children".into()))?;
    let success_node = children
        .iter()
        .find(|c| c.tag == "pair-success")
        .ok_or_else(|| ClientError::Protocol("pair-success missing pair-success child".into()))?;

    let device_identity_bytes = success_node
        .children()
        .and_then(|chs| chs.iter().find(|c| c.tag == "device-identity"))
        .and_then(|c| c.content.clone())
        .and_then(|content| match content {
            NodeContent::Bytes(b) => Some(b),
            _ => None,
        })
        .ok_or_else(|| {
            ClientError::Protocol("pair-success missing device-identity bytes".into())
        })?;

    let business_name = success_node
        .children()
        .and_then(|chs| chs.iter().find(|c| c.tag == "biz"))
        .and_then(|c| c.attrs.get("name"))
        .map(|v| v.as_str().to_string())
        .unwrap_or_default();
    let platform = success_node
        .children()
        .and_then(|chs| chs.iter().find(|c| c.tag == "platform"))
        .and_then(|c| c.attrs.get("name"))
        .map(|v| v.as_str().to_string())
        .unwrap_or_default();
    let device_node = success_node
        .children()
        .and_then(|chs| chs.iter().find(|c| c.tag == "device"));

    let jid = device_node
        .and_then(|d| d.attrs.get("jid"))
        .and_then(|v| v.as_str().parse::<Jid>().ok())
        .unwrap_or_default();
    let lid = device_node
        .and_then(|d| d.attrs.get("lid"))
        .and_then(|v| v.as_str().parse::<Jid>().ok())
        .unwrap_or_default();

    tracing::debug!(
        jid = %jid,
        lid = %lid,
        business_name = %business_name,
        platform = %platform,
        "processing pair-success"
    );

    let device_state = DeviceState {
        identity_key: device.identity_key.clone(),
        noise_key: device.noise_key.clone(),
        adv_secret_key: device.adv_secret_key,
    };

    let (self_signed_identity_bytes, key_index) =
        PairUtils::do_pair_crypto(&device_state, &device_identity_bytes).map_err(|e| {
            tracing::error!("pair crypto failed: {e}");
            ClientError::Auth(AuthError::PairFailed(format!(
                "crypto validation failed ({}): {}",
                e.code, e.text
            )))
        })?;

    let response_node =
        PairUtils::build_pair_success_response(&req_id, self_signed_identity_bytes, key_index);
    handle.send_node(response_node).await.map_err(|e| {
        tracing::error!("failed to send pair-device-sign: {e}");
        ClientError::Auth(AuthError::PairFailed(format!(
            "failed to send pair-device-sign: {e}"
        )))
    })?;

    let mut updated_device = device.clone();
    updated_device.pn = Some(jid.clone());
    if !lid.user.is_empty() {
        updated_device.lid = Some(lid.clone());
    }
    if !business_name.is_empty() {
        updated_device.push_name = business_name.clone();
    }
    updated_device.server_has_prekeys = false;

    session.save(&updated_device).await.map_err(|e| {
        tracing::error!("failed to save device after pairing: {e}");
        ClientError::Internal(format!("failed to save device: {e}"))
    })?;

    let signed_prekey_structure = wacore_record::new_signed_pre_key_record(
        updated_device.signed_pre_key_id,
        &updated_device.signed_pre_key,
        updated_device.signed_pre_key_signature,
        Utc::now(),
    );
    let signed_prekey_bytes = signed_prekey_structure.encode_to_vec();
    session
        .store_signed_prekey(updated_device.signed_pre_key_id, &signed_prekey_bytes)
        .await
        .map_err(|e| {
            tracing::error!("failed to store signed prekey in signal store: {e}");
            ClientError::Internal(format!("failed to store signed prekey: {e}"))
        })?;

    tracing::debug!(
        jid = %jid,
        push_name = %business_name,
        "device paired successfully"
    );

    if let Err(e) = upload_prekeys_if_needed(handle, session, &updated_device).await {
        tracing::warn!("prekey upload after pairing failed (non-fatal): {e}");
    }

    if let Err(e) = send_active_iq(handle).await {
        tracing::warn!("failed to send active iq (non-fatal): {e}");
    }

    Ok(())
}

async fn is_already_paired(session: &Arc<dyn Session>) -> bool {
    match session.load().await {
        Ok(Some(d)) => d.pn.is_some(),
        _ => false,
    }
}

pub(crate) async fn upload_prekeys_if_needed(
    handle: &ConnectionHandle,
    session: &Arc<dyn Session>,
    device: &wacore::store::Device,
) -> Result<()> {
    let count_spec = PreKeyCountSpec::new();
    let count_response = handle.send_iq(count_spec).await.map_err(|e| {
        tracing::warn!("failed to query prekey count: {e}");
        ClientError::Internal(format!("failed to query prekey count: {e}"))
    })?;

    tracing::debug!(
        server_prekey_count = count_response.count,
        "server prekey count"
    );

    if count_response.count >= MIN_PREKEY_COUNT {
        tracing::debug!("prekey count sufficient, skipping upload");
        return Ok(());
    }

    let needed = MIN_PREKEY_COUNT - count_response.count;
    tracing::debug!(needed, "uploading prekeys");

    let mut rng = {
        let mut thread_rng = rand::rng();
        StdRng::from_rng(&mut thread_rng)
    };
    let start_id = device.next_pre_key_id;
    let pre_keys: Vec<(u32, KeyPair)> = (0..needed as u32)
        .map(|i| {
            let kp = KeyPair::generate(&mut rng);
            (start_id + i, kp)
        })
        .collect();

    let upload_spec = PreKeyUploadSpec::new(
        device.registration_id,
        device.identity_key.public_key,
        device.signed_pre_key_id,
        device.signed_pre_key.public_key,
        device.signed_pre_key_signature.to_vec(),
        pre_keys
            .iter()
            .map(|(id, kp)| (*id, kp.public_key))
            .collect(),
    );

    handle.send_iq(upload_spec).await.map_err(|e| {
        tracing::error!("prekey upload failed: {e}");
        ClientError::Internal(format!("prekey upload failed: {e}"))
    })?;

    for (id, kp) in &pre_keys {
        let structure = wacore_record::new_pre_key_record(*id, kp);
        let encoded = structure.encode_to_vec();
        if let Err(e) = session.store_prekey(*id, &encoded, true).await {
            tracing::warn!("failed to store prekey {id}: {e}");
        }
    }

    tracing::debug!(count = pre_keys.len(), "prekeys uploaded successfully");
    Ok(())
}

/// Send an active IQ to exit passive mode so the server delivers messages.
pub(crate) async fn send_active_iq(handle: &ConnectionHandle) -> Result<()> {
    handle
        .send_iq(PassiveModeSpec::active())
        .await
        .map_err(|e| {
            tracing::error!("failed to send active iq: {e}");
            ClientError::Internal(format!("failed to send active iq: {e}"))
        })?;

    tracing::debug!("sent active iq (exit passive mode)");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use wacore_binary::{
        SERVER_JID,
        builder::NodeBuilder,
        node::{Attrs, NodeValue},
    };

    #[test]
    fn is_pair_success_detects_valid_node() {
        let mut attrs = Attrs::new();
        attrs.push("type".to_string(), NodeValue::String("set".into()));
        attrs.push(
            "from".to_string(),
            NodeValue::String(SERVER_JID.to_string().into()),
        );

        let success_child = NodeBuilder::new("pair-success")
            .children([NodeBuilder::new("device-identity")
                .bytes(vec![1, 2, 3])
                .build()])
            .build();

        let node = Node::new("iq", attrs, Some(NodeContent::Nodes(vec![success_child])));
        assert!(is_pair_success_node(&node));
    }

    #[test]
    fn is_pair_success_rejects_non_iq() {
        let node = NodeBuilder::new("message").build();
        assert!(!is_pair_success_node(&node));
    }

    #[test]
    fn is_pair_success_rejects_non_set_iq() {
        let mut attrs = Attrs::new();
        attrs.push("type".to_string(), NodeValue::String("result".into()));

        let node = Node::new("iq", attrs, None);
        assert!(!is_pair_success_node(&node));
    }

    #[test]
    fn is_pair_success_rejects_get_iq() {
        let mut attrs = Attrs::new();
        attrs.push("type".to_string(), NodeValue::String("get".into()));

        let success_child = NodeBuilder::new("pair-success").build();
        let node = Node::new("iq", attrs, Some(NodeContent::Nodes(vec![success_child])));
        assert!(!is_pair_success_node(&node));
    }

    #[test]
    fn is_pair_success_rejects_iq_without_pair_success_child() {
        let mut attrs = Attrs::new();
        attrs.push("type".to_string(), NodeValue::String("set".into()));

        let other_child = NodeBuilder::new("other").build();
        let node = Node::new("iq", attrs, Some(NodeContent::Nodes(vec![other_child])));
        assert!(!is_pair_success_node(&node));
    }

    #[compio::test]
    async fn pair_event_stream_recv() {
        let (tx, rx) = async_channel::bounded(4);
        let mut stream = PairEventStream::new(rx);

        tx.send(PairEvent::QrCode {
            code: "test".into(),
            timeout: 60,
        })
        .await
        .unwrap();
        drop(tx);

        let event = stream.recv().await;
        assert!(matches!(event, Some(PairEvent::QrCode { .. })));

        let event = stream.recv().await;
        assert!(event.is_none());
    }
}
