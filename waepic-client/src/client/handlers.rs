//! Incoming node handlers.
//!
//! Each handler processes a raw protocol `Node` and produces an
//! `Update` event for the application layer.

use std::{mem, str::FromStr};

use buffa::message::Message as _;
use chrono::{DateTime, Utc};
use rand::rngs::StdRng;
use wacore::{
    iq::{
        dirty::{CleanDirtyBitsSpec, DirtyBit, DirtyType},
        spec::IqSpec,
    },
    libsignal::{
        self,
        protocol::{
            CiphertextMessage, PreKeySignalMessage, ProtocolAddress, SignalMessage, UsePQRatchet,
        },
        store::sender_key_name::SenderKeyName,
    },
    message_processing::{EncType, categorize_enc_nodes},
    messages::{decode_plaintext, is_sender_key_distribution_only, unwrap_device_sent},
    pair_code::{PairCodeState, PairCodeUtils},
    request::InfoQuery,
    types::message::{MessageInfo, MessageSource},
};
use wacore_binary::{
    Jid, Node, NodeContentRef, NodeRef, NodeValue, SERVER_JID, builder::NodeBuilder,
    node::NodeContent, zlib_pool,
};
use waproto::whatsapp as wa;

use crate::{
    Result,
    client::{Client, signal_adapter::SignalProtocolStoreAdapter},
    message::Message,
    update::{
        ChatPresence, ChatPresenceState, ConnectFailure, ConnectFailureReason, ContactUpdate,
        DisappearingModeChanged, GroupUpdate, HistorySyncChunk, PictureUpdate, Presence,
        PushNameUpdate, Receipt, ReceiptType, StreamError, SyncedConversation, TemporaryBan,
        Update,
    },
};

/// Process an incoming protocol node and produce an [`Update`] if applicable.
///
/// For `<message>` nodes with `<enc>` children, performs Signal E2E decryption
/// via the wacore libsignal protocol. Falls back to `<plaintext>` child decoding
/// for nodes without encryption (e.g., our own sent messages echoed back).
///
/// Returns `Ok(None)` for nodes that are not message stanzas or cannot be
/// decoded (the caller should continue processing other nodes).
#[allow(dead_code)] // FIXME: call at UpdateStream dispatch
pub(crate) async fn process_incoming_node(node: &Node, client: &Client) -> Result<Option<Update>> {
    if node.tag != "message" {
        return Ok(None);
    }

    let Some(from_jid) = node.attrs.get("from").and_then(NodeValue::to_jid) else {
        tracing::warn!("message node missing valid 'from' attribute, skipping");
        return Ok(None);
    };

    let msg_id = node
        .attrs
        .get("id")
        .map(|v| v.as_str().to_string())
        .unwrap_or_default();
    let msg_type = node
        .attrs
        .get("type")
        .map(|v| v.as_str().to_string())
        .unwrap_or_default();

    // Parse the "t" attribute as a unix timestamp (seconds)
    let timestamp = node
        .attrs
        .get("t")
        .and_then(|v| v.as_str().parse::<i64>().ok())
        .and_then(|ts| DateTime::from_timestamp(ts, 0))
        .unwrap_or_else(Utc::now);

    // Check for <enc> children for E2E decryption
    let enc_children = node
        .children()
        .map(|children| {
            children
                .iter()
                .filter(|c| c.tag == "enc")
                .collect::<Vec<&Node>>()
        })
        .unwrap_or_default();
    if !enc_children.is_empty() {
        return decrypt_e2e_message(
            node,
            &enc_children,
            &from_jid,
            &msg_id,
            &msg_type,
            timestamp,
            client,
        )
        .await;
    }

    // Fallback: plaintext content (our own sent messages echoed back)
    let proto_bytes = extract_plaintext_content(node);
    let Some(proto_bytes) = proto_bytes else {
        tracing::warn!("message node has no decodable content (id={msg_id}), skipping");
        return Ok(None);
    };

    let wa_msg = match wa::Message::decode_from_slice(proto_bytes.as_slice()) {
        Ok(msg) => msg,
        Err(e) => {
            tracing::warn!("failed to decode message protobuf (id={msg_id}): {e}");
            return Ok(None);
        }
    };

    let info = MessageInfo {
        id: msg_id,
        source: MessageSource {
            chat: from_jid.clone(),
            sender: from_jid.clone(),
            is_from_me: false,
            ..Default::default()
        },
        timestamp,
        r#type: msg_type,
        ..Default::default()
    };
    let chat = client.chat(from_jid);
    let message = Message::new(wa_msg, info, client.clone(), chat);

    Ok(Some(Update::NewMessage(message)))
}

/// Decrypt an E2E-encrypted message with `<enc>` children.
///
/// Classifies enc nodes into session (pkmsg/msg) and group (skmsg) buckets,
/// decrypts each, decodes the protobuf, and returns a `NewMessage` update.
#[allow(dead_code)]
async fn decrypt_e2e_message(
    node: &Node,
    enc_children: &[&Node],
    from_jid: &wacore_binary::Jid,
    msg_id: &str,
    msg_type: &str,
    timestamp: chrono::DateTime<chrono::Utc>,
    client: &Client,
) -> Result<Option<Update>> {
    let categorized = categorize_enc_nodes(enc_children);

    let backend = client.inner.session.clone();
    let device = client.inner.device.clone();
    let cache = client.inner.signal_cache.clone();

    let mut adapter = SignalProtocolStoreAdapter::new(device, cache, backend);

    let sender_jid = node
        .attrs
        .get("participant")
        .and_then(NodeValue::to_jid)
        .unwrap_or_else(|| from_jid.clone());
    let sender_address = ProtocolAddress::new(sender_jid.to_string(), 0.into());

    let mut plaintext = None;
    for enc_info in &categorized.session_enc {
        let ct_msg = match enc_info.enc_type {
            EncType::PreKeyMessage => match PreKeySignalMessage::try_from(enc_info.ciphertext) {
                Ok(m) => CiphertextMessage::PreKeySignalMessage(m),
                Err(e) => {
                    tracing::warn!("failed to parse PreKeySignalMessage: {e}");
                    continue;
                }
            },
            EncType::Message => match SignalMessage::try_from(enc_info.ciphertext) {
                Ok(m) => CiphertextMessage::SignalMessage(m),
                Err(e) => {
                    tracing::warn!("failed to parse SignalMessage: {e}");
                    continue;
                }
            },
            _ => continue,
        };

        let mut csprng = rand::make_rng::<StdRng>();
        match libsignal::protocol::message_decrypt(
            &ct_msg,
            &sender_address,
            &mut adapter.session_store,
            &mut adapter.identity_store,
            &mut adapter.pre_key_store,
            &adapter.signed_pre_key_store,
            &mut csprng,
            UsePQRatchet::No,
        )
        .await
        {
            Ok(result) => {
                plaintext = Some(result.plaintext);
                break; // Use the first successfully decrypted payload
            }
            Err(e) => {
                tracing::warn!("failed to decrypt session message (id={msg_id}): {e}");
            }
        }
    }

    // Process group (skmsg) enc nodes
    if plaintext.is_none() {
        for enc_info in &categorized.group_enc {
            let group_jid = from_jid.to_string();
            let sk_name = SenderKeyName::from_jid(&group_jid, &sender_address);

            match libsignal::protocol::group_decrypt(
                enc_info.ciphertext,
                &mut adapter.sender_key_store,
                &sk_name,
            )
            .await
            {
                Ok(pt) => {
                    plaintext = Some(pt);
                    break;
                }
                Err(e) => {
                    tracing::warn!("failed to decrypt group message (id={msg_id}): {e}");
                }
            }
        }
    }

    // If no decryption succeeded, return None
    let Some(plaintext) = plaintext else {
        tracing::warn!("all decryption attempts failed for message (id={msg_id}), skipping");
        return Ok(None);
    };

    // Decode the plaintext into a wa::Message
    let padding_version = enc_children
        .first()
        .and_then(|enc| enc.attrs.get("v"))
        .and_then(|v| v.as_str().parse::<u8>().ok())
        .unwrap_or(2);

    let mut wa_msg = match decode_plaintext(&plaintext, padding_version) {
        Ok(msg) => msg,
        Err(e) => {
            tracing::warn!("failed to decode decrypted plaintext (id={msg_id}): {e}");
            return Ok(None);
        }
    };

    // Check if this is a sender key distribution message only (no user content)
    wa_msg = unwrap_device_sent(wa_msg);
    if is_sender_key_distribution_only(&mut wa_msg) {
        if let Some(skdm_bytes) = &wa_msg
            .sender_key_distribution_message
            .axolotl_sender_key_distribution_message
        {
            let group_jid = wa_msg
                .sender_key_distribution_message
                .group_id
                .as_deref()
                .unwrap_or("");
            let sk_name = SenderKeyName::from_jid(&group_jid.to_string(), &sender_address);

            if let Ok(skdm_msg) =
                libsignal::protocol::SenderKeyDistributionMessage::try_from(skdm_bytes.as_slice())
            {
                if let Err(e) = libsignal::protocol::process_sender_key_distribution_message(
                    &sk_name,
                    &skdm_msg,
                    &mut adapter.sender_key_store,
                )
                .await
                {
                    tracing::warn!("failed to process SKDM: {e}");
                } else {
                    tracing::trace!("processed SKDM for group {group_jid}");
                }
            }
        }

        return Ok(None);
    }

    let info = MessageInfo {
        id: msg_id.to_string(),
        source: MessageSource {
            chat: from_jid.clone(),
            sender: sender_jid,
            is_from_me: false,
            ..Default::default()
        },
        timestamp,
        r#type: msg_type.to_string(),
        ..Default::default()
    };
    let chat = client.chat(from_jid.clone());
    let message = Message::new(wa_msg, info, client.clone(), chat);

    Ok(Some(Update::NewMessage(message)))
}

/// Extract protobuf bytes from a `<plaintext>` child node, falling back to
/// the node's direct byte content.
#[allow(dead_code)]
fn extract_plaintext_content(node: &Node) -> Option<Vec<u8>> {
    if let Some(plaintext_node) = node.get_optional_child("plaintext") {
        match &plaintext_node.content {
            Some(NodeContent::Bytes(b)) => return Some(b.clone()),
            Some(NodeContent::String(s)) => return Some(s.as_bytes().to_vec()),
            _ => {}
        }
    }

    match &node.content {
        Some(NodeContent::Bytes(b)) => Some(b.clone()),
        _ => None,
    }
}

/// Process an incoming `<receipt>` node and produce an [`Update::Receipt`].
///
/// Receipts indicate delivery or read confirmation for one or more messages.
/// The node may carry a single message ID in the `id` attribute, or multiple
/// IDs in `<list><item id="..."/></list>` children.
#[allow(dead_code)]
pub(crate) fn handle_receipt(node: &Node, client: &Client) -> Option<Update> {
    let Some(from_jid) = node.attrs.get("from").and_then(NodeValue::to_jid) else {
        tracing::warn!("receipt node missing valid 'from' attribute, skipping");
        return None;
    };
    let receipt_type = match node.attrs.get("type").map(|v| v.as_str()) {
        Some(s) if s.as_ref() == "read" => ReceiptType::Read,
        _ => ReceiptType::Delivered,
    };

    let timestamp = node
        .attrs
        .get("t")
        .and_then(|v| v.as_str().parse::<u64>().ok())
        .unwrap_or(0);

    // Collect message IDs: single "id" attribute or <list><item id="..."/></list>
    let message_ids = if let Some(list_node) = node.get_optional_child("list") {
        list_node
            .children()
            .map(|children| {
                children
                    .iter()
                    .filter(|c| c.tag == "item")
                    .filter_map(|c| c.attrs.get("id").map(|v| v.as_str().to_string()))
                    .collect()
            })
            .unwrap_or_default()
    } else {
        node.attrs
            .get("id")
            .map(|v| vec![v.as_str().to_string()])
            .unwrap_or_default()
    };

    let chat = client.chat(from_jid);

    Some(Update::Receipt(Receipt {
        chat,
        message_ids,
        receipt_type,
        timestamp,
    }))
}

/// Process an incoming `<presence>` node and produce an [`Update::Presence`].
///
/// Presence stanzas indicate a contact's online/offline status. An
/// `unavailable` type means the contact went offline; otherwise they are
/// online. The optional `last` attribute carries the last-seen timestamp.
#[allow(dead_code)]
pub(crate) fn handle_presence(node: &Node, client: &Client) -> Option<Update> {
    let Some(from_jid) = node.attrs.get("from").and_then(NodeValue::to_jid) else {
        tracing::warn!("presence node missing valid 'from' attribute, skipping");
        return None;
    };

    // "unavailable" type means offline; anything else (or missing) means online
    let available = node
        .attrs
        .get("type")
        .is_none_or(|v| v.as_str().as_ref() != "unavailable");

    // Parse last_seen from "last" attribute if present
    let last_seen = node
        .attrs
        .get("last")
        .and_then(|v| v.as_str().parse::<u64>().ok());

    let chat = client.chat(from_jid);

    Some(Update::Presence(Presence {
        chat,
        available,
        last_seen,
    }))
}

/// Process an incoming `<chatstate>` node and produce an [`Update::ChatPresence`].
///
/// Chatstate stanzas indicate typing/recording activity. The child element
/// (`<composing>` or `<paused>`) determines the state. For group chats, the
/// `participant` attribute identifies the specific sender.
#[allow(dead_code)]
pub(crate) fn handle_chatstate(node: &Node, client: &Client) -> Option<Update> {
    let Some(from_jid) = node.attrs.get("from").and_then(NodeValue::to_jid) else {
        tracing::warn!("chatstate node missing valid 'from' attribute, skipping");
        return None;
    };

    // Determine the chatstate from child elements
    let state = if node.get_optional_child("composing").is_some() {
        ChatPresenceState::Composing
    } else if node.get_optional_child("paused").is_some() {
        ChatPresenceState::Paused
    } else {
        // Unknown or missing child - ignore
        return None;
    };

    // For group chats, the participant attribute identifies the sender
    let sender =
        if let Some(participant_jid) = node.attrs.get("participant").and_then(NodeValue::to_jid) {
            client.chat(participant_jid)
        } else {
            client.chat(from_jid.clone())
        };

    let chat = client.chat(from_jid);

    Some(Update::ChatPresence(ChatPresence {
        chat,
        sender,
        state,
    }))
}

/// Process an incoming `<notification>` node and produce an [`Update`].
///
/// Dispatches by the notification `type` attribute:
/// - `contacts` with `<update>` child -> [`Update::ContactUpdate`]
/// - `w:gp2` -> [`Update::GroupUpdate`]
///
/// Also checks for `link_code_companion_reg` child (pair-code stage 2 trigger)
/// before the type-based dispatch.
#[allow(dead_code)]
pub(crate) async fn handle_notification(node: &Node, client: &Client) -> Result<Option<Update>> {
    if handle_pair_code_notification(client, node).await? {
        return Ok(None);
    }

    let notif_type = node
        .attrs
        .get("type")
        .map(|v| v.as_str().to_string())
        .unwrap_or_default();

    match notif_type.as_str() {
        "contacts" => Ok(handle_contacts_notification(node, client)),
        "w:gp2" => Ok(handle_group_notification(node, client)),
        "picture" => Ok(handle_picture_notification(node, client)),
        "w:push" | "push_name" => Ok(handle_push_name_notification(node, client)),
        "disappearing_mode" => Ok(handle_disappearing_mode_notification(node, client)),
        _ => Ok(None),
    }
}

/// Handle the `link_code_companion_reg` notification (pair-code stage 2 trigger).
///
/// Called when the user enters the code on their phone. The notification
/// contains the primary device's encrypted ephemeral public key and identity
/// public key. Performs DH key exchange and sends the `companion_finish` IQ.
async fn handle_pair_code_notification(client: &Client, node: &Node) -> Result<bool> {
    let node_ref = node.as_node_ref();
    let Some(reg_node) = node_ref.get_optional_child_by_tag(&["link_code_companion_reg"]) else {
        return Ok(false);
    };

    let Some(primary_wrapped_ephemeral) = reg_node
        .get_optional_child_by_tag(&["link_code_pairing_wrapped_primary_ephemeral_pub"])
        .and_then(|n| match n.content.as_deref() {
            Some(NodeContentRef::Bytes(b)) if b.len() == 80 => Some(b.to_vec()),
            _ => None,
        })
    else {
        tracing::warn!("missing or invalid primary wrapped ephemeral pub in notification");
        return Ok(false);
    };
    let Some(primary_identity_pub) = reg_node
        .get_optional_child_by_tag(&["primary_identity_pub"])
        .and_then(|n| match n.content.as_deref() {
            Some(NodeContentRef::Bytes(b)) if b.len() == 32 => {
                <[u8; 32]>::try_from(b.as_ref()).ok()
            }
            _ => None,
        })
    else {
        tracing::warn!("missing or invalid primary identity pub in notification");
        return Ok(false);
    };

    let mut state_guard = client.inner.pair_code_state.lock().await;
    let state = mem::take(&mut *state_guard);
    drop(state_guard);

    let PairCodeState::WaitingForPhoneConfirmation {
        pairing_ref,
        phone_jid,
        pair_code,
        ephemeral_keypair,
        ..
    } = state
    else {
        tracing::warn!("received pair code notification but not in waiting state");
        return Ok(false);
    };
    tracing::debug!("phone confirmed code entry, processing stage 2");

    let primary_ephemeral_pub = match PairCodeUtils::decrypt_primary_ephemeral_pub(
        &primary_wrapped_ephemeral,
        &pair_code,
    ) {
        Ok(pub_key) => pub_key,
        Err(e) => {
            tracing::warn!("failed to decrypt primary ephemeral pub: {e}");
            return Ok(false);
        }
    };

    let device = client.inner.device.read().await;
    let (wrapped_bundle, new_adv_secret) = match PairCodeUtils::prepare_key_bundle(
        &ephemeral_keypair,
        &primary_ephemeral_pub,
        &primary_identity_pub,
        &device.identity_key,
    ) {
        Ok(result) => result,
        Err(e) => {
            tracing::warn!("failed to prepare key bundle: {e}");
            return Ok(false);
        }
    };

    let identity_pub: [u8; 32] = device
        .identity_key
        .public_key
        .public_key_bytes()
        .try_into()
        .expect("identity key is 32 bytes");
    drop(device);

    {
        let mut device = client.inner.device.write().await;
        device.adv_secret_key = new_adv_secret;

        let device_clone = device.clone();
        drop(device);

        if let Err(e) = client.inner.session.save(&device_clone).await {
            tracing::warn!("failed to save device after adv secret rotation: {e}");
        }
    }

    let req_id = format!("{:016x}", rand::random::<u64>());
    let iq = PairCodeUtils::build_companion_finish_iq(
        &phone_jid,
        wrapped_bundle,
        &identity_pub,
        &pairing_ref,
        req_id,
    );

    if let Err(e) = client.inner.handle.send_node(iq).await {
        tracing::warn!("failed to send companion_finish: {e}");
        return Ok(false);
    }

    tracing::debug!("sent companion_finish, waiting for pair-success");
    *client.inner.pair_code_state.lock().await = PairCodeState::Completed;

    Ok(true)
}

/// Handle `<notification type="contacts">` - contact profile updates.
fn handle_contacts_notification(node: &Node, _client: &Client) -> Option<Update> {
    if let Some(update_node) = node.get_optional_child("update")
        && let Some(jid) = update_node.attrs.get("jid").and_then(NodeValue::to_jid)
    {
        let name = update_node
            .attrs
            .get("name")
            .map(|v| v.as_str().to_string());
        let push_name = update_node
            .attrs
            .get("push_name")
            .map(|v| v.as_str().to_string());

        return Some(Update::ContactUpdate(ContactUpdate {
            jid,
            name,
            push_name,
        }));
    }

    None
}

/// Handle `<notification type="w:gp2">` - group metadata updates.
fn handle_group_notification(node: &Node, _client: &Client) -> Option<Update> {
    let Some(group_jid) = node.attrs.get("from").and_then(NodeValue::to_jid) else {
        tracing::warn!("group notification missing valid 'from' attribute, skipping");
        return None;
    };
    let name = node
        .get_optional_child("subject")
        .and_then(Node::content_as_string)
        .map(|s| s.to_string());

    // Collect participants from <add> or <remove> children
    let mut participants = Vec::<Jid>::new();
    if let Some(children) = node.children() {
        for child in children {
            if (child.tag == "add" || child.tag == "remove")
                && let Some(child_children) = child.children()
            {
                for participant_node in child_children.iter().filter(|c| c.tag == "participant") {
                    if let Some(jid) = participant_node
                        .attrs
                        .get("jid")
                        .and_then(NodeValue::to_jid)
                    {
                        participants.push(jid);
                    }
                }
            }
        }
    }

    Some(Update::GroupUpdate(GroupUpdate {
        jid: group_jid,
        name,
        participants,
    }))
}

/// Handle `<notification type="picture">` - contact profile picture changes.
fn handle_picture_notification(node: &Node, _client: &Client) -> Option<Update> {
    let Some(from_jid) = node.attrs.get("from").and_then(NodeValue::to_jid) else {
        tracing::warn!("picture node missing valid 'from' attribute, skipping");
        return None;
    };
    let timestamp = node
        .attrs
        .get("t")
        .and_then(|v| v.as_str().parse::<u64>().ok())
        .unwrap_or(0);

    if node.get_optional_child("remove").is_some() {
        return Some(Update::PictureUpdate(PictureUpdate {
            jid: from_jid,
            author: None,
            timestamp,
            removed: true,
            picture_id: None,
        }));
    }

    let picture_id = node
        .get_optional_child("set")
        .or_else(|| node.get_optional_child("picture"))
        .and_then(|n| n.attrs.get("id").map(|v| v.as_str().to_string()));

    Some(Update::PictureUpdate(PictureUpdate {
        jid: from_jid,
        author: None,
        timestamp,
        removed: false,
        picture_id,
    }))
}

/// Handle `<notification type="w:push">` or `push_name` - contact push name changes.
fn handle_push_name_notification(node: &Node, _client: &Client) -> Option<Update> {
    let Some(from_jid) = node.attrs.get("from").and_then(NodeValue::to_jid) else {
        tracing::warn!("push node missing valid 'from' attribute, skipping");
        return None;
    };

    let push_name = node
        .get_optional_child("push")
        .and_then(Node::content_as_string)
        .map(|s| s.to_string())
        .unwrap_or_default();

    Some(Update::PushNameUpdate(PushNameUpdate {
        jid: from_jid,
        old_push_name: String::new(),
        new_push_name: push_name,
    }))
}

/// Handle `<notification type="disappearing_mode">` - disappearing messages setting changed.
fn handle_disappearing_mode_notification(node: &Node, _client: &Client) -> Option<Update> {
    let Some(from_jid) = node.attrs.get("from").and_then(NodeValue::to_jid) else {
        tracing::warn!("dissapearing mode node missing valid 'from' attribute, skipping");
        return None;
    };

    let duration = node
        .attrs
        .get("duration")
        .and_then(|v| v.as_str().parse::<u32>().ok())
        .unwrap_or(0);

    let setting_timestamp = node
        .attrs
        .get("t")
        .and_then(|v| v.as_str().parse::<u64>().ok())
        .unwrap_or(0);

    Some(Update::DisappearingModeChanged(DisappearingModeChanged {
        from: from_jid,
        duration,
        setting_timestamp,
    }))
}

/// Hard ceiling on the decompressed size of a history-sync blob, preventing
/// OOM on malformed or hostile input. Typical InitialBootstrap chunks inflate
/// to 5-20 MB.
const MAX_DECOMPRESSED: u64 = 64 * 1024 * 1024;

/// Process an incoming history sync notification and produce [`Update::HistorySync`]
/// chunks followed by [`Update::HistorySyncCompleted`].
///
/// History sync data arrives in two forms:
/// - `<notification type="history_sync_notification">` with a protobuf-encoded
///   `HistorySyncNotification` whose `initialHistBootstrapInlinePayload` field
///   carries the zlib-compressed [`wa::HistorySync`] blob.
/// - `<ib>` node with a `<history_sync>` child whose content is the compressed blob.
///
/// Returns a [`Vec<Update>`] because a single history sync blob may produce
/// multiple chunks (one per conversation batch) plus a completion marker.
#[allow(dead_code)]
pub(crate) fn handle_history_sync(node: &Node, client: &Client) -> Vec<Update> {
    let Some(compressed) = extract_history_sync_payload(node) else {
        return Vec::new();
    };
    let decompressed = match zlib_pool::decompress_zlib_pooled(&compressed, MAX_DECOMPRESSED) {
        Ok(data) => data,
        Err(e) => {
            tracing::warn!("failed to decompress history sync payload: {e}");
            return Vec::new();
        }
    };

    let history_sync = match wa::HistorySync::decode_from_slice(decompressed.as_slice()) {
        Ok(hs) => hs,
        Err(e) => {
            tracing::warn!("failed to decode HistorySync protobuf: {e}");
            return Vec::new();
        }
    };

    let mut updates = Vec::new();
    let mut synced_conversations = Vec::new();

    for conv in &history_sync.conversations {
        let chat_jid_str = conv.new_jid.as_deref().unwrap_or(&conv.id);
        let Ok(jid) = Jid::from_str(chat_jid_str) else {
            tracing::warn!("invalid JID in history sync conversation: {chat_jid_str}");
            continue;
        };

        let name = conv.name.clone().filter(|n| !n.is_empty());
        let pinned = conv.pinned.is_some_and(|p| p > 0);
        let archived = conv.archived.unwrap_or(false);
        let muted = conv.mute_end_time.is_some_and(|t| t > 0);
        let unread_count = conv.unread_count.unwrap_or(0);

        let mut messages = Vec::new();
        for hist_msg in &conv.messages {
            let web_msg = &hist_msg.message;

            let msg_id = web_msg.key.id.clone().unwrap_or_default();
            let from_me = web_msg.key.from_me.unwrap_or(false);

            let sender_jid_str = web_msg.key.participant.as_deref().unwrap_or(chat_jid_str);
            let Ok(sender_jid) = Jid::from_str(sender_jid_str) else {
                tracing::warn!("invalid sender JID in history sync message: {sender_jid_str}");
                continue;
            };

            let timestamp = web_msg.message_timestamp.unwrap_or(0);
            let info = MessageInfo {
                id: msg_id,
                source: MessageSource {
                    chat: jid.clone(),
                    sender: sender_jid.clone(),
                    is_from_me: from_me,
                    ..Default::default()
                },
                timestamp: DateTime::from_timestamp(timestamp as i64, 0).unwrap_or_else(Utc::now),
                ..Default::default()
            };

            let wa_msg = (*web_msg.message).clone();

            let chat = client.chat(jid.clone());
            let message = Message::new(wa_msg, info, client.clone(), chat);
            messages.push(message);
        }

        synced_conversations.push(SyncedConversation {
            jid,
            name,
            messages,
            pinned,
            archived,
            muted,
            unread_count,
        });
    }

    if !synced_conversations.is_empty() {
        updates.push(Update::HistorySync(HistorySyncChunk {
            conversations: synced_conversations,
        }));
    }

    // Always emit completion, even if no conversations were processed
    updates.push(Update::HistorySyncCompleted);

    updates
}

/// Extract the zlib-compressed history sync payload from a node.
///
/// Handles two forms:
/// 1. `<notification type="history_sync_notification">` - decodes the node
///    content as `wa::Message`, extracts `historySyncNotification`, then reads
///    `initialHistBootstrapInlinePayload`.
/// 2. `<ib>` with a `<history_sync>` child - reads the child's byte content.
fn extract_history_sync_payload(node: &Node) -> Option<Vec<u8>> {
    // Form 1: notification with type="history_sync_notification"
    if node.tag == "notification"
        && node
            .attrs
            .get("type")
            .is_some_and(|v| v.as_str().as_ref() == "history_sync_notification")
    {
        // The node content is a protobuf-encoded wa::Message containing
        // a historySyncNotification field
        let Some(NodeContent::Bytes(proto_bytes)) = &node.content else {
            tracing::warn!("history_sync_notification node has no byte content");
            return None;
        };
        let proto_bytes = proto_bytes.clone();

        let wa_msg = match wa::Message::decode_from_slice(proto_bytes.as_slice()) {
            Ok(msg) => msg,
            Err(e) => {
                tracing::warn!("failed to decode history_sync_notification protobuf: {e}");
                return None;
            }
        };

        let notif = &wa_msg.protocol_message.history_sync_notification;
        return notif.initial_hist_bootstrap_inline_payload.clone();
    }

    // Form 2: ib node with <history_sync> child
    if node.tag == "ib"
        && let Some(hs_child) = node.get_optional_child("history_sync")
    {
        let Some(NodeContent::Bytes(b)) = &hs_child.content else {
            tracing::warn!("<history_sync> child has no byte content");
            return None;
        };
        return Some(b.clone());
    }

    None
}

/// Process an incoming `<success>` node and produce an [`Update::PairSuccess`].
///
/// The `<success>` node is received after QR or pair-code pairing completes.
/// The `lid` attribute carries the assigned LID (Lightweight ID).
#[allow(dead_code)]
pub(crate) fn handle_success(_node: &Node) -> Update {
    // Log the LID for debugging; the pairing flow already handles the full
    // pair-success processing. This handler just signals the application.
    Update::PairSuccess
}

/// Process an incoming `<failure>` or `<stream:error>` node and produce
/// the appropriate [`Update`] variant.
///
/// The node is inspected for:
/// - `<stream:error><conflict/>` -> [`Update::StreamReplaced`]
/// - Numeric error codes on `<failure>` or `<stream:error>` ->
///   [`Update::ConnectFailure`], [`Update::TemporaryBan`], or
///   [`Update::ClientOutdated`]
/// - `<stream:error>` without numeric code -> [`Update::StreamError`]
/// - Bare `<failure>` without code -> [`Update::LoggedOut`]
#[allow(dead_code)]
pub(crate) fn handle_failure(node: &Node) -> Update {
    // <stream:error><conflict/> means another device took over the connection.
    if node.tag == "stream:error" && node.get_optional_child("conflict").is_some() {
        return Update::StreamReplaced;
    }

    // Parse error code from the node (present on <failure> and some <stream:error>).
    if let Some(code_val) = node.attrs.get("code")
        && let Ok(code) = code_val.as_str().parse::<i32>()
    {
        // 402 = temporary ban
        if code == 402 {
            let expire = node
                .attrs
                .get("expire")
                .and_then(|v| v.as_str().parse::<u64>().ok())
                .unwrap_or(0);
            return Update::TemporaryBan(TemporaryBan { code, expire });
        }

        // 405 = client outdated
        if code == 405 {
            return Update::ClientOutdated;
        }

        let reason = ConnectFailureReason::from_i32(code);
        let message = node
            .get_optional_child("text")
            .and_then(Node::content_as_string)
            .map(|s| s.to_string())
            .unwrap_or_default();
        return Update::ConnectFailure(ConnectFailure { reason, message });
    }

    // <stream:error> without a numeric code -> StreamError
    if node.tag == "stream:error" {
        let code = node
            .attrs
            .get("code")
            .map(|v| v.as_str().to_string())
            .unwrap_or_default();
        return Update::StreamError(StreamError { code });
    }

    // Bare <failure> without code -> LoggedOut
    Update::LoggedOut
}

/// Process a server-initiated `<pair-code>` IQ (type="set") and produce
/// an [`Update::PairingCode`] with the code and timeout.
///
/// The server may push a pair-code response as a set IQ (like pair-device)
/// instead of responding directly to our set IQ. This handler catches those
/// push notifications.
#[allow(dead_code)]
pub(crate) fn handle_pair_code(node: &Node) -> Option<Update> {
    if node.tag != "iq" {
        return None;
    }
    let is_set = node
        .attrs
        .get("type")
        .is_some_and(|v| v.as_str().as_ref() == "set");
    if !is_set {
        return None;
    }

    let pair_code = node.get_optional_child("pair-code")?;
    let code = pair_code
        .attrs
        .get("code")
        .map(|v| v.as_str().to_string())
        .unwrap_or_default();
    let timeout = pair_code
        .attrs
        .get("timeout")
        .and_then(|v| v.as_str().parse::<u64>().ok())
        .unwrap_or(120);

    if code.is_empty() {
        tracing::warn!("pair-code IQ missing 'code' attribute");
        return None;
    }

    Some(Update::PairingCode { code, timeout })
}

/// App state collection names that the server uses in `<dirty>` notifications
/// and that we request via the `w:sync:app:state` IQ.
const APP_STATE_COLLECTIONS: &[&str] = &[
    "critical_block",
    "critical_unblock_low",
    "regular_low",
    "regular_high",
    "regular",
];

/// IQ spec for requesting an app state collection sync from the server.
///
/// Sends a `set` IQ to `w:sync:app:state` with a `<sync><collection>` child:
///
/// ```xml
/// <iq type="set" to="s.whatsapp.net" xmlns="w:sync:app:state">
///   <sync>
///     <collection name="{name}" version="{version}" return_snapshot="{snapshot}"/>
///   </sync>
/// </iq>
/// ```
#[allow(dead_code)]
struct AppStateSyncSpec {
    /// Collection name (e.g. "critical_block", "regular_high").
    name: String,
    /// Current local version; 0 means "request a full snapshot".
    version: u64,
}

impl AppStateSyncSpec {
    /// Create a spec for the given collection name, starting from version 0
    /// (requesting a snapshot). This is the initial-sync path.
    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: 0,
        }
    }

    /// Create a spec requesting patches after the given version.
    #[allow(dead_code)]
    fn after_version(name: impl Into<String>, version: u64) -> Self {
        Self {
            name: name.into(),
            version,
        }
    }

    /// Whether this request asks for a full snapshot (version == 0).
    fn wants_snapshot(&self) -> bool {
        self.version == 0
    }
}

impl IqSpec for AppStateSyncSpec {
    type Response = ();

    fn build_iq(&self) -> InfoQuery<'static> {
        let want_snapshot = self.wants_snapshot();

        let mut collection_builder = NodeBuilder::new("collection")
            .attr("name", self.name.as_str())
            .attr(
                "return_snapshot",
                if want_snapshot { "true" } else { "false" },
            );

        if !want_snapshot {
            collection_builder = collection_builder.attr("version", self.version);
        }

        let sync_node = NodeBuilder::new("sync")
            .children([collection_builder.build()])
            .build();

        InfoQuery::set(
            "w:sync:app:state",
            SERVER_JID
                .parse::<Jid>()
                .expect("SERVER_JID is a valid JID"),
            Some(NodeContent::Nodes(vec![sync_node])),
        )
    }

    fn parse_response(&self, _response: &NodeRef<'_>) -> std::result::Result<(), anyhow::Error> {
        Ok(())
    }
}

/// Process an incoming `<ib>` node from the server.
///
/// `<ib>` (information broadcast) nodes carry server-side inbox metadata.
/// The key children we handle:
///
/// - `<dirty type="..." timestamp="..."/>` - The server signals that a
///   category of data is stale. We respond by sending a "clean dirty bits"
///   IQ (`urn:xmpp:whatsapp:dirty`) to acknowledge, and if the type is
///   `syncd_app_state` we also request a full re-sync of all app state
///   collections. Without this handshake the server will not deliver
///   `<message>` nodes to the client.
///
/// - `<edge_routing><routing_info>...</routing_info></edge_routing>` -
///   Optimized reconnection routing data. We store it on the device for
///   use in subsequent handshake headers.
///
/// - `<offline_sync_preview count="..."/>` - Counts of offline messages.
///   Logged for debugging; a future version can emit an update event.
///
/// IQ sending is spawned as a background task so the update stream is not
/// blocked while waiting for server responses.
#[allow(dead_code)]
pub(crate) async fn handle_ib(node: &Node, client: &Client) -> Result<Option<Update>> {
    if let Some(children) = node.children() {
        for child in children {
            tracing::trace!("ib child: tag={}", child.tag);
        }
    }

    if let Some(children) = node.children() {
        for child in children {
            match child.tag.as_ref() {
                "dirty" => handle_dirty_child(child, client),
                "edge_routing" => handle_edge_routing_child(child, client),
                "offline_sync_preview" => {
                    let count = child
                        .attrs
                        .get("count")
                        .and_then(|v| v.as_str().parse::<u32>().ok())
                        .unwrap_or(0);
                    return Ok(Some(Update::OfflineSyncPreview { count }));
                }
                "offline" => {
                    let count = child
                        .attrs
                        .get("count")
                        .and_then(|v| v.as_str().parse::<u32>().ok())
                        .unwrap_or(0);
                    return Ok(Some(Update::OfflineSyncCompleted { count }));
                }
                "thread_metadata" => {
                    tracing::trace!("received thread metadata, ignoring");
                }
                _ => {
                    tracing::trace!(tag = %child.tag, "unhandled ib child");
                }
            }
        }
    }

    Ok(None)
}

/// Handle a `<dirty>` child of `<ib>`.
///
/// Parses the `type` and optional `timestamp` attributes, then spawns a
/// background task to:
/// 1. Send a "clean dirty bits" IQ to acknowledge the notification.
/// 2. If the type is `syncd_app_state`, request a full re-sync of all
///    app state collections.
fn handle_dirty_child(child: &Node, client: &Client) {
    let Some(dirty_type_str) = child.attrs.get("type").map(|v| v.as_str().to_string()) else {
        tracing::warn!("dirty notification missing 'type' attribute, skipping");
        return;
    };
    let timestamp_str = child.attrs.get("timestamp").map(|v| v.as_str().to_string());

    let bit = match DirtyBit::from_raw(&dirty_type_str, timestamp_str.as_deref()) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!("invalid dirty notification: {e}");
            return;
        }
    };

    let needs_app_state_resync = bit.dirty_type == DirtyType::SyncdAppState;

    tracing::trace!(
        dirty_type = %dirty_type_str,
        timestamp = ?bit.timestamp,
        needs_resync = needs_app_state_resync,
        "received dirty notification, sending clean IQ"
    );

    let handle = client.inner.handle.clone();
    let dirty_type = dirty_type_str.clone();
    let ts = bit.timestamp;

    compio::runtime::spawn(async move {
        // Acknowledge the dirty notification so the server knows we've seen it
        let clean_spec = match ts {
            Some(ts_val) => CleanDirtyBitsSpec::single(DirtyBit::with_timestamp(
                DirtyType::from(dirty_type.as_str()),
                ts_val,
            )),
            None => CleanDirtyBitsSpec::single(DirtyBit::new(DirtyType::from(dirty_type.as_str()))),
        };

        match handle.send_iq(clean_spec).await {
            Ok(()) => tracing::trace!("clean dirty bits IQ succeeded for type '{dirty_type}'"),
            Err(e) => tracing::warn!("clean dirty bits IQ failed for type '{dirty_type}': {e}"),
        }

        // syncd_app_state means all collections are stale; re-sync them
        if needs_app_state_resync {
            tracing::info!("syncd_app_state dirty - requesting full app state re-sync");
            for &name in APP_STATE_COLLECTIONS {
                let spec = AppStateSyncSpec::new(name);
                match handle.send_iq(spec).await {
                    Ok(()) => tracing::trace!("app state sync IQ succeeded for '{name}'"),
                    Err(e) => tracing::warn!("app state sync IQ failed for '{name}': {e}"),
                }
            }
        }
    })
    .detach();
}

/// Handle an `<edge_routing>` child of `<ib>`.
///
/// Extracts the `<routing_info>` byte content and stores it on the device
/// for use in subsequent Noise handshake headers (optimized reconnection).
fn handle_edge_routing_child(child: &Node, client: &Client) {
    let child_ref = child.as_node_ref();
    if let Some(routing_info_node) = child_ref.get_optional_child("routing_info")
        && let Some(routing_bytes) = routing_info_node.content_bytes()
        && !routing_bytes.is_empty()
    {
        tracing::trace!(
            bytes = routing_bytes.len(),
            "received edge routing info, storing for reconnection"
        );
        let routing_bytes = routing_bytes.to_vec();
        let device = client.inner.device.clone();

        compio::runtime::spawn(async move {
            let mut device_guard = device.write().await;
            device_guard.edge_routing_info = Some(routing_bytes);
        })
        .detach();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use wacore_binary::{Jid, builder::NodeBuilder};

    fn make_test_client() -> Client {
        use crate::ClientConfiguration;
        use waepic_connection::Connection;
        use waepic_session::MemorySession;

        let session = Arc::new(MemorySession::new());
        let config = ClientConfiguration::default();
        let (_runner, _event_tx, handle) =
            Connection::new(session.clone(), config.connection.clone());
        Client::new(handle, session, config)
    }

    #[test]
    fn success_node_produces_pair_success() {
        let node = NodeBuilder::new("success").attr("lid", "12345@lid").build();

        let update = handle_success(&node);
        assert!(
            matches!(update, Update::PairSuccess),
            "expected PairSuccess, got {update:?}"
        );
    }

    #[test]
    fn failure_node_produces_logged_out() {
        let node = NodeBuilder::new("failure").build();

        let update = handle_failure(&node);
        assert!(
            matches!(update, Update::LoggedOut),
            "expected LoggedOut, got {update:?}"
        );
    }

    #[test]
    fn stream_error_with_conflict_produces_stream_replaced() {
        let node = NodeBuilder::new("stream:error")
            .children([NodeBuilder::new("conflict").build()])
            .build();

        let update = handle_failure(&node);
        assert!(
            matches!(update, Update::StreamReplaced),
            "expected StreamReplaced, got {update:?}"
        );
    }

    #[test]
    fn stream_error_with_code_produces_connect_failure() {
        let node = NodeBuilder::new("stream:error").attr("code", "400").build();

        let update = handle_failure(&node);
        assert!(
            matches!(update, Update::ConnectFailure(_)),
            "expected ConnectFailure, got {update:?}"
        );
    }

    #[test]
    fn failure_with_temp_ban_code_produces_temporary_ban() {
        let node = NodeBuilder::new("failure")
            .attr("code", "402")
            .attr("expire", "86400")
            .build();

        let update = handle_failure(&node);
        assert!(
            matches!(
                update,
                Update::TemporaryBan(crate::update::TemporaryBan {
                    code: 402,
                    expire: 86400
                })
            ),
            "expected TemporaryBan, got {update:?}"
        );
    }

    #[test]
    fn failure_with_client_outdated_code() {
        let node = NodeBuilder::new("failure").attr("code", "405").build();

        let update = handle_failure(&node);
        assert!(
            matches!(update, Update::ClientOutdated),
            "expected ClientOutdated, got {update:?}"
        );
    }

    #[test]
    fn bare_stream_error_produces_stream_error() {
        let node = NodeBuilder::new("stream:error").build();

        let update = handle_failure(&node);
        assert!(
            matches!(update, Update::StreamError(StreamError { ref code }) if code.is_empty()),
            "expected StreamError with empty code, got {update:?}"
        );
    }

    #[test]
    fn picture_notification_remove_produces_picture_update() {
        let node = NodeBuilder::new("notification")
            .attr("type", "picture")
            .attr("from", "12345@s.whatsapp.net")
            .attr("t", "1700000000")
            .children([NodeBuilder::new("remove").build()])
            .build();

        let client = make_test_client();
        let update = handle_picture_notification(&node, &client);

        assert!(update.is_some(), "expected Some(PictureUpdate)");
        match update.unwrap() {
            Update::PictureUpdate(p) => {
                assert!(p.removed, "expected removed=true");
                assert!(p.picture_id.is_none());
            }
            other => panic!("expected PictureUpdate, got {other:?}"),
        }
    }

    #[test]
    fn picture_notification_set_produces_picture_update_with_id() {
        let node = NodeBuilder::new("notification")
            .attr("type", "picture")
            .attr("from", "12345@s.whatsapp.net")
            .attr("t", "1700000000")
            .children([NodeBuilder::new("set").attr("id", "pic123").build()])
            .build();

        let client = make_test_client();
        let update = handle_picture_notification(&node, &client);

        assert!(update.is_some());
        match update.unwrap() {
            Update::PictureUpdate(p) => {
                assert!(!p.removed);
                assert_eq!(p.picture_id.as_deref(), Some("pic123"));
            }
            other => panic!("expected PictureUpdate, got {other:?}"),
        }
    }

    /// Build a notification node with type="history_sync_notification"
    /// containing a protobuf-encoded Message with a HistorySyncNotification
    /// that carries the given inline payload bytes.
    fn make_history_sync_notification_node(inline_payload: Vec<u8>) -> Node {
        let notif = wa::message::HistorySyncNotification {
            initial_hist_bootstrap_inline_payload: Some(inline_payload),
            sync_type: Some(wa::message::HistorySyncType::InitialBootstrap),
            ..Default::default()
        };
        let proto_msg = wa::Message {
            protocol_message: wa::message::ProtocolMessage {
                history_sync_notification: Some(notif).into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        let encoded = proto_msg.encode_to_vec();

        NodeBuilder::new("notification")
            .attr("from", "s.whatsapp.net")
            .attr("type", "history_sync_notification")
            .attr("id", "hs-notif-1")
            .bytes(encoded)
            .build()
    }

    /// Build an <ib> node with a <history_sync> child containing the given bytes.
    fn make_ib_history_sync_node(payload: Vec<u8>) -> Node {
        NodeBuilder::new("ib")
            .attr("from", "s.whatsapp.net")
            .children([NodeBuilder::new("history_sync").bytes(payload).build()])
            .build()
    }

    #[test]
    fn extract_payload_from_notification_node() {
        let payload = vec![1, 2, 3, 4];
        let node = make_history_sync_notification_node(payload.clone());

        let extracted = extract_history_sync_payload(&node);
        assert_eq!(extracted, Some(payload));
    }

    #[test]
    fn extract_payload_from_ib_node() {
        let payload = vec![5, 6, 7, 8];
        let node = make_ib_history_sync_node(payload.clone());

        let extracted = extract_history_sync_payload(&node);
        assert_eq!(extracted, Some(payload));
    }

    #[test]
    fn extract_payload_returns_none_for_other_nodes() {
        let node = NodeBuilder::new("message")
            .attr("from", Jid::pn("12345"))
            .build();

        let extracted = extract_history_sync_payload(&node);
        assert!(extracted.is_none());
    }

    #[test]
    fn extract_payload_returns_none_for_ib_without_history_sync_child() {
        let node = NodeBuilder::new("ib")
            .attr("from", "s.whatsapp.net")
            .children([NodeBuilder::new("other").build()])
            .build();

        let extracted = extract_history_sync_payload(&node);
        assert!(extracted.is_none());
    }

    #[test]
    fn pair_code_set_iq_produces_pairing_code_update() {
        let node = NodeBuilder::new("iq")
            .attr("type", "set")
            .attr("id", "server-pc-1")
            .attr("from", "s.whatsapp.net")
            .children([NodeBuilder::new("pair-code")
                .attr("code", "ABCD-EFGH")
                .attr("timeout", "120")
                .build()])
            .build();

        let result = handle_pair_code(&node);
        let update = result.expect("should produce an update");

        match update {
            Update::PairingCode { code, timeout } => {
                assert_eq!(code, "ABCD-EFGH");
                assert_eq!(timeout, 120);
            }
            other => panic!("expected PairingCode, got {other:?}"),
        }
    }

    #[test]
    fn pair_code_default_timeout_when_missing() {
        let node = NodeBuilder::new("iq")
            .attr("type", "set")
            .attr("id", "server-pc-2")
            .children([NodeBuilder::new("pair-code")
                .attr("code", "WXYZ-1234")
                .build()])
            .build();

        let result = handle_pair_code(&node);
        let update = result.expect("should produce an update");

        match update {
            Update::PairingCode { code, timeout } => {
                assert_eq!(code, "WXYZ-1234");
                assert_eq!(timeout, 120); // default
            }
            other => panic!("expected PairingCode, got {other:?}"),
        }
    }

    #[test]
    fn pair_code_non_iq_returns_none() {
        let node = NodeBuilder::new("message")
            .children([NodeBuilder::new("pair-code").attr("code", "TEST").build()])
            .build();

        let result = handle_pair_code(&node);
        assert!(result.is_none());
    }

    #[test]
    fn pair_code_non_set_iq_returns_none() {
        let node = NodeBuilder::new("iq")
            .attr("type", "get")
            .children([NodeBuilder::new("pair-code").attr("code", "TEST").build()])
            .build();

        let result = handle_pair_code(&node);
        assert!(result.is_none());
    }

    #[test]
    fn pair_code_missing_code_attr_returns_none() {
        let node = NodeBuilder::new("iq")
            .attr("type", "set")
            .children([NodeBuilder::new("pair-code").attr("timeout", "60").build()])
            .build();

        let result = handle_pair_code(&node);
        assert!(result.is_none());
    }

    #[test]
    fn pair_code_no_pair_code_child_returns_none() {
        let node = NodeBuilder::new("iq")
            .attr("type", "set")
            .children([NodeBuilder::new("other").build()])
            .build();

        let result = handle_pair_code(&node);
        assert!(result.is_none());
    }

    #[test]
    fn app_state_sync_spec_new_requests_snapshot() {
        let spec = AppStateSyncSpec::new("critical_block");

        assert!(spec.wants_snapshot(), "version 0 should request snapshot");
        assert_eq!(spec.name, "critical_block");
        assert_eq!(spec.version, 0);
    }

    #[test]
    fn app_state_sync_spec_after_version_does_not_request_snapshot() {
        let spec = AppStateSyncSpec::after_version("regular_high", 42);

        assert!(
            !spec.wants_snapshot(),
            "version > 0 should not request snapshot"
        );
        assert_eq!(spec.name, "regular_high");
        assert_eq!(spec.version, 42);
    }

    #[test]
    fn app_state_sync_spec_build_iq_snapshot_has_correct_structure() {
        let spec = AppStateSyncSpec::new("critical_block");
        let iq = spec.build_iq();

        assert_eq!(iq.namespace, "w:sync:app:state");
        assert_eq!(iq.to, SERVER_JID.parse::<Jid>().unwrap());
        match iq.query_type {
            wacore::request::InfoQueryType::Set => {}
            other => panic!("expected Set, got {other:?}"),
        }

        let nodes = match &iq.content {
            Some(NodeContent::Nodes(n)) => n.clone(),
            other => panic!("expected NodeContent::Nodes, got {other:?}"),
        };
        assert_eq!(nodes.len(), 1, "should have one <sync> child");
        assert_eq!(nodes[0].tag, "sync");

        let collection = nodes[0]
            .get_optional_child("collection")
            .expect("should have <collection> child");
        assert_eq!(
            collection.attrs.get("name").map(|v| v.as_str().to_string()),
            Some("critical_block".to_string())
        );
        assert_eq!(
            collection
                .attrs
                .get("return_snapshot")
                .map(|v| v.as_str().to_string()),
            Some("true".to_string())
        );
        assert!(
            collection.attrs.get("version").is_none(),
            "snapshot request should not have version attr"
        );
    }

    #[test]
    fn app_state_sync_spec_build_iq_patch_has_version() {
        let spec = AppStateSyncSpec::after_version("regular", 15);
        let iq = spec.build_iq();

        let nodes = match &iq.content {
            Some(NodeContent::Nodes(n)) => n.clone(),
            _ => panic!("expected NodeContent::Nodes"),
        };

        let collection = nodes[0]
            .get_optional_child("collection")
            .expect("should have <collection> child");
        assert_eq!(
            collection
                .attrs
                .get("version")
                .map(|v| v.as_str().to_string()),
            Some("15".to_string())
        );
        assert_eq!(
            collection
                .attrs
                .get("return_snapshot")
                .map(|v| v.as_str().to_string()),
            Some("false".to_string())
        );
    }

    #[test]
    fn app_state_sync_spec_parse_response_succeeds_for_result() {
        let spec = AppStateSyncSpec::new("critical_block");
        let response = NodeBuilder::new("iq").attr("type", "result").build();

        let result = spec.parse_response(&response.as_node_ref());
        assert!(
            result.is_ok(),
            "parse_response should succeed for result IQ"
        );
    }
}
