//! Message operations: send, edit, delete, forward, react, and mark as read.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use buffa::message::Message as _;
use chrono::Utc;
use wacore::{
    client::context::GroupInfo,
    send::{SignalStores, prepare_dm_stanza, prepare_group_stanza},
    types::message::{AddressingMode, MessageSource},
};
use wacore_binary::{Jid, JidExt, Node, builder::NodeBuilder};
use waproto::whatsapp as wa;

use crate::{
    Result,
    client::{Client, context::RuntimeHandle, signal_adapter::SignalProtocolStoreAdapter},
    error::ClientError,
    message::{InputMessage, Message, MessageInfo},
    peer::Chat,
};

/// Monotonic counter for generating unique message IDs.
static MSG_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate a WhatsApp-format message ID ("3EB0" + 18 hex chars).
pub(crate) fn generate_message_id() -> String {
    let count = MSG_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    format!("3EB0{timestamp:0>8x}{count:0>10x}")
}

/// Convert an [`InputMessage`] into a [`wa::Message`] protobuf.
fn input_to_proto(msg: &InputMessage) -> wa::Message {
    let text = msg.text.as_deref();

    if let Some(reply_to_id) = msg.reply_to.as_deref() {
        wa::Message {
            extended_text_message: wa::message::ExtendedTextMessage {
                text: text.map(ToString::to_string),
                context_info: wa::ContextInfo {
                    stanza_id: Some(reply_to_id.to_string()),
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }
    } else {
        wa::Message {
            conversation: text.map(ToString::to_string),
            ..Default::default()
        }
    }
}

/// Build a message stanza node for sending.
fn build_message_node(to: &Jid, msg_id: &str, proto: &wa::Message) -> Node {
    let encoded = proto.encode_to_vec();

    NodeBuilder::new("message")
        .attr("to", to)
        .attr("type", "text")
        .attr("id", msg_id)
        .children([NodeBuilder::new("plaintext").bytes(encoded).build()])
        .build()
}

/// Specifies who is revoking (deleting) the message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RevokeType {
    /// The message sender deleting their own message.
    Sender,
    /// A group admin deleting another user's message.
    Admin {
        /// JID of the user who originally sent the message being deleted.
        original_sender: Jid,
    },
}

/// Build a [`wa::Message`] protobuf for editing a message.
///
/// Constructs a `protocol_message` with type `MessageEdit`, referencing the
/// original message by key and carrying the replacement content.
fn build_edit_proto(
    chat_jid: &Jid,
    message_id: &str,
    new_content: &wa::Message,
    timestamp_ms: i64,
) -> wa::Message {
    wa::Message {
        protocol_message: wa::message::ProtocolMessage {
            key: wa::MessageKey {
                remote_jid: Some(chat_jid.to_string()),
                from_me: Some(true),
                id: Some(message_id.to_string()),
                participant: None,
            }
            .into(),
            r#type: Some(wa::message::protocol_message::Type::MessageEdit),
            edited_message: new_content.clone().into(),
            timestamp_ms: Some(timestamp_ms),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

/// Build a [`wa::Message`] protobuf for revoking (deleting) a message.
///
/// Constructs a `protocol_message` with type `Revoke`, referencing the
/// target message by key. `participant` is set for admin revokes to identify
/// the original sender.
fn build_revoke_proto(chat_jid: &Jid, message_id: &str, participant: Option<&str>) -> wa::Message {
    wa::Message {
        protocol_message: wa::message::ProtocolMessage {
            key: wa::MessageKey {
                remote_jid: Some(chat_jid.to_string()),
                from_me: Some(true),
                id: Some(message_id.to_string()),
                participant: participant.map(ToString::to_string),
            }
            .into(),
            r#type: Some(wa::message::protocol_message::Type::Revoke),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

/// Build a [`wa::Message`] protobuf for forwarding a message reference.
///
/// For v0.1, constructs an `extended_text_message` with forwarding context
/// info (`is_forwarded`, `forwarding_score`) referencing the original message.
/// Full content forwarding requires message storage (v0.2).
fn build_forward_proto(source_jid: &Jid, original_msg_id: &str) -> wa::Message {
    wa::Message {
        extended_text_message: wa::message::ExtendedTextMessage {
            text: Some("[Forwarded message]".to_string()),
            context_info: wa::ContextInfo {
                stanza_id: Some(original_msg_id.to_string()),
                remote_jid: Some(source_jid.to_string()),
                is_forwarded: Some(true),
                forwarding_score: Some(1),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

/// Build a reaction protobuf message for the given target message and emoji.
fn build_reaction_proto(chat_jid: &Jid, message_id: &str, emoji: &str) -> wa::Message {
    let timestamp_ms = Utc::now().timestamp_millis();

    wa::Message {
        reaction_message: wa::message::ReactionMessage {
            key: wa::MessageKey {
                remote_jid: Some(chat_jid.to_string()),
                from_me: Some(true),
                id: Some(message_id.to_string()),
                participant: None,
            }
            .into(),
            text: Some(emoji.to_string()),
            sender_timestamp_ms: Some(timestamp_ms),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

/// Build a reaction stanza node for sending.
fn build_reaction_node(to: &Jid, msg_id: &str, proto: &wa::Message) -> Node {
    let encoded = proto.encode_to_vec();

    NodeBuilder::new("message")
        .attr("to", to)
        .attr("type", "text")
        .attr("id", msg_id)
        .children([NodeBuilder::new("plaintext").bytes(encoded).build()])
        .build()
}

/// Build a read receipt (`<receipt type="read">`) node.
fn build_read_receipt_node(to: &Jid, message_ids: &[&str]) -> Node {
    let timestamp = Utc::now().timestamp().to_string();

    NodeBuilder::new("receipt")
        .attr("to", to)
        .attr("type", "read")
        .attr("id", message_ids[0])
        .attr("t", &timestamp)
        .build()
}

impl Client {
    /// Send a message to a chat.
    ///
    /// For newsletters, sends plaintext. For DMs and groups, encrypts via
    /// the Signal protocol using wacore's `prepare_dm_stanza` and
    /// `prepare_group_stanza`.
    #[tracing::instrument(skip(self, chat))]
    pub async fn send_message<C: Into<Chat>>(
        &self,
        chat: C,
        message: InputMessage,
    ) -> Result<Message> {
        let chat = chat.into();
        let jid = chat.id().clone();
        let msg_id = generate_message_id();
        let proto = input_to_proto(&message);

        // Newsletters are plaintext channels - keep the existing path.
        if chat.is_newsletter() {
            let node = build_message_node(&jid, &msg_id, &proto);
            self.inner.handle.send_node(node).await?;

            let now = Utc::now();
            let info = MessageInfo {
                id: msg_id.clone(),
                source: MessageSource {
                    chat: jid.clone(),
                    sender: jid,
                    is_from_me: true,
                    ..Default::default()
                },
                timestamp: now,
                ..Default::default()
            };

            return Ok(Message::new(proto, info, self.clone(), chat));
        }

        let device = self.inner.device.read().await;
        let own_jid = device.pn.clone().ok_or(ClientError::NotLoggedIn)?;
        let own_lid = device.lid.clone();
        let account = device.account.clone();

        let runtime = RuntimeHandle::new();
        let backend = Arc::clone(&self.inner.session);
        let mut adapter = SignalProtocolStoreAdapter::new(
            Arc::clone(&self.inner.device),
            Arc::clone(&self.inner.signal_cache),
            backend,
        );

        let mut stores = SignalStores {
            sender_key_store: &mut adapter.sender_key_store,
            session_store: &mut adapter.session_store,
            identity_store: &mut adapter.identity_store,
            prekey_store: &mut adapter.pre_key_store,
            signed_prekey_store: &adapter.signed_pre_key_store,
        };

        let node = if jid.is_group() {
            let group_info = Arc::new(GroupInfo::new(vec![], AddressingMode::Pn));
            let own_lid = own_lid
                .ok_or_else(|| ClientError::Internal("LID not set, cannot send to group".into()))?;

            let prepared = prepare_group_stanza(
                &runtime,
                &mut stores,
                self,
                &group_info,
                &own_jid,
                &own_lid,
                account.as_deref(),
                jid.clone(),
                &proto,
                msg_id.clone(),
                false,
                None,
                None,
                None,
                &[],
                None,
            )
            .await
            .map_err(|e| ClientError::EncryptSend(format!("group encrypt failed: {e}")))?;

            prepared.node
        } else {
            let prepared = prepare_dm_stanza(
                &runtime,
                &mut stores,
                self,
                &own_jid,
                own_lid.as_ref(),
                account.as_deref(),
                jid.clone(),
                &proto,
                msg_id.clone(),
                None,
                &[],
                vec![],
                None,
            )
            .await
            .map_err(|e| ClientError::EncryptSend(format!("dm encrypt failed: {e}")))?;

            prepared.node
        };

        drop(device);
        self.inner.handle.send_node(node).await?;

        let now = Utc::now();
        let info = MessageInfo {
            id: msg_id.clone(),
            source: MessageSource {
                chat: jid.clone(),
                sender: jid,
                is_from_me: true,
                ..Default::default()
            },
            timestamp: now,
            ..Default::default()
        };

        Ok(Message::new(proto, info, self.clone(), chat))
    }

    /// Send a reaction to a message.
    #[tracing::instrument(skip(self, chat))]
    pub async fn send_reaction<C: Into<Chat>>(
        &self,
        chat: C,
        message_id: &str,
        emoji: &str,
    ) -> Result<()> {
        let chat = chat.into();
        let jid = chat.id().clone();
        let msg_id = generate_message_id();
        let proto = build_reaction_proto(&jid, message_id, emoji);
        let node = build_reaction_node(&jid, &msg_id, &proto);

        self.inner.handle.send_node(node).await?;

        Ok(())
    }

    /// Mark messages as read in a chat.
    #[tracing::instrument(skip(self, chat))]
    pub async fn mark_as_read<C: Into<Chat>>(&self, chat: C, message_ids: &[&str]) -> Result<()> {
        let chat = chat.into();
        if message_ids.is_empty() {
            return Ok(());
        }

        let jid = chat.id().clone();
        let node = build_read_receipt_node(&jid, message_ids);

        self.inner.handle.send_node(node).await?;

        Ok(())
    }

    /// Edit a previously sent message.
    ///
    /// Sends a protocol message of type `MessageEdit` referencing the original
    /// message and carrying the replacement text content.
    #[tracing::instrument(skip(self, chat))]
    pub async fn edit_message<C: Into<Chat>>(
        &self,
        chat: C,
        message_id: &str,
        new_text: InputMessage,
    ) -> Result<()> {
        let chat = chat.into();
        let jid = chat.id().clone();
        let new_msg_id = generate_message_id();
        let new_content = input_to_proto(&new_text);
        let timestamp_ms = Utc::now().timestamp_millis();

        let proto = build_edit_proto(&jid, message_id, &new_content, timestamp_ms);
        let node = build_message_node(&jid, &new_msg_id, &proto);

        self.inner.handle.send_node(node).await?;

        Ok(())
    }

    /// Delete messages for everyone.
    ///
    /// Sends a revoke protocol message for each message ID in the list.
    #[tracing::instrument(skip(self, chat))]
    pub async fn delete_messages<C: Into<Chat>>(
        &self,
        chat: C,
        message_ids: &[&str],
    ) -> Result<()> {
        let chat = chat.into();
        for msg_id in message_ids {
            self.revoke_message(chat.clone(), msg_id, RevokeType::Sender)
                .await?;
        }
        Ok(())
    }

    /// Revoke a single message (sender or admin-initiated deletion).
    ///
    /// Sends a protocol message of type `Revoke` for the given message.
    /// `revoke_type` determines whether this is a sender-initiated or admin-initiated
    /// deletion.
    #[tracing::instrument(skip(self, chat))]
    pub async fn revoke_message<C: Into<Chat>>(
        &self,
        chat: C,
        message_id: &str,
        revoke_type: RevokeType,
    ) -> Result<()> {
        let chat = chat.into();
        let jid = chat.id().clone();
        let new_msg_id = generate_message_id();

        let participant = match &revoke_type {
            RevokeType::Sender => None,
            RevokeType::Admin { original_sender } => Some(original_sender.to_string()),
        };

        let proto = build_revoke_proto(&jid, message_id, participant.as_deref());
        let node = build_message_node(&jid, &new_msg_id, &proto);

        self.inner.handle.send_node(node).await?;

        Ok(())
    }

    /// Forward messages to another chat.
    ///
    /// For v0.1, sends a placeholder forwarded message to the destination chat
    /// with forwarding context info referencing each original message.
    /// Full content forwarding requires message storage (v0.2).
    #[tracing::instrument(skip(self, dest, source))]
    pub async fn forward_messages<D: Into<Chat>, S: Into<Chat>>(
        &self,
        dest: D,
        message_ids: &[&str],
        source: S,
    ) -> Result<()> {
        let dest = dest.into();
        let source = source.into();
        let dest_jid = dest.id().clone();
        let source_jid = source.id().clone();

        for msg_id in message_ids {
            let new_id = generate_message_id();
            let proto = build_forward_proto(&source_jid, msg_id);
            let node = build_message_node(&dest_jid, &new_id, &proto);

            self.inner.handle.send_node(node).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;

    #[test]
    fn generate_message_id_has_correct_prefix() {
        let id = generate_message_id();

        assert!(
            id.starts_with("3EB0"),
            "message ID should start with 3EB0, got: {id}"
        );
    }

    #[test]
    fn generate_message_id_has_correct_length() {
        let id = generate_message_id();

        assert_eq!(
            id.len(),
            22,
            "message ID should be 22 chars, got {id}: '{id}'"
        );
    }

    #[test]
    fn generate_message_id_is_unique() {
        let id1 = generate_message_id();
        let id2 = generate_message_id();

        assert_ne!(id1, id2, "consecutive message IDs should be unique");
    }

    #[test]
    fn input_to_proto_simple_text() {
        let msg = InputMessage::text("hello world");
        let proto = input_to_proto(&msg);

        assert_eq!(proto.conversation.as_deref(), Some("hello world"));
        assert!(proto.extended_text_message.text.is_none());
    }

    #[test]
    fn input_to_proto_empty_text() {
        let msg = InputMessage::empty();
        let proto = input_to_proto(&msg);

        assert!(proto.conversation.is_none());
        assert!(proto.extended_text_message.text.is_none());
    }

    #[test]
    fn input_to_proto_reply() {
        let msg = InputMessage::text("a reply").reply_to(Some("ORIGINAL_MSG_ID"));
        let proto = input_to_proto(&msg);
        assert!(proto.conversation.is_none());

        let etm = &proto.extended_text_message;
        assert_eq!(etm.text.as_deref(), Some("a reply"));

        let ctx = &etm.context_info;
        assert_eq!(ctx.stanza_id.as_deref(), Some("ORIGINAL_MSG_ID"));
    }

    #[test]
    fn input_to_proto_reply_without_text() {
        let msg = InputMessage::empty().reply_to(Some("ORIGINAL_MSG_ID"));
        let proto = input_to_proto(&msg);
        assert!(proto.conversation.is_none());

        let etm = &proto.extended_text_message;
        assert!(etm.text.is_none());

        let ctx = &etm.context_info;
        assert_eq!(ctx.stanza_id.as_deref(), Some("ORIGINAL_MSG_ID"));
    }

    #[test]
    fn build_message_node_has_correct_structure() {
        let jid = Jid::pn("12345");
        let proto = wa::Message {
            conversation: Some("test".to_string()),
            ..Default::default()
        };
        let node = build_message_node(&jid, "3EB0TEST", &proto);

        assert_eq!(node.tag, "message");
        assert_eq!(
            node.attrs.get("to").map(|v| v.as_str()),
            Some(Cow::Borrowed("12345@s.whatsapp.net"))
        );
        assert_eq!(
            node.attrs.get("type").map(|v| v.as_str()),
            Some(Cow::Borrowed("text"))
        );
        assert_eq!(
            node.attrs.get("id").map(|v| v.as_str()),
            Some(Cow::Borrowed("3EB0TEST"))
        );
    }

    #[test]
    fn build_reaction_proto_has_correct_structure() {
        let jid = Jid::pn("12345");
        let proto = build_reaction_proto(&jid, "TARGET_MSG_ID", ":heart:️");

        let reaction = &proto.reaction_message;
        assert_eq!(reaction.text.as_deref(), Some(":heart:️"));

        let key = &reaction.key;
        assert_eq!(key.remote_jid.as_deref(), Some("12345@s.whatsapp.net"));
        assert_eq!(key.from_me, Some(true));
        assert_eq!(key.id.as_deref(), Some("TARGET_MSG_ID"));
        assert!(key.participant.is_none());
        assert!(reaction.sender_timestamp_ms.is_some());
    }

    #[test]
    fn build_reaction_node_has_correct_structure() {
        let jid = Jid::pn("12345");
        let proto = build_reaction_proto(&jid, "TARGET_MSG_ID", ":+1:");
        let node = build_reaction_node(&jid, "3EB0REACT", &proto);

        assert_eq!(node.tag, "message");
        assert_eq!(
            node.attrs.get("to").map(|v| v.as_str()),
            Some(Cow::Borrowed("12345@s.whatsapp.net"))
        );
        assert_eq!(
            node.attrs.get("type").map(|v| v.as_str()),
            Some(Cow::Borrowed("text"))
        );
        assert_eq!(
            node.attrs.get("id").map(|v| v.as_str()),
            Some(Cow::Borrowed("3EB0REACT"))
        );

        let plaintext = node
            .get_optional_child("plaintext")
            .expect("should have plaintext child");
        assert!(plaintext.content.is_some(), "plaintext should have content");
    }

    #[test]
    fn build_read_receipt_node_has_correct_structure() {
        let jid = Jid::pn("12345");
        let node = build_read_receipt_node(&jid, &["MSG_ID_001"]);

        assert_eq!(node.tag, "receipt");
        assert_eq!(
            node.attrs.get("to").map(|v| v.as_str()),
            Some(Cow::Borrowed("12345@s.whatsapp.net"))
        );
        assert_eq!(
            node.attrs.get("type").map(|v| v.as_str()),
            Some(Cow::Borrowed("read"))
        );
        assert_eq!(
            node.attrs.get("id").map(|v| v.as_str()),
            Some(Cow::Borrowed("MSG_ID_001"))
        );
        assert!(node.attrs.get("t").is_some(), "should have timestamp");
    }

    #[test]
    fn build_read_receipt_node_for_group() {
        use std::borrow::Cow;
        let jid = Jid::group("123456789");
        let node = build_read_receipt_node(&jid, &["GRP_MSG_001"]);

        assert_eq!(node.tag, "receipt");
        assert_eq!(
            node.attrs.get("to").map(|v| v.as_str()),
            Some(Cow::Borrowed("123456789@g.us"))
        );
        assert_eq!(
            node.attrs.get("type").map(|v| v.as_str()),
            Some(Cow::Borrowed("read"))
        );
    }

    #[test]
    fn build_edit_proto_has_correct_protocol_message_type() {
        let jid = Jid::pn("12345");
        let new_content = wa::Message {
            conversation: Some("edited text".to_string()),
            ..Default::default()
        };
        let proto = build_edit_proto(&jid, "ORIGINAL_ID", &new_content, 1719000000000);

        let pm = &proto.protocol_message;
        assert_eq!(
            pm.r#type,
            Some(wa::message::protocol_message::Type::MessageEdit)
        );
    }

    #[test]
    fn build_edit_proto_has_correct_key() {
        let jid = Jid::pn("12345");
        let new_content = wa::Message {
            conversation: Some("edited text".to_string()),
            ..Default::default()
        };
        let proto = build_edit_proto(&jid, "ORIGINAL_ID", &new_content, 1719000000000);

        let pm = &proto.protocol_message;
        let key = &pm.key;
        assert_eq!(key.remote_jid.as_deref(), Some("12345@s.whatsapp.net"));
        assert_eq!(key.from_me, Some(true));
        assert_eq!(key.id.as_deref(), Some("ORIGINAL_ID"));
        assert!(key.participant.is_none());
    }

    #[test]
    fn build_edit_proto_carries_edited_content() {
        let jid = Jid::pn("12345");
        let new_content = wa::Message {
            conversation: Some("edited text".to_string()),
            ..Default::default()
        };
        let proto = build_edit_proto(&jid, "ORIGINAL_ID", &new_content, 1719000000000);

        let pm = &proto.protocol_message;
        let edited = &pm.edited_message;
        assert_eq!(edited.conversation.as_deref(), Some("edited text"));
    }

    #[test]
    fn build_edit_proto_has_timestamp() {
        let jid = Jid::pn("12345");
        let new_content = wa::Message::default();
        let proto = build_edit_proto(&jid, "ORIGINAL_ID", &new_content, 1719000000000);

        let pm = &proto.protocol_message;
        assert_eq!(pm.timestamp_ms, Some(1719000000000));
    }

    #[test]
    fn build_revoke_proto_has_correct_protocol_message_type() {
        let jid = Jid::pn("12345");
        let proto = build_revoke_proto(&jid, "MSG_TO_DELETE", None);

        let pm = &proto.protocol_message;
        assert_eq!(pm.r#type, Some(wa::message::protocol_message::Type::Revoke));
    }

    #[test]
    fn build_revoke_proto_has_correct_key() {
        let jid = Jid::pn("12345");
        let proto = build_revoke_proto(&jid, "MSG_TO_DELETE", None);

        let pm = &proto.protocol_message;
        let key = &pm.key;
        assert_eq!(key.remote_jid.as_deref(), Some("12345@s.whatsapp.net"));
        assert_eq!(key.from_me, Some(true));
        assert_eq!(key.id.as_deref(), Some("MSG_TO_DELETE"));
        assert!(key.participant.is_none());
    }

    #[test]
    fn build_revoke_proto_sender_has_no_participant() {
        let jid = Jid::pn("12345");
        let proto = build_revoke_proto(&jid, "MSG_TO_DELETE", None);

        let pm = &proto.protocol_message;
        let key = &pm.key;
        assert!(key.participant.is_none());
    }

    #[test]
    fn build_revoke_proto_admin_has_participant() {
        let jid = Jid::pn("12345");
        let proto = build_revoke_proto(&jid, "MSG_TO_DELETE", Some("99999@s.whatsapp.net"));

        let pm = &proto.protocol_message;
        let key = &pm.key;
        assert_eq!(key.participant.as_deref(), Some("99999@s.whatsapp.net"));
    }

    #[test]
    fn build_revoke_proto_has_no_edited_message() {
        let jid = Jid::pn("12345");
        let proto = build_revoke_proto(&jid, "MSG_TO_DELETE", None);

        let pm = &proto.protocol_message;
        assert_eq!(pm.edited_message, wa::Message::default().into());
    }

    #[test]
    fn build_forward_proto_has_forwarding_context() {
        let source_jid = Jid::pn("11111");
        let proto = build_forward_proto(&source_jid, "ORIGINAL_MSG");

        let etm = &proto.extended_text_message;
        let ctx = &etm.context_info;
        assert_eq!(ctx.is_forwarded, Some(true));
        assert_eq!(ctx.forwarding_score, Some(1));
        assert_eq!(ctx.stanza_id.as_deref(), Some("ORIGINAL_MSG"));
        assert_eq!(ctx.remote_jid.as_deref(), Some("11111@s.whatsapp.net"));
    }

    #[test]
    fn build_forward_proto_has_placeholder_text() {
        let source_jid = Jid::pn("11111");
        let proto = build_forward_proto(&source_jid, "ORIGINAL_MSG");

        let etm = &proto.extended_text_message;
        assert_eq!(etm.text.as_deref(), Some("[Forwarded message]"));
    }

    #[test]
    fn build_forward_proto_has_no_conversation_field() {
        let source_jid = Jid::pn("11111");
        let proto = build_forward_proto(&source_jid, "ORIGINAL_MSG");

        assert!(proto.conversation.is_none());
    }

    #[test]
    fn revoke_type_sender_variant() {
        let rt = RevokeType::Sender;
        assert!(matches!(rt, RevokeType::Sender));
    }

    #[test]
    fn revoke_type_admin_variant() {
        let rt = RevokeType::Admin {
            original_sender: Jid::pn("99999"),
        };
        assert!(matches!(rt, RevokeType::Admin { .. }));
    }
}
