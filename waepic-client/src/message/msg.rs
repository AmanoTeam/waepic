//! High-level message wrapper and metadata.
//!
//! `Message` bundles the raw protobuf `Message`, `MessageInfo` metadata,
//! the owning `Client`, and the `Chat` it belongs to.

use crate::{Chat, Client, InputMessage, Result};

/// Re-export of message metadata from `wacore`.
pub use wacore::types::message::MessageInfo;

/// A high-level WhatsApp message, wrapping the protobuf `Message` and
/// `MessageInfo` metadata together with the owning `Client` and `Chat`.
#[derive(Clone, Debug)]
pub struct Message {
    raw: waproto::whatsapp::Message,
    info: MessageInfo,
    client: Client,
    chat: Chat,
}

impl Message {
    /// Create a new `Message` from its raw parts.
    #[allow(dead_code)]
    pub(crate) fn new(
        raw: waproto::whatsapp::Message,
        info: MessageInfo,
        client: Client,
        chat: Chat,
    ) -> Self {
        Self {
            raw,
            info,
            client,
            chat,
        }
    }

    /// The server-assigned message ID.
    pub fn id(&self) -> &str {
        self.info.id.as_str()
    }

    /// The plain-text body of the message, if any.
    ///
    /// Checks `conversation` first (simple text messages), then falls back to
    /// `extended_text_message.text` (link previews, formatted text, etc.).
    pub fn text(&self) -> Option<&str> {
        self.raw
            .conversation
            .as_deref()
            .or_else(|| self.raw.extended_text_message.as_ref()?.text.as_deref())
    }

    /// Whether this message was sent by the current user.
    pub fn outgoing(&self) -> bool {
        self.info.source.is_from_me
    }

    /// The chat this message belongs to.
    pub fn chat(&self) -> &Chat {
        &self.chat
    }

    /// The sender of this message, constructed from the sender JID.
    ///
    /// For outgoing messages this returns the current user's own chat
    /// representation; for incoming messages it returns the peer who sent it.
    pub fn sender(&self) -> Chat {
        self.client.chat_from_jid(self.info.source.sender.clone())
    }

    /// Unix timestamp (seconds) when the message was originally sent.
    pub fn date(&self) -> u64 {
        self.info.timestamp.timestamp() as u64
    }

    /// Unix timestamp (seconds) when the message was last edited, if ever.
    pub fn edit_date(&self) -> Option<u64> {
        let _ = &self.raw.edited_message;
        None
    }

    /// If this message is a reply to another message, return the target
    /// message's ID (stanza_id from extended_text_message.context_info).
    pub fn reply_to_id(&self) -> Option<&str> {
        self.raw
            .extended_text_message
            .as_ref()?
            .context_info
            .as_ref()?
            .stanza_id
            .as_deref()
    }

    /// Send a new message to the same chat without replying to this message.
    pub async fn respond(&self, _msg: impl Into<InputMessage>) -> Result<Message> {
        todo!()
    }

    /// Send a reply to this message.
    pub async fn reply(&self, _msg: impl Into<InputMessage>) -> Result<Message> {
        todo!()
    }

    /// Edit this message's text.
    pub async fn edit(&self, _new_text: impl Into<InputMessage>) -> Result<()> {
        todo!()
    }

    /// Delete this message for everyone.
    pub async fn delete(&self) -> Result<()> {
        todo!()
    }

    /// React to this message with an emoji.
    pub async fn react(&self, _emoji: &str) -> Result<()> {
        todo!()
    }

    /// Mark this message as read.
    pub async fn mark_as_read(&self) -> Result<()> {
        todo!()
    }
}
