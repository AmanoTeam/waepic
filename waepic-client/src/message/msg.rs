//! High-level message wrapper and metadata.
//!
//! `Message` bundles the raw protobuf `Message`, `MessageInfo` metadata,
//! the owning `Client`, and the `Chat` it belongs to.

use crate::{Chat, Client, InputMessage, Result};

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
            .or_else(|| self.raw.extended_text_message.text.as_deref())
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
        self.client.chat(self.info.source.sender.clone())
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
            .context_info
            .stanza_id
            .as_deref()
    }

    /// Send a new message to the same chat without replying to this message.
    pub async fn respond(&self, msg: impl Into<InputMessage>) -> Result<Message> {
        self.client
            .send_message(self.chat.clone(), msg.into())
            .await
    }

    /// Send a reply to this message.
    pub async fn reply(&self, msg: impl Into<InputMessage>) -> Result<Message> {
        let reply_msg: InputMessage = msg.into();
        let reply_msg = reply_msg.reply_to(Some(self.id().to_owned()));

        self.client.send_message(self.chat.clone(), reply_msg).await
    }

    /// Edit this message's text.
    pub async fn edit(&self, new_text: impl Into<InputMessage>) -> Result<()> {
        self.client
            .edit_message(self.chat.clone(), self.id(), new_text.into())
            .await
    }

    /// Delete this message for everyone.
    pub async fn delete(&self) -> Result<()> {
        self.client
            .delete_messages(self.chat.clone(), &[self.id()])
            .await
    }

    /// React to this message with an emoji.
    pub async fn react(&self, emoji: &str) -> Result<()> {
        self.client
            .send_reaction(self.chat.clone(), self.id(), emoji)
            .await
    }

    /// Mark this message as read.
    pub async fn mark_as_read(&self) -> Result<()> {
        self.client
            .mark_as_read(self.chat.clone(), &[self.id()])
            .await
    }
}
