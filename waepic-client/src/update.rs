//! Event types emitted by the client to the application.
//!
//! The main type is `Update`, an enum with one variant per event kind.
//! Supporting structs (`Receipt`, `Presence`, `ChatPresence`, etc.)
//! carry the event payload.

use crate::{
    message::Message,
    peer::{Chat, Jid},
};

pub use wacore_binary::Node;

/// High-level event emitted by the client to the application.
///
/// Each variant represents a meaningful occurrence during the WhatsApp
/// session lifecycle - connection state, incoming messages, receipts,
/// presence changes, pairing flow, and raw protocol access.
#[derive(Clone, Debug)]
pub enum Update {
    /// A new message arrived in a conversation.
    NewMessage(Message),
    /// An existing message was edited.
    MessageEdited(Message),
    /// One or more messages were deleted.
    MessageDeleted(MessageDeletion),
    /// Delivery/read receipts for one or more messages.
    Receipt(Receipt),
    /// A contact's online/offline presence changed.
    Presence(Presence),
    /// A contact's chat-activity state (composing / paused) changed.
    ChatPresence(ChatPresence),
    /// The WebSocket connection was established.
    Connected,
    /// The WebSocket connection was lost.
    Disconnected,
    /// The current session was invalidated (logged out server-side).
    LoggedOut,
    /// A chunk of history-sync data arrived (initial bootstrap or
    /// on-demand sync).
    HistorySync(HistorySyncChunk),
    /// A contact's profile information was updated.
    ContactUpdate(ContactUpdate),
    /// A group's metadata or participant list changed.
    GroupUpdate(GroupUpdate),
    /// A QR-code pairing URL to display.
    PairingQrCode {
        /// The pairing URL (whatsapp:// link).
        code: String,
        /// Validity duration in seconds.
        timeout: u64,
    },
    /// A numeric pairing code for phone-number linking.
    PairingCode {
        /// The 8-character code the user enters on their phone.
        code: String,
        /// Approximate validity duration in seconds.
        timeout: u64,
    },
    /// Pairing completed successfully.
    PairSuccess,
    /// History sync has fully completed (all chunks processed).
    HistorySyncCompleted,
    /// Raw decoded protocol node, forwarded before router dispatch.
    /// Only emitted when raw-node forwarding is enabled on the client.
    Raw(Node),
}

/// One or more messages deleted from a conversation.
#[derive(Clone, Debug)]
pub struct MessageDeletion {
    /// The chat the deleted messages belonged to.
    pub chat: Chat,
    /// IDs of the deleted messages.
    pub message_ids: Vec<String>,
}

/// Delivery or read receipt for one or more messages.
#[derive(Clone, Debug)]
pub struct Receipt {
    /// The chat the receipt applies to.
    pub chat: Chat,
    /// IDs of the messages the receipt confirms.
    pub message_ids: Vec<String>,
    /// Whether the receipt is a read or delivery confirmation.
    pub receipt_type: ReceiptType,
    /// Unix timestamp (seconds) of the receipt.
    pub timestamp: u64,
}

/// Simplified receipt type for v0.1.
#[derive(Clone, Debug)]
pub enum ReceiptType {
    /// The recipient has read the message(s).
    Read,
    /// The message(s) were delivered to the recipient's device.
    Delivered,
}

/// A contact's online/offline presence.
#[derive(Clone, Debug)]
pub struct Presence {
    /// The chat (contact) whose presence changed.
    pub chat: Chat,
    /// Whether the contact is currently online.
    pub available: bool,
    /// Unix timestamp (seconds) when the contact was last seen, if known.
    pub last_seen: Option<u64>,
}

/// A contact's chat-activity state (composing / paused).
#[derive(Clone, Debug)]
pub struct ChatPresence {
    /// The chat where the activity is happening.
    pub chat: Chat,
    /// The specific participant whose state changed (for groups).
    pub sender: Chat,
    /// The activity state.
    pub state: ChatPresenceState,
}

/// Chat-activity state.
#[derive(Clone, Debug)]
pub enum ChatPresenceState {
    /// The contact is typing a message.
    Composing,
    /// The contact stopped typing.
    Paused,
}

/// A chunk of history-sync data containing one or more conversations.
#[derive(Clone, Debug)]
pub struct HistorySyncChunk {
    /// Conversations included in this chunk.
    pub conversations: Vec<SyncedConversation>,
}

/// A single synced conversation from a history-sync chunk.
#[derive(Clone, Debug)]
pub struct SyncedConversation {
    /// The JID of the conversation.
    pub jid: Jid,
    /// The conversation name, if known.
    pub name: Option<String>,
    /// Messages in this conversation from the sync.
    pub messages: Vec<Message>,
    /// Whether this conversation is pinned.
    pub pinned: bool,
    /// Whether this conversation is archived.
    pub archived: bool,
    /// Whether this conversation is muted.
    pub muted: bool,
    /// Number of unread messages in this conversation.
    pub unread_count: u32,
}

/// A contact's profile information was updated.
#[derive(Clone, Debug)]
pub struct ContactUpdate {
    /// The JID of the contact whose profile changed.
    pub jid: Jid,
    /// The contact's business name, if updated.
    pub name: Option<String>,
    /// The contact's push name, if updated.
    pub push_name: Option<String>,
}

/// A group's metadata or participant list changed.
#[derive(Clone, Debug)]
pub struct GroupUpdate {
    /// The JID of the group that changed.
    pub jid: Jid,
    /// The new group name, if updated.
    pub name: Option<String>,
    /// Participants added or removed (depends on notification subtype).
    pub participants: Vec<Jid>,
}
