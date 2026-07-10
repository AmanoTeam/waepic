//! Event types emitted by the client to the application.
//!
//! The main type is `Update`, an enum with one variant per event kind.
//! Supporting structs (`Receipt`, `Presence`, `ChatPresence`, etc.)
//! carry the event payload.

use wacore::types::presence::ReceiptType;

use crate::{
    message::Message,
    peer::{Chat, Jid},
};

/// Re-export of the raw protocol node type.
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
    /// Server is about to deliver N offline messages.
    OfflineSyncPreview {
        /// Number of offline messages pending delivery.
        count: u32,
    },
    /// Offline sync finished, N messages delivered.
    OfflineSyncCompleted {
        /// Number of offline messages that were delivered.
        count: u32,
    },
    /// A pairing attempt failed (wrong code, timeout, etc.).
    PairError(PairError),
    /// Server asked to refresh an in-progress pairing code.
    PairingCodeRefresh {
        /// `true` when the server set `force_manual_refresh`.
        force_manual: bool,
    },
    /// QR code was scanned on the phone but multidevice is not supported.
    QrScannedWithoutMultidevice,
    /// The client version is outdated and must be updated.
    ClientOutdated,
    /// An inbound message could not be decrypted.
    UndecryptableMessage(UndecryptableMessage),
    /// A contact's profile picture was updated or removed.
    PictureUpdate(PictureUpdate),
    /// A contact's about/status text changed.
    UserAboutUpdate(UserAboutUpdate),
    /// A contact's profile changed (server notification).
    ContactUpdated(ContactUpdated),
    /// A contact changed their phone number.
    ContactNumberChanged(ContactNumberChanged),
    /// Server requests a full contact re-sync.
    ContactSyncRequested(ContactSyncRequested),
    /// A contact changed their push name.
    PushNameUpdate(PushNameUpdate),
    /// Our own push name was updated (from server or locally).
    SelfPushNameUpdated(SelfPushNameUpdated),
    /// A chat was pinned or unpinned.
    PinUpdate(ChatFlagUpdate),
    /// A chat was muted or unmuted.
    MuteUpdate(ChatFlagUpdate),
    /// A chat was archived or unarchived.
    ArchiveUpdate(ChatFlagUpdate),
    /// A message was starred or unstarred.
    StarUpdate(StarUpdate),
    /// A chat was marked as read or unread.
    MarkChatAsReadUpdate(ChatFlagUpdate),
    /// A chat was deleted.
    DeleteChatUpdate(DeleteChatUpdate),
    /// A chat's messages were cleared.
    ClearChatUpdate(ClearChatUpdate),
    /// A contact/group/newsletter's status updates were muted/unmuted.
    UserStatusMuteUpdate(UserStatusMuteUpdate),
    /// A message was deleted for me only.
    DeleteMessageForMeUpdate(DeleteMessageForMeUpdate),
    /// A label was created, renamed, or deleted.
    LabelEditUpdate(LabelEditUpdate),
    /// A label was associated with or removed from a chat.
    LabelAssociationUpdate(LabelAssociationUpdate),
    /// A user's device list changed (device added/removed/updated).
    DeviceListUpdate(DeviceListUpdate),
    /// A user's identity key changed (e.g. reinstalled WhatsApp).
    IdentityChange(IdentityChange),
    /// A business account's status changed.
    BusinessStatusUpdate(BusinessStatusUpdate),
    /// The server replaced our connection (another client connected).
    StreamReplaced,
    /// The account was temporarily banned.
    TemporaryBan(TemporaryBan),
    /// Connection failed with a reason code.
    ConnectFailure(ConnectFailure),
    /// A stream error occurred.
    StreamError(StreamError),
    /// A contact changed their default disappearing messages setting.
    DisappearingModeChanged(DisappearingModeChanged),
    /// A newsletter live update (reaction counts, etc.).
    NewsletterLiveUpdate(NewsletterLiveUpdate),
    /// A server-pushed MEX (GraphQL) notification.
    MexNotification(MexNotification),
    /// Server requested a WebAuthn assertion for companion linking.
    PairPasskeyRequest(PairPasskeyRequest),
    /// Passkey linking reached verification stage.
    PairPasskeyConfirmation(PairPasskeyConfirmation),
    /// Passkey linking failed.
    PairPasskeyError(PairPasskeyError),
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
    /// Sync progress percentage (0-100), if reported by the server.
    pub progress: Option<u32>,
    /// The type of history sync (e.g. `InitialBootstrap`, `Full`, `OnDemand`).
    pub sync_type: Option<i32>,
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

/// A pairing attempt failed.
#[derive(Clone, Debug)]
pub struct PairError {
    pub id: Jid,
    pub lid: Jid,
    pub business_name: String,
    pub platform: String,
    pub error: String,
}

/// An inbound message could not be decrypted.
#[derive(Clone, Debug)]
pub struct UndecryptableMessage {
    /// Sender JID, if available.
    pub sender: Option<Jid>,
    /// Chat JID, if available.
    pub chat: Option<Jid>,
    /// Whether the message content is unavailable (placeholder).
    pub is_unavailable: bool,
    /// The type of unavailability.
    pub unavailable_type: UnavailableType,
    /// How the client should handle decrypt failure.
    pub decrypt_fail_mode: DecryptFailMode,
}

/// Type of unavailability for undecryptable messages.
#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub enum UnavailableType {
    Unknown,
    ViewOnce,
    Hosted,
    Bot,
}

/// How the client should display a decrypt-failed message.
#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub enum DecryptFailMode {
    Show,
    Hide,
}

/// A contact's profile picture was updated or removed.
#[derive(Clone, Debug)]
pub struct PictureUpdate {
    pub jid: Jid,
    pub author: Option<Jid>,
    pub timestamp: u64,
    pub removed: bool,
    pub picture_id: Option<String>,
}

/// A contact's about/status text changed.
#[derive(Clone, Debug)]
pub struct UserAboutUpdate {
    pub jid: Jid,
    pub status: String,
    pub timestamp: u64,
}

/// A contact's profile changed (server notification).
#[derive(Clone, Debug)]
pub struct ContactUpdated {
    pub jid: Jid,
    pub timestamp: u64,
}

/// A contact changed their phone number.
#[derive(Clone, Debug)]
pub struct ContactNumberChanged {
    pub old_jid: Jid,
    pub new_jid: Jid,
    pub old_lid: Option<Jid>,
    pub new_lid: Option<Jid>,
    pub timestamp: u64,
}

/// Server requests a full contact re-sync.
#[derive(Clone, Debug)]
pub struct ContactSyncRequested {
    pub after: Option<u64>,
    pub timestamp: u64,
}

/// A contact changed their push name.
#[derive(Clone, Debug)]
pub struct PushNameUpdate {
    pub jid: Jid,
    pub old_push_name: String,
    pub new_push_name: String,
}

/// Our own push name was updated.
#[derive(Clone, Debug)]
pub struct SelfPushNameUpdated {
    pub from_server: bool,
    pub old_name: String,
    pub new_name: String,
}

/// Simple boolean flag update for a chat (pin/mute/archive/mark-as-read).
#[derive(Clone, Debug)]
pub struct ChatFlagUpdate {
    pub jid: Jid,
    pub timestamp: u64,
    pub on: bool,
    pub from_full_sync: bool,
}

/// A message was starred or unstarred.
#[derive(Clone, Debug)]
pub struct StarUpdate {
    pub chat_jid: Jid,
    pub participant_jid: Option<Jid>,
    pub message_id: String,
    pub from_me: bool,
    pub timestamp: u64,
    pub starred: bool,
    pub from_full_sync: bool,
}

/// A chat was deleted.
#[derive(Clone, Debug)]
pub struct DeleteChatUpdate {
    pub jid: Jid,
    pub timestamp: u64,
    pub delete_media: bool,
    pub from_full_sync: bool,
}

/// A chat's messages were cleared.
#[derive(Clone, Debug)]
pub struct ClearChatUpdate {
    pub jid: Jid,
    pub timestamp: u64,
    pub delete_starred: bool,
    pub delete_media: bool,
    pub from_full_sync: bool,
}

/// A contact/group/newsletter's status updates were muted/unmuted.
#[derive(Clone, Debug)]
pub struct UserStatusMuteUpdate {
    pub jid: Jid,
    pub muted: bool,
    pub timestamp: u64,
    pub from_full_sync: bool,
}

/// A message was deleted for me only.
#[derive(Clone, Debug)]
pub struct DeleteMessageForMeUpdate {
    pub chat_jid: Jid,
    pub participant_jid: Option<Jid>,
    pub message_id: String,
    pub from_me: bool,
    pub timestamp: u64,
    pub from_full_sync: bool,
}

/// A label was created, renamed, or deleted.
#[derive(Clone, Debug)]
pub struct LabelEditUpdate {
    pub label_id: String,
    pub timestamp: u64,
    pub deleted: bool,
    pub from_full_sync: bool,
}

/// A label was associated with or removed from a chat.
#[derive(Clone, Debug)]
pub struct LabelAssociationUpdate {
    pub label_id: String,
    pub chat_jid: Jid,
    pub timestamp: u64,
    pub labeled: bool,
    pub from_full_sync: bool,
}

/// A user's device list changed.
#[derive(Clone, Debug)]
pub struct DeviceListUpdate {
    pub user: Jid,
    pub lid_user: Option<Jid>,
    pub update_type: DeviceListUpdateType,
    pub devices: Vec<DeviceNotificationInfo>,
}

/// Type of device list update.
#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub enum DeviceListUpdateType {
    Add,
    Remove,
    Update,
}

/// Device information from a device list notification.
#[derive(Clone, Debug)]
pub struct DeviceNotificationInfo {
    pub device_id: u32,
    pub key_index: Option<u32>,
}

/// A user's identity key changed.
#[derive(Clone, Debug)]
pub struct IdentityChange {
    pub user: Jid,
    pub lid_user: Option<Jid>,
    pub implicit: bool,
}

/// A business account's status changed.
#[derive(Clone, Debug)]
pub struct BusinessStatusUpdate {
    pub jid: Jid,
    pub update_type: BusinessUpdateType,
    pub timestamp: u64,
    pub target_jid: Option<Jid>,
    pub verified_name: Option<String>,
}

/// Type of business status update.
#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub enum BusinessUpdateType {
    RemovedAsBusiness,
    VerifiedNameChanged,
    ProfileUpdated,
    ProductsUpdated,
    CollectionsUpdated,
    Unknown,
}

/// The account was temporarily banned.
#[derive(Clone, Debug)]
pub struct TemporaryBan {
    pub code: i32,
    /// Unix timestamp (seconds) when the ban expires.
    pub expire: u64,
}

/// Connection failed with a reason code.
#[derive(Clone, Debug)]
pub struct ConnectFailure {
    pub reason: ConnectFailureReason,
    pub message: String,
}

/// Reason for a connection failure.
#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub enum ConnectFailureReason {
    Generic,
    LoggedOut,
    TempBanned,
    AccountLocked,
    UnknownLogout,
    ClientOutdated,
    BadUserAgent,
    CatExpired,
    CatInvalid,
    NotFound,
    ClientUnknown,
    InternalServerError,
    Experimental,
    ServiceUnavailable,
    Unknown(i32),
}

impl ConnectFailureReason {
    pub fn from_i32(code: i32) -> Self {
        match code {
            400 => Self::Generic,
            401 => Self::LoggedOut,
            402 => Self::TempBanned,
            403 => Self::AccountLocked,
            405 => Self::ClientOutdated,
            406 => Self::UnknownLogout,
            409 => Self::BadUserAgent,
            413 => Self::CatExpired,
            414 => Self::CatInvalid,
            415 => Self::NotFound,
            418 => Self::ClientUnknown,
            500 => Self::InternalServerError,
            501 => Self::Experimental,
            503 => Self::ServiceUnavailable,
            _ => Self::Unknown(code),
        }
    }

    pub fn is_logged_out(&self) -> bool {
        matches!(
            self,
            Self::LoggedOut | Self::AccountLocked | Self::UnknownLogout
        )
    }
}

/// A stream error occurred.
#[derive(Clone, Debug)]
pub struct StreamError {
    pub code: String,
}

/// A contact changed their default disappearing messages setting.
#[derive(Clone, Debug)]
pub struct DisappearingModeChanged {
    pub from: Jid,
    pub duration: u32,
    pub setting_timestamp: u64,
}

/// A newsletter live update (reaction counts, etc.).
#[derive(Clone, Debug)]
pub struct NewsletterLiveUpdate {
    pub newsletter_jid: Jid,
    pub messages: Vec<NewsletterLiveUpdateMessage>,
}

/// A single message in a newsletter live update.
#[derive(Clone, Debug)]
pub struct NewsletterLiveUpdateMessage {
    pub server_id: u64,
    pub reactions: Vec<NewsletterLiveUpdateReaction>,
}

/// A reaction count in a newsletter live update.
#[derive(Clone, Debug)]
pub struct NewsletterLiveUpdateReaction {
    pub code: String,
    pub count: u64,
}

/// A server-pushed MEX (GraphQL) notification.
#[derive(Clone, Debug)]
pub struct MexNotification {
    pub op_name: String,
    pub from: Option<Jid>,
    pub stanza_id: Option<String>,
    pub offline: Option<String>,
}

/// Server requested a WebAuthn assertion for companion linking.
#[derive(Clone, Debug)]
pub struct PairPasskeyRequest {
    pub request_options_json: String,
}

/// Passkey linking reached verification stage.
#[derive(Clone, Debug)]
pub struct PairPasskeyConfirmation {
    pub code: String,
    pub skip_handoff_ux: bool,
}

/// Passkey linking failed.
#[derive(Clone, Debug)]
pub struct PairPasskeyError {
    pub error: String,
    pub continuation: bool,
}
