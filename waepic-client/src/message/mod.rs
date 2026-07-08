//! High-level message types: `Message`, `InputMessage`, `InputMedia`, and `MessageInfo`.

/// Input media builder for constructing outgoing medias.
pub mod input_media;
/// Input message builder for constructing outgoing messages.
pub mod input_message;
/// High-level message wrapper and metadata.
pub mod msg;

/// Re-export of the input media type.
pub use input_media::InputMedia;
/// Re-export of the input message type.
pub use input_message::InputMessage;
/// Re-export of the message and message-info types.
pub use msg::{Message, MessageInfo};
