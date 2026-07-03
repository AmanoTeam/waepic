//! High-level message types: `Message`, `InputMessage`, and `MessageInfo`.

/// Input message builder for constructing outgoing messages.
pub mod input_message;
/// High-level message wrapper and metadata.
pub mod msg;

/// Re-export of the input message builder.
pub use input_message::InputMessage;
/// Re-export of the message and message-info types.
pub use msg::{Message, MessageInfo};
