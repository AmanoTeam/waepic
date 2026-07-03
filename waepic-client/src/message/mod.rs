//! High-level message types: [`Message`], [`InputMessage`], and [`MessageInfo`].

pub mod input_message;
pub mod msg;

pub use input_message::InputMessage;
pub use msg::{Message, MessageInfo};
