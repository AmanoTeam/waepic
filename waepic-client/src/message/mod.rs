//! High-level message types: [`Message`], [`InputMessage`], and [`MessageInfo`].

pub mod input_message;
pub mod message;

pub use input_message::InputMessage;
pub use message::{Message, MessageInfo};
