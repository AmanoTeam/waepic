//! Cached chat record.

use wacore_binary::Jid;

/// A chat entry cached in the session.
#[derive(Clone, Debug)]
pub struct ChatEntry {
    /// The JID of the chat.
    pub jid: Jid,
    /// The display name of the chat, if known.
    pub name: Option<String>,
    /// The chat type: "user", "group", "newsletter", or "other".
    pub kind: String,
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use wacore_binary::Jid;

    pub fn test_jid() -> Jid {
        Jid::pn("5511999998888")
    }

    pub fn test_chat_entry() -> ChatEntry {
        ChatEntry {
            jid: test_jid(),
            name: Some("Test User".into()),
            kind: "user".into(),
        }
    }
}
