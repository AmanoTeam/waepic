//! Types relating to WhatsApp chats: users, groups, newsletters, and more.

pub mod group;
pub mod newsletter;
pub mod other;
pub mod user;

pub use group::Group;
pub use newsletter::Newsletter;
pub use other::OtherChat;
pub use user::User;

use std::fmt;

pub use wacore_binary::{Jid, JidExt, Server};

/// The universal conversation target.
#[derive(Clone, Debug)]
pub enum Chat {
    /// A private 1:1 conversation with a WhatsApp user.
    User(User),
    /// A group conversation.
    Group(Group),
    /// A newsletter (channel) conversation.
    Newsletter(Newsletter),
    /// A conversation with an unrecognised JID server type.
    Other(OtherChat),
}

impl Chat {
    /// The JID of this chat, regardless of variant.
    pub fn id(&self) -> &Jid {
        match self {
            Self::User(u) => u.id(),
            Self::Group(g) => g.id(),
            Self::Newsletter(n) => n.id(),
            Self::Other(o) => o.id(),
        }
    }

    /// Best-effort display name: User -> name or push_name, Group -> subject,
    /// Newsletter -> name, Other -> None.
    pub fn name(&self) -> Option<&str> {
        match self {
            Self::User(u) => u.name().or_else(|| u.push_name()),
            Self::Group(g) => g.subject(),
            Self::Newsletter(n) => n.name(),
            Self::Other(_) => None,
        }
    }

    /// Whether this chat is a private 1:1 user conversation.
    pub fn is_user(&self) -> bool {
        matches!(self, Self::User(_))
    }

    /// Whether this chat is a group conversation.
    pub fn is_group(&self) -> bool {
        matches!(self, Self::Group(_))
    }

    /// Whether this chat is a newsletter (channel).
    pub fn is_newsletter(&self) -> bool {
        matches!(self, Self::Newsletter(_))
    }
}

impl fmt::Display for Chat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.name() {
            Some(name) => f.write_str(name),
            None => fmt::Display::fmt(self.id(), f),
        }
    }
}

impl From<User> for Chat {
    fn from(user: User) -> Self {
        Self::User(user)
    }
}

impl From<Group> for Chat {
    fn from(group: Group) -> Self {
        Self::Group(group)
    }
}

impl From<Newsletter> for Chat {
    fn from(newsletter: Newsletter) -> Self {
        Self::Newsletter(newsletter)
    }
}

impl From<OtherChat> for Chat {
    fn from(other: OtherChat) -> Self {
        Self::Other(other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::test_client;

    #[test]
    fn jid_pn_maps_to_user() {
        let client = test_client();

        let chat = client.chat_from_jid(Jid::pn("12345"));
        assert!(chat.is_user());
    }

    #[test]
    fn jid_group_maps_to_group() {
        let client = test_client();

        let chat = client.chat_from_jid(Jid::group("123"));
        assert!(chat.is_group());
    }

    #[test]
    fn jid_newsletter_maps_to_newsletter() {
        let client = test_client();

        let chat = client.chat_from_jid(Jid::newsletter("xyz"));
        assert!(chat.is_newsletter());
    }

    #[test]
    fn user_from_new_produces_chat_user_variant() {
        let client = test_client();

        let user = User::new(Jid::pn("123"), client);
        let chat = Chat::from(user);
        assert!(chat.is_user());
    }

    #[test]
    fn chat_id_delegates_to_inner() {
        let client = test_client();

        let jid = Jid::pn("555");
        let chat = client.chat_from_jid(jid.clone());
        assert_eq!(chat.id(), &jid);
    }

    #[test]
    fn chat_name_falls_back_to_jid_display() {
        let client = test_client();

        let chat = client.chat_from_jid(Jid::pn("555"));
        assert_eq!(chat.to_string(), "555@s.whatsapp.net");
    }

    #[test]
    fn lid_jid_maps_to_user() {
        let client = test_client();

        let chat = client.chat_from_jid(Jid::lid("100000012345678"));
        assert!(chat.is_user());
    }

    #[test]
    fn broadcast_jid_maps_to_other() {
        let client = test_client();

        let chat = client.chat_from_jid(Jid::new("12345", Server::Broadcast));
        assert!(!chat.is_user());
        assert!(!chat.is_group());
        assert!(!chat.is_newsletter());
    }
}
