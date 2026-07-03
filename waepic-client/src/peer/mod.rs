//! Types relating to WhatsApp chats: users, groups, newsletters, and more.

/// Group conversation type.
pub mod group;
/// Newsletter (channel) conversation type.
pub mod newsletter;
/// Other chat type for unrecognised JID servers.
pub mod other;
/// User (1:1 conversation) type.
pub mod user;

/// Re-export of the group conversation type.
pub use group::Group;
/// Re-export of the newsletter conversation type.
pub use newsletter::Newsletter;
/// Re-export of the other chat type.
pub use other::OtherChat;
/// Re-export of the user conversation type.
pub use user::User;

use std::fmt;

/// Re-export of JID types and utilities from `wacore_binary`.
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
