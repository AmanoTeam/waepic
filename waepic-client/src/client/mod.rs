use wacore_binary::{Jid, JidExt, Server};

use crate::peer::{Chat, Group, Newsletter, OtherChat, User};

/// The main WhatsApp client handle.
#[derive(Clone, Debug)]
pub struct Client;

impl Client {
    /// Map a JID to the appropriate [`Chat`] variant based on its server type.
    #[allow(dead_code)]
    pub(crate) fn chat_from_jid(&self, jid: Jid) -> Chat {
        match jid.server() {
            Server::Pn | Server::Lid => Chat::User(User::new(jid, self.clone())),
            Server::Group => Chat::Group(Group::new(jid, self.clone())),
            Server::Newsletter => Chat::Newsletter(Newsletter::new(jid, self.clone())),
            _ => Chat::Other(OtherChat::new(jid, self.clone())),
        }
    }
}

/// Create a test [`Client`] instance for use in unit tests.
#[cfg(test)]
pub(crate) fn test_client() -> Client {
    Client
}
