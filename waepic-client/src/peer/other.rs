use wacore_binary::Jid;

use crate::Client;

/// A conversation with an unrecognised JID server type.
#[derive(Clone, Debug)]
pub struct OtherChat {
    jid: Jid,
    #[allow(dead_code)]
    pub(crate) client: Client,
}

impl OtherChat {
    pub(crate) fn new(jid: Jid, client: Client) -> Self {
        Self { jid, client }
    }

    /// The JID of this chat.
    pub fn jid(&self) -> &Jid {
        &self.jid
    }
}
