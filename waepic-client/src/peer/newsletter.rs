use wacore_binary::Jid;

use crate::Client;

/// A newsletter (channel) conversation.
#[derive(Clone, Debug)]
pub struct Newsletter {
    jid: Jid,
    name: Option<String>,
    #[allow(dead_code)]
    client: Client,
}

impl Newsletter {
    pub(crate) fn new(jid: Jid, client: Client) -> Self {
        Self {
            jid,
            name: None,
            client,
        }
    }

    /// The JID of this newsletter.
    pub fn id(&self) -> &Jid {
        &self.jid
    }

    /// The newsletter name, if known.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}
