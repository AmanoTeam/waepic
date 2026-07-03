use wacore_binary::Jid;

use crate::Client;

/// A group conversation.
#[derive(Clone, Debug)]
pub struct Group {
    jid: Jid,
    subject: Option<String>,
    #[allow(dead_code)]
    pub(crate) client: Client,
}

impl Group {
    pub(crate) fn new(jid: Jid, client: Client) -> Self {
        Self {
            jid,
            subject: None,
            client,
        }
    }

    /// The JID of this group.
    pub fn id(&self) -> &Jid {
        &self.jid
    }

    /// The group subject (name), if known.
    pub fn subject(&self) -> Option<&str> {
        self.subject.as_deref()
    }
}
