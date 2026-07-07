use wacore_binary::Jid;

use crate::{Client, InputMessage, Message, Result};

/// A group conversation.
#[derive(Clone, Debug)]
pub struct Group {
    jid: Jid,
    subject: Option<String>,
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

    /// Send a message to this group.
    pub async fn send_message<M: Into<InputMessage>>(&self, msg: M) -> Result<Message> {
        self.client.send_message(self.clone(), msg.into()).await
    }

    /// Mark messages as read in this group.
    pub async fn mark_as_read(&self, message_ids: &[&str]) -> Result<()> {
        self.client.mark_as_read(self.clone(), message_ids).await
    }
}
