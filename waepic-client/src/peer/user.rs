use wacore_binary::Jid;

use crate::Client;

/// A private 1:1 conversation with a WhatsApp user.
#[derive(Clone, Debug)]
pub struct User {
    jid: Jid,
    name: Option<String>,
    push_name: Option<String>,
    phone_number: Option<String>,
    #[allow(dead_code)]
    client: Client,
}

impl User {
    pub(crate) fn new(jid: Jid, client: Client) -> Self {
        Self {
            jid,
            name: None,
            push_name: None,
            phone_number: None,
            client,
        }
    }

    /// The JID of this user.
    pub fn id(&self) -> &Jid {
        &self.jid
    }

    /// The business name or saved name for this user, if known.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// The push name (display name) the user set for themselves, if known.
    pub fn push_name(&self) -> Option<&str> {
        self.push_name.as_deref()
    }

    /// The phone number of this user, if known.
    pub fn phone_number(&self) -> Option<&str> {
        self.phone_number.as_deref()
    }

    /// Set the push name.
    #[allow(dead_code)]
    pub(crate) fn set_push_name(&mut self, name: String) {
        self.push_name = Some(name);
    }
}
