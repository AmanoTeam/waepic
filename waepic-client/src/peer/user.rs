use wacore_binary::Jid;

use crate::{Client, ClientError, InputMessage, Message, Result};

/// A private 1:1 conversation with a WhatsApp user.
#[derive(Clone, Debug)]
pub struct User {
    jid: Jid,
    name: Option<String>,
    push_name: Option<String>,
    phone_number: Option<String>,
    pub(crate) client: Client,
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
    pub fn jid(&self) -> &Jid {
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

    /// Send a message to this user.
    pub async fn send_message<M: Into<InputMessage>>(&self, msg: M) -> Result<Message> {
        self.client.send_message(self.clone(), msg.into()).await
    }

    /// Mark messages as read in this chat.
    pub async fn mark_as_read(&self, message_ids: &[&str]) -> Result<()> {
        self.client.mark_as_read(self.clone(), message_ids).await
    }

    /// Fetch the profile picture URL for this user.
    ///
    /// Returns `Ok(Some(url))` if a picture exists, `Ok(None)` if no picture
    /// is set or the request indicates no picture is available.
    pub async fn profile_picture_url(&self) -> Result<Option<String>> {
        let spec = wacore::iq::contacts::ProfilePictureSpec::full(&self.jid);
        let response = self
            .client
            .inner
            .handle
            .send_iq(spec)
            .await
            .map_err(|e| {
                ClientError::Internal(format!("profile picture request failed: {e}"))
            })?;
        Ok(response.map(|p| p.url))
    }
}
