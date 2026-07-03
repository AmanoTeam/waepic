//! Authentication status and login-check helpers.

use wacore_binary::Jid;

use crate::{Client, Result, error::ClientError, peer::Chat};

/// Whether the client has an authenticated session.
#[derive(Clone, Debug)]
pub enum LoginStatus {
    /// The client is authorized with a known JID and push name.
    Authorized {
        /// The user's JID.
        jid: Jid,
        /// The user's push name as stored on the device.
        push_name: String,
    },
    /// The client is not paired / not logged in.
    NotAuthorized,
}

impl Client {
    /// Check whether the backend has a stored device with a phone number,
    /// which indicates a completed pairing flow.
    #[tracing::instrument(skip(self))]
    pub async fn is_authorized(&self) -> Result<bool> {
        let Some(backend) = self.inner.backend.as_ref() else {
            return Ok(false);
        };

        let device = backend
            .load()
            .await
            .map_err(|e| ClientError::Internal(format!("failed to load device: {e}")))?;

        match device {
            Some(d) => {
                let authorized = d.pn.is_some();
                tracing::trace!(authorized, "checked");

                Ok(authorized)
            }
            None => {
                tracing::trace!("no device stored");
                Ok(false)
            }
        }
    }

    /// Return the current [`LoginStatus`] by inspecting the stored device.
    #[tracing::instrument(skip(self))]
    pub async fn check_login_status(&self) -> Result<LoginStatus> {
        let backend = self
            .inner
            .backend
            .as_ref()
            .ok_or_else(|| ClientError::Internal("no backend configured".into()))?;

        let device = backend
            .load()
            .await
            .map_err(|e| ClientError::Internal(format!("failed to load device: {e}")))?;

        match device {
            Some(d) if d.pn.is_some() => {
                let jid = d.pn.clone().expect("pn is Some");
                tracing::trace!(%jid, push_name = %d.push_name, "authorized");

                Ok(LoginStatus::Authorized {
                    jid,
                    push_name: d.push_name,
                })
            }
            _ => {
                tracing::trace!("not authorized");
                Ok(LoginStatus::NotAuthorized)
            }
        }
    }

    /// Return the current user as a [`Chat::User`] variant.
    ///
    /// Requires an authorized session; returns [`ClientError::NotLoggedIn`]
    /// when the client is not paired.
    #[tracing::instrument(skip(self))]
    pub async fn get_me(&self) -> Result<Chat> {
        let backend = self
            .inner
            .backend
            .as_ref()
            .ok_or_else(|| ClientError::Internal("no backend configured".into()))?;

        let device = backend
            .load()
            .await
            .map_err(|e| ClientError::Internal(format!("failed to load device: {e}")))?;

        match device {
            Some(d) if d.pn.is_some() => {
                let jid = d.pn.clone().expect("pn is Some");
                let push_name = d.push_name;

                let mut chat = self.chat(jid);
                if let Chat::User(ref mut user) = chat {
                    user.set_push_name(push_name);
                }

                tracing::trace!("returning user chat");
                Ok(chat)
            }
            _ => {
                tracing::trace!("not authorized");
                Err(ClientError::NotLoggedIn)
            }
        }
    }
}
