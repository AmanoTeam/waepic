pub mod auth;

use std::{fmt, sync::Arc};

use wacore::store::traits::Backend;
use wacore_binary::{Jid, JidExt, Server};
use waepic_connection::{Connection, ConnectionHandle, ConnectionRunner, RawEvent};
use waepic_session::Session;

use crate::{
    config::ClientConfiguration,
    peer::{Chat, Group, Newsletter, OtherChat, User},
};

/// The main WhatsApp client handle.
#[derive(Clone)]
pub struct Client {
    #[allow(dead_code)]
    pub(crate) inner: Arc<ClientInner>,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client").finish_non_exhaustive()
    }
}

#[allow(dead_code)]
pub(crate) struct ClientInner {
    pub(crate) handle: ConnectionHandle,
    pub(crate) session: Arc<dyn Session>,
    pub(crate) config: ClientConfiguration,
    pub(crate) backend: Option<Arc<dyn Backend>>,
    pub(crate) raw_rx: Option<async_broadcast::Receiver<RawEvent>>,
}

impl Client {
    /// Create a new `Client` with an existing connection handle.
    pub fn new(
        handle: ConnectionHandle,
        session: Arc<dyn Session>,
        config: ClientConfiguration,
    ) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                handle,
                session,
                config,
                backend: None,
                raw_rx: None,
            }),
        }
    }

    /// Create a new `Client` by establishing a connection.
    #[tracing::instrument(skip(backend, session))]
    pub fn connect(
        backend: Arc<dyn Backend>,
        session: Arc<dyn Session>,
        config: ClientConfiguration,
    ) -> (Self, async_broadcast::Receiver<RawEvent>, ConnectionRunner) {
        let (runner, raw_rx, handle) =
            Connection::new(Arc::clone(&backend), config.connection.clone());

        let client = Self {
            inner: Arc::new(ClientInner {
                handle,
                session,
                config,
                backend: Some(backend),
                raw_rx: Some(raw_rx.clone()),
            }),
        };

        (client, raw_rx, runner)
    }

    /// Map a JID to the appropriate [`Chat`] variant based on its server type.
    #[tracing::instrument(skip(self))]
    pub fn chat(&self, jid: Jid) -> Chat {
        match jid.server() {
            Server::Pn | Server::Lid => Chat::User(User::new(jid, self.clone())),
            Server::Group => Chat::Group(Group::new(jid, self.clone())),
            Server::Newsletter => Chat::Newsletter(Newsletter::new(jid, self.clone())),
            _ => Chat::Other(OtherChat::new(jid, self.clone())),
        }
    }
}
