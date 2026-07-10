mod app_sync_store;
mod device_store;
mod msg_secret_store;
mod protocol_store;
mod session;
mod signal_store;

use std::{path::Path, sync::Mutex};

use rusqlite::Connection;
use wacore::store::error::StoreError;

use crate::Result;

pub(crate) fn rusqlite_err(err: rusqlite::Error) -> StoreError {
    StoreError::Database(Box::new(err))
}

const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS identities (
        address TEXT PRIMARY KEY,
        key     BLOB NOT NULL
    );

    CREATE TABLE IF NOT EXISTS signal_sessions (
        address TEXT PRIMARY KEY,
        data    BLOB NOT NULL
    );

    CREATE TABLE IF NOT EXISTS prekeys (
        id       INTEGER PRIMARY KEY,
        record   BLOB    NOT NULL,
        uploaded INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS signed_prekeys (
        id     INTEGER PRIMARY KEY,
        record BLOB    NOT NULL
    );

    CREATE TABLE IF NOT EXISTS sender_keys (
        address TEXT PRIMARY KEY,
        record  BLOB NOT NULL
    );

    CREATE TABLE IF NOT EXISTS sync_keys (
        key_id     BLOB PRIMARY KEY,
        key_data   BLOB    NOT NULL,
        fingerprint BLOB   NOT NULL,
        timestamp  INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS app_versions (
        name             TEXT PRIMARY KEY,
        version          INTEGER NOT NULL,
        hash             BLOB,
        index_value_map  TEXT
    );

    CREATE TABLE IF NOT EXISTS mutation_macs (
        name      TEXT    NOT NULL,
        index_mac BLOB    NOT NULL,
        value_mac BLOB    NOT NULL,
        PRIMARY KEY (name, index_mac)
    );

    CREATE TABLE IF NOT EXISTS sender_key_devices (
        group_jid  TEXT    NOT NULL,
        device_jid TEXT    NOT NULL,
        has_key    INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (group_jid, device_jid)
    );

    CREATE TABLE IF NOT EXISTS lid_mappings (
        lid             TEXT PRIMARY KEY,
        phone_number    TEXT    NOT NULL,
        created_at      INTEGER NOT NULL,
        updated_at      INTEGER NOT NULL,
        learning_source TEXT    NOT NULL
    );

    CREATE TABLE IF NOT EXISTS base_keys (
        address   TEXT    NOT NULL,
        message_id TEXT   NOT NULL,
        base_key  BLOB    NOT NULL,
        PRIMARY KEY (address, message_id)
    );

    CREATE TABLE IF NOT EXISTS device_lists (
        user   TEXT PRIMARY KEY,
        record TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS group_metadata (
        group_jid TEXT PRIMARY KEY,
        data      BLOB NOT NULL
    );

    CREATE TABLE IF NOT EXISTS tc_tokens (
        jid               TEXT PRIMARY KEY,
        token             BLOB    NOT NULL,
        token_timestamp   INTEGER NOT NULL,
        sender_timestamp  INTEGER
    );

    CREATE TABLE IF NOT EXISTS sent_messages (
        chat_jid    TEXT    NOT NULL,
        message_id  TEXT    NOT NULL,
        payload     BLOB    NOT NULL,
        created_at  INTEGER NOT NULL,
        PRIMARY KEY (chat_jid, message_id)
    );

    CREATE TABLE IF NOT EXISTS msg_secrets (
        chat       TEXT    NOT NULL,
        sender     TEXT    NOT NULL,
        msg_id     TEXT    NOT NULL,
        secret     BLOB    NOT NULL,
        expires_at INTEGER NOT NULL,
        message_ts INTEGER NOT NULL,
        PRIMARY KEY (chat, sender, msg_id)
    );

    CREATE TABLE IF NOT EXISTS device (
        id   INTEGER PRIMARY KEY CHECK(id = 0),
        data TEXT    NOT NULL
    );

    CREATE TABLE IF NOT EXISTS pending_inbound (
        chat    TEXT NOT NULL,
        sender  TEXT NOT NULL,
        id      TEXT NOT NULL,
        message BLOB NOT NULL,
        PRIMARY KEY (chat, sender, id)
    );

    CREATE TABLE IF NOT EXISTS chats (
        jid  TEXT PRIMARY KEY,
        name TEXT,
        kind TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS contacts (
        jid TEXT PRIMARY KEY
    );
";

/// SQLite-backed session storage.
///
/// Wraps a [`rusqlite::Connection`] behind a [`Mutex`] for thread-safe access.
/// Implements [`Backend`](crate::Backend) for protocol-level persistence and
/// [`Session`](crate::Session) for chat/contact caching.
pub struct SqliteSession {
    conn: Mutex<Connection>,
}

impl SqliteSession {
    /// Open or create a SQLite database at the given path.
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        conn.execute_batch(SCHEMA)?;

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Wrap an existing connection (e.g. one with a custom PRAGMA like a
    /// password-protected database).
    pub fn from_conn(conn: Connection) -> Result<Self> {
        conn.execute_batch(SCHEMA)?;

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Execute a closure with the raw connection held under the lock.
    pub fn with_conn<R>(&self, f: impl FnOnce(&Connection) -> R) -> R {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        f(&conn)
    }
}
