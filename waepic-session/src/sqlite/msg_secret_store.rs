use async_trait::async_trait;
use rusqlite::params;
use wacore::store::{
    error::Result as StoreResult,
    traits::{MsgSecretEntry, MsgSecretStore},
};

use super::{SqliteSession, rusqlite_err};

#[async_trait]
impl MsgSecretStore for SqliteSession {
    async fn put_msg_secrets(&self, entries: Vec<MsgSecretEntry>) -> StoreResult<usize> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare(
                "INSERT OR REPLACE INTO msg_secrets (chat, sender, msg_id, secret, expires_at, message_ts) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )
            .map_err(rusqlite_err)?;

        let mut count = 0usize;
        for entry in &entries {
            stmt.execute(params![
                entry.chat,
                entry.sender,
                entry.msg_id,
                entry.secret.as_slice(),
                entry.expires_at,
                entry.message_ts,
            ])
            .map_err(rusqlite_err)?;
            count += 1;
        }

        Ok(count)
    }

    async fn get_msg_secret(
        &self,
        chat: &str,
        sender: &str,
        msg_id: &str,
    ) -> StoreResult<Option<Vec<u8>>> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT secret FROM msg_secrets WHERE chat = ?1 AND sender = ?2 AND msg_id = ?3",
            )
            .map_err(rusqlite_err)?;
        let mut rows = stmt
            .query_map(params![chat, sender, msg_id], |row| {
                row.get::<_, Vec<u8>>(0)
            })
            .map_err(rusqlite_err)?;

        match rows.next() {
            Some(row) => Ok(Some(row.map_err(rusqlite_err)?)),
            None => Ok(None),
        }
    }

    async fn get_msg_secret_with_ts(
        &self,
        chat: &str,
        sender: &str,
        msg_id: &str,
    ) -> StoreResult<Option<(Vec<u8>, i64)>> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT secret, message_ts FROM msg_secrets WHERE chat = ?1 AND sender = ?2 AND msg_id = ?3",
            )
            .map_err(rusqlite_err)?;
        let mut rows = stmt
            .query_map(params![chat, sender, msg_id], |row| {
                Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(rusqlite_err)?;

        match rows.next() {
            Some(row) => Ok(Some(row.map_err(rusqlite_err)?)),
            None => Ok(None),
        }
    }

    async fn delete_expired_msg_secrets(&self, cutoff_timestamp: i64) -> StoreResult<u32> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let rows = conn
            .execute(
                "DELETE FROM msg_secrets WHERE expires_at < ?1",
                params![cutoff_timestamp],
            )
            .map_err(rusqlite_err)?;

        Ok(u32::try_from(rows).unwrap_or(u32::MAX))
    }
}
