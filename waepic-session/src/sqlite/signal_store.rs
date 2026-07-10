use async_trait::async_trait;
use bytes::Bytes;
use rusqlite::params;
use wacore::store::{
    error::{Result as StoreResult, StoreError},
    traits::SignalStore,
};

use super::{SqliteSession, rusqlite_err};

#[async_trait]
impl SignalStore for SqliteSession {
    async fn put_identity(&self, address: &str, key: [u8; 32]) -> StoreResult<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        conn.execute(
            "INSERT OR REPLACE INTO identities (address, key) VALUES (?1, ?2)",
            params![address, key.as_slice()],
        )
        .map_err(rusqlite_err)?;

        Ok(())
    }

    async fn load_identity(&self, address: &str) -> StoreResult<Option<[u8; 32]>> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare("SELECT key FROM identities WHERE address = ?1")
            .map_err(rusqlite_err)?;

        let mut rows = stmt
            .query_map(params![address], |row| {
                let key: Vec<u8> = row.get(0)?;
                Ok(key)
            })
            .map_err(rusqlite_err)?;
        match rows.next() {
            Some(row) => {
                let key = row.map_err(rusqlite_err)?;
                let arr: [u8; 32] = key
                    .try_into()
                    .map_err(|_| StoreError::Validation("identity key must be 32 bytes".into()))?;
                Ok(Some(arr))
            }
            None => Ok(None),
        }
    }

    async fn delete_identity(&self, address: &str) -> StoreResult<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        conn.execute(
            "DELETE FROM identities WHERE address = ?1",
            params![address],
        )
        .map_err(rusqlite_err)?;

        Ok(())
    }

    async fn get_session(&self, address: &str) -> StoreResult<Option<Bytes>> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare("SELECT data FROM signal_sessions WHERE address = ?1")
            .map_err(rusqlite_err)?;

        let mut rows = stmt
            .query_map(params![address], |row| {
                let data: Vec<u8> = row.get(0)?;
                Ok(data)
            })
            .map_err(rusqlite_err)?;
        match rows.next() {
            Some(row) => {
                let data = row.map_err(rusqlite_err)?;
                Ok(Some(Bytes::from(data)))
            }
            None => Ok(None),
        }
    }

    async fn put_session(&self, address: &str, session: &[u8]) -> StoreResult<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        conn.execute(
            "INSERT OR REPLACE INTO signal_sessions (address, data) VALUES (?1, ?2)",
            params![address, session],
        )
        .map_err(rusqlite_err)?;

        Ok(())
    }

    async fn delete_session(&self, address: &str) -> StoreResult<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        conn.execute(
            "DELETE FROM signal_sessions WHERE address = ?1",
            params![address],
        )
        .map_err(rusqlite_err)?;

        Ok(())
    }

    async fn store_prekey(&self, id: u32, record: &[u8], uploaded: bool) -> StoreResult<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        conn.execute(
            "INSERT OR REPLACE INTO prekeys (id, record, uploaded) VALUES (?1, ?2, ?3)",
            params![id, record, i32::from(uploaded)],
        )
        .map_err(rusqlite_err)?;

        Ok(())
    }

    async fn load_prekey(&self, id: u32) -> StoreResult<Option<Bytes>> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare("SELECT record FROM prekeys WHERE id = ?1")
            .map_err(rusqlite_err)?;

        let mut rows = stmt
            .query_map(params![id], |row| {
                let data: Vec<u8> = row.get(0)?;
                Ok(data)
            })
            .map_err(rusqlite_err)?;
        match rows.next() {
            Some(row) => {
                let data = row.map_err(rusqlite_err)?;
                Ok(Some(Bytes::from(data)))
            }
            None => Ok(None),
        }
    }

    async fn remove_prekey(&self, id: u32) -> StoreResult<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        conn.execute("DELETE FROM prekeys WHERE id = ?1", params![id])
            .map_err(rusqlite_err)?;

        Ok(())
    }

    async fn get_max_prekey_id(&self) -> StoreResult<u32> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let max_id: Option<u32> = conn
            .query_row("SELECT MAX(id) FROM prekeys", [], |row| row.get(0))
            .map_err(rusqlite_err)?;

        Ok(max_id.unwrap_or(0))
    }

    async fn store_signed_prekey(&self, id: u32, record: &[u8]) -> StoreResult<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        conn.execute(
            "INSERT OR REPLACE INTO signed_prekeys (id, record) VALUES (?1, ?2)",
            params![id, record],
        )
        .map_err(rusqlite_err)?;

        Ok(())
    }

    async fn load_signed_prekey(&self, id: u32) -> StoreResult<Option<Vec<u8>>> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare("SELECT record FROM signed_prekeys WHERE id = ?1")
            .map_err(rusqlite_err)?;

        let mut rows = stmt
            .query_map(params![id], |row| row.get::<_, Vec<u8>>(0))
            .map_err(rusqlite_err)?;
        match rows.next() {
            Some(row) => Ok(Some(row.map_err(rusqlite_err)?)),
            None => Ok(None),
        }
    }

    async fn load_all_signed_prekeys(&self) -> StoreResult<Vec<(u32, Vec<u8>)>> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare("SELECT id, record FROM signed_prekeys")
            .map_err(rusqlite_err)?;

        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, u32>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .map_err(rusqlite_err)?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row.map_err(rusqlite_err)?);
        }

        Ok(result)
    }

    async fn remove_signed_prekey(&self, id: u32) -> StoreResult<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        conn.execute("DELETE FROM signed_prekeys WHERE id = ?1", params![id])
            .map_err(rusqlite_err)?;

        Ok(())
    }

    async fn put_sender_key(&self, address: &str, record: &[u8]) -> StoreResult<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        conn.execute(
            "INSERT OR REPLACE INTO sender_keys (address, record) VALUES (?1, ?2)",
            params![address, record],
        )
        .map_err(rusqlite_err)?;

        Ok(())
    }

    async fn get_sender_key(&self, address: &str) -> StoreResult<Option<Vec<u8>>> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare("SELECT record FROM sender_keys WHERE address = ?1")
            .map_err(rusqlite_err)?;

        let mut rows = stmt
            .query_map(params![address], |row| row.get::<_, Vec<u8>>(0))
            .map_err(rusqlite_err)?;
        match rows.next() {
            Some(row) => Ok(Some(row.map_err(rusqlite_err)?)),
            None => Ok(None),
        }
    }

    async fn delete_sender_key(&self, address: &str) -> StoreResult<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        conn.execute(
            "DELETE FROM sender_keys WHERE address = ?1",
            params![address],
        )
        .map_err(rusqlite_err)?;

        Ok(())
    }

    async fn mark_prekeys_uploaded(&self, ids: &[u32]) -> StoreResult<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare("UPDATE prekeys SET uploaded = 1 WHERE id = ?1")
            .map_err(rusqlite_err)?;

        for &id in ids {
            stmt.execute(params![id]).map_err(rusqlite_err)?;
        }

        Ok(())
    }
}
