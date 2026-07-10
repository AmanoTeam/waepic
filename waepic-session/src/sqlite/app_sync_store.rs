use async_trait::async_trait;
use rusqlite::params;
use wacore::{
    appstate::{hash::HashState, processor::AppStateMutationMAC},
    store::{
        error::{Result as StoreResult, StoreError},
        traits::{AppStateSyncKey, AppSyncStore},
    },
};

use super::{SqliteSession, rusqlite_err};

#[async_trait]
impl AppSyncStore for SqliteSession {
    async fn get_sync_key(&self, key_id: &[u8]) -> StoreResult<Option<AppStateSyncKey>> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare("SELECT key_data, fingerprint, timestamp FROM sync_keys WHERE key_id = ?1")
            .map_err(rusqlite_err)?;
        let mut rows = stmt
            .query_map(params![key_id], |row| {
                Ok(AppStateSyncKey {
                    key_data: row.get(0)?,
                    fingerprint: row.get(1)?,
                    timestamp: row.get(2)?,
                })
            })
            .map_err(rusqlite_err)?;

        match rows.next() {
            Some(row) => Ok(Some(row.map_err(rusqlite_err)?)),
            None => Ok(None),
        }
    }

    async fn set_sync_key(&self, key_id: &[u8], key: AppStateSyncKey) -> StoreResult<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        conn.execute(
            "INSERT OR REPLACE INTO sync_keys (key_id, key_data, fingerprint, timestamp) \
             VALUES (?1, ?2, ?3, ?4)",
            params![key_id, key.key_data, key.fingerprint, key.timestamp],
        )
        .map_err(rusqlite_err)?;

        Ok(())
    }

    async fn get_version(&self, name: &str) -> StoreResult<HashState> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare("SELECT version, hash, index_value_map FROM app_versions WHERE name = ?1")
            .map_err(rusqlite_err)?;
        let mut rows = stmt
            .query_map(params![name], |row| {
                let version: u64 = row.get(0)?;
                let hash: Option<Vec<u8>> = row.get(1)?;
                let ivm: Option<String> = row.get(2)?;
                Ok((version, hash, ivm))
            })
            .map_err(rusqlite_err)?;

        match rows.next() {
            Some(row) => {
                let (version, hash, ivm) = row.map_err(rusqlite_err)?;
                let hash_arr: [u8; 128] =
                    hash.and_then(|h| h.try_into().ok()).unwrap_or([0u8; 128]);
                let index_value_map = ivm
                    .and_then(|s| serde_json::from_str(&s).ok())
                    .unwrap_or_default();

                Ok(HashState {
                    version,
                    hash: hash_arr,
                    index_value_map,
                })
            }
            None => Ok(HashState::default()),
        }
    }

    async fn set_version(&self, name: &str, state: HashState) -> StoreResult<()> {
        let ivm = serde_json::to_string(&state.index_value_map)
            .map_err(|e| StoreError::Serialization(e.to_string().into()))?;
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        conn.execute(
            "INSERT OR REPLACE INTO app_versions (name, version, hash, index_value_map) \
             VALUES (?1, ?2, ?3, ?4)",
            params![name, state.version, state.hash.as_slice(), ivm],
        )
        .map_err(rusqlite_err)?;

        Ok(())
    }

    async fn put_mutation_macs(
        &self,
        name: &str,
        _version: u64,
        mutations: &[AppStateMutationMAC],
    ) -> StoreResult<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare(
                "INSERT OR REPLACE INTO mutation_macs (name, index_mac, value_mac) VALUES (?1, ?2, ?3)",
            )
            .map_err(rusqlite_err)?;
        for m in mutations {
            stmt.execute(params![
                name,
                m.index_mac.as_slice(),
                m.value_mac.as_slice()
            ])
            .map_err(rusqlite_err)?;
        }

        Ok(())
    }

    async fn get_mutation_mac(&self, name: &str, index_mac: &[u8]) -> StoreResult<Option<Vec<u8>>> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare("SELECT value_mac FROM mutation_macs WHERE name = ?1 AND index_mac = ?2")
            .map_err(rusqlite_err)?;
        let mut rows = stmt
            .query_map(params![name, index_mac], |row| row.get::<_, Vec<u8>>(0))
            .map_err(rusqlite_err)?;

        match rows.next() {
            Some(row) => Ok(Some(row.map_err(rusqlite_err)?)),
            None => Ok(None),
        }
    }

    async fn delete_mutation_macs(&self, name: &str, index_macs: &[Vec<u8>]) -> StoreResult<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare("DELETE FROM mutation_macs WHERE name = ?1 AND index_mac = ?2")
            .map_err(rusqlite_err)?;

        for mac in index_macs {
            stmt.execute(params![name, mac.as_slice()])
                .map_err(rusqlite_err)?;
        }

        Ok(())
    }

    async fn clear_mutation_macs(&self, name: &str) -> StoreResult<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        conn.execute("DELETE FROM mutation_macs WHERE name = ?1", params![name])
            .map_err(rusqlite_err)?;

        Ok(())
    }

    async fn get_latest_sync_key_id(&self) -> StoreResult<Option<Vec<u8>>> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare("SELECT key_id FROM sync_keys ORDER BY rowid DESC LIMIT 1")
            .map_err(rusqlite_err)?;
        let mut rows = stmt
            .query_map([], |row| row.get::<_, Vec<u8>>(0))
            .map_err(rusqlite_err)?;

        match rows.next() {
            Some(row) => Ok(Some(row.map_err(rusqlite_err)?)),
            None => Ok(None),
        }
    }
}
