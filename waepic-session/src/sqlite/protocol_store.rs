use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use rusqlite::params;
use wacore::store::{
    error::{Result as StoreResult, StoreError},
    traits::{DeviceListRecord, LidPnMappingEntry, ProtocolStore, TcTokenEntry},
};

use super::{SqliteSession, rusqlite_err};

#[async_trait]
impl ProtocolStore for SqliteSession {
    async fn get_sender_key_devices(&self, group_jid: &str) -> StoreResult<Vec<(String, bool)>> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare("SELECT device_jid, has_key FROM sender_key_devices WHERE group_jid = ?1")
            .map_err(rusqlite_err)?;

        let rows = stmt
            .query_map(params![group_jid], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i32>(1)? != 0))
            })
            .map_err(rusqlite_err)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(rusqlite_err)
    }

    async fn set_sender_key_status(
        &self,
        group_jid: &str,
        entries: &[(&str, bool)],
    ) -> StoreResult<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare(
                "INSERT OR REPLACE INTO sender_key_devices (group_jid, device_jid, has_key) \
                 VALUES (?1, ?2, ?3)",
            )
            .map_err(rusqlite_err)?;

        for (jid, has_key) in entries {
            stmt.execute(params![group_jid, jid, i32::from(*has_key)])
                .map_err(rusqlite_err)?;
        }

        Ok(())
    }

    async fn clear_sender_key_devices(&self, group_jid: &str) -> StoreResult<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        conn.execute(
            "DELETE FROM sender_key_devices WHERE group_jid = ?1",
            params![group_jid],
        )
        .map_err(rusqlite_err)?;

        Ok(())
    }

    async fn clear_all_sender_key_devices(&self) -> StoreResult<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        conn.execute("DELETE FROM sender_key_devices", [])
            .map_err(rusqlite_err)?;

        Ok(())
    }

    async fn delete_sender_key_device_rows(&self, device_jids: &[&str]) -> StoreResult<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare("DELETE FROM sender_key_devices WHERE device_jid = ?1")
            .map_err(rusqlite_err)?;

        for jid in device_jids {
            stmt.execute(params![jid]).map_err(rusqlite_err)?;
        }

        Ok(())
    }

    async fn get_lid_mapping(&self, lid: &str) -> StoreResult<Option<LidPnMappingEntry>> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT lid, phone_number, created_at, updated_at, learning_source \
                 FROM lid_mappings WHERE lid = ?1",
            )
            .map_err(rusqlite_err)?;
        let mut rows = stmt
            .query_map(params![lid], |row| {
                Ok(LidPnMappingEntry {
                    lid: row.get(0)?,
                    phone_number: row.get(1)?,
                    created_at: row.get(2)?,
                    updated_at: row.get(3)?,
                    learning_source: row.get(4)?,
                })
            })
            .map_err(rusqlite_err)?;

        match rows.next() {
            Some(row) => Ok(Some(row.map_err(rusqlite_err)?)),
            None => Ok(None),
        }
    }

    async fn get_pn_mapping(&self, phone: &str) -> StoreResult<Option<LidPnMappingEntry>> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT lid, phone_number, created_at, updated_at, learning_source \
                 FROM lid_mappings WHERE phone_number = ?1",
            )
            .map_err(rusqlite_err)?;
        let mut rows = stmt
            .query_map(params![phone], |row| {
                Ok(LidPnMappingEntry {
                    lid: row.get(0)?,
                    phone_number: row.get(1)?,
                    created_at: row.get(2)?,
                    updated_at: row.get(3)?,
                    learning_source: row.get(4)?,
                })
            })
            .map_err(rusqlite_err)?;

        match rows.next() {
            Some(row) => Ok(Some(row.map_err(rusqlite_err)?)),
            None => Ok(None),
        }
    }

    async fn put_lid_mapping(&self, entry: &LidPnMappingEntry) -> StoreResult<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        conn.execute(
            "INSERT OR REPLACE INTO lid_mappings \
             (lid, phone_number, created_at, updated_at, learning_source) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                entry.lid,
                entry.phone_number,
                entry.created_at,
                entry.updated_at,
                entry.learning_source,
            ],
        )
        .map_err(rusqlite_err)?;

        Ok(())
    }

    async fn get_all_lid_mappings(&self) -> StoreResult<Vec<LidPnMappingEntry>> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT lid, phone_number, created_at, updated_at, learning_source FROM lid_mappings",
            )
            .map_err(rusqlite_err)?;

        let rows = stmt
            .query_map([], |row| {
                Ok(LidPnMappingEntry {
                    lid: row.get(0)?,
                    phone_number: row.get(1)?,
                    created_at: row.get(2)?,
                    updated_at: row.get(3)?,
                    learning_source: row.get(4)?,
                })
            })
            .map_err(rusqlite_err)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(rusqlite_err)
    }

    async fn save_base_key(
        &self,
        address: &str,
        message_id: &str,
        base_key: &[u8],
    ) -> StoreResult<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        conn.execute(
            "INSERT OR REPLACE INTO base_keys (address, message_id, base_key) \
             VALUES (?1, ?2, ?3)",
            params![address, message_id, base_key],
        )
        .map_err(rusqlite_err)?;

        Ok(())
    }

    async fn has_same_base_key(
        &self,
        address: &str,
        message_id: &str,
        current_base_key: &[u8],
    ) -> StoreResult<bool> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare("SELECT base_key FROM base_keys WHERE address = ?1 AND message_id = ?2")
            .map_err(rusqlite_err)?;
        let mut rows = stmt
            .query_map(params![address, message_id], |row| row.get::<_, Vec<u8>>(0))
            .map_err(rusqlite_err)?;

        match rows.next() {
            Some(row) => Ok(row.map_err(rusqlite_err)? == current_base_key),
            None => Ok(false),
        }
    }

    async fn delete_base_key(&self, address: &str, message_id: &str) -> StoreResult<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        conn.execute(
            "DELETE FROM base_keys WHERE address = ?1 AND message_id = ?2",
            params![address, message_id],
        )
        .map_err(rusqlite_err)?;

        Ok(())
    }

    async fn update_device_list(&self, record: DeviceListRecord) -> StoreResult<()> {
        let json = serde_json::to_string(&record)
            .map_err(|e| StoreError::Serialization(e.to_string().into()))?;
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        conn.execute(
            "INSERT OR REPLACE INTO device_lists (user, record) VALUES (?1, ?2)",
            params![record.user, json],
        )
        .map_err(rusqlite_err)?;

        Ok(())
    }

    async fn get_devices(&self, user: &str) -> StoreResult<Option<DeviceListRecord>> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare("SELECT record FROM device_lists WHERE user = ?1")
            .map_err(rusqlite_err)?;

        let mut rows = stmt
            .query_map(params![user], |row| {
                let json: String = row.get(0)?;
                Ok(json)
            })
            .map_err(rusqlite_err)?;
        match rows.next() {
            Some(row) => {
                let json = row.map_err(rusqlite_err)?;
                let record: DeviceListRecord = serde_json::from_str(&json)
                    .map_err(|e| StoreError::Serialization(e.to_string().into()))?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    async fn delete_devices(&self, user: &str) -> StoreResult<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        conn.execute("DELETE FROM device_lists WHERE user = ?1", params![user])
            .map_err(rusqlite_err)?;

        Ok(())
    }

    async fn get_tc_token(&self, jid: &str) -> StoreResult<Option<TcTokenEntry>> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT token, token_timestamp, sender_timestamp FROM tc_tokens WHERE jid = ?1",
            )
            .map_err(rusqlite_err)?;

        let mut rows = stmt
            .query_map(params![jid], |row| {
                Ok(TcTokenEntry {
                    token: row.get(0)?,
                    token_timestamp: row.get(1)?,
                    sender_timestamp: row.get(2)?,
                })
            })
            .map_err(rusqlite_err)?;
        match rows.next() {
            Some(row) => Ok(Some(row.map_err(rusqlite_err)?)),
            None => Ok(None),
        }
    }

    async fn put_tc_token(&self, jid: &str, entry: &TcTokenEntry) -> StoreResult<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        conn.execute(
            "INSERT OR REPLACE INTO tc_tokens (jid, token, token_timestamp, sender_timestamp) \
             VALUES (?1, ?2, ?3, ?4)",
            params![
                jid,
                entry.token.as_slice(),
                entry.token_timestamp,
                entry.sender_timestamp
            ],
        )
        .map_err(rusqlite_err)?;

        Ok(())
    }

    async fn delete_tc_token(&self, jid: &str) -> StoreResult<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        conn.execute("DELETE FROM tc_tokens WHERE jid = ?1", params![jid])
            .map_err(rusqlite_err)?;

        Ok(())
    }

    async fn get_all_tc_token_jids(&self) -> StoreResult<Vec<String>> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare("SELECT jid FROM tc_tokens")
            .map_err(rusqlite_err)?;

        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(rusqlite_err)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(rusqlite_err)
    }

    async fn delete_expired_tc_tokens(
        &self,
        token_cutoff: i64,
        sender_cutoff: i64,
    ) -> StoreResult<u32> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let rows = conn
            .execute(
                "DELETE FROM tc_tokens WHERE token_timestamp < ?1 OR sender_timestamp < ?2",
                params![token_cutoff, sender_cutoff],
            )
            .map_err(rusqlite_err)?;

        Ok(u32::try_from(rows).unwrap_or(u32::MAX))
    }

    async fn store_sent_message(
        &self,
        chat_jid: &str,
        message_id: &str,
        payload: &[u8],
    ) -> StoreResult<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0i64, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX));
        conn.execute(
            "INSERT OR REPLACE INTO sent_messages (chat_jid, message_id, payload, created_at) \
             VALUES (?1, ?2, ?3, ?4)",
            params![chat_jid, message_id, payload, now],
        )
        .map_err(rusqlite_err)?;

        Ok(())
    }

    async fn take_sent_message(
        &self,
        chat_jid: &str,
        message_id: &str,
    ) -> StoreResult<Option<Vec<u8>>> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare(
                "DELETE FROM sent_messages WHERE chat_jid = ?1 AND message_id = ?2 RETURNING payload",
            )
            .map_err(rusqlite_err)?;

        let mut rows = stmt
            .query_map(params![chat_jid, message_id], |row| {
                row.get::<_, Vec<u8>>(0)
            })
            .map_err(rusqlite_err)?;
        match rows.next() {
            Some(row) => Ok(Some(row.map_err(rusqlite_err)?)),
            None => Ok(None),
        }
    }

    async fn delete_expired_sent_messages(&self, cutoff_timestamp: i64) -> StoreResult<u32> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let rows = conn
            .execute(
                "DELETE FROM sent_messages WHERE created_at < ?1",
                params![cutoff_timestamp],
            )
            .map_err(rusqlite_err)?;

        Ok(u32::try_from(rows).unwrap_or(u32::MAX))
    }
}
