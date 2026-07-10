use async_trait::async_trait;
use rusqlite::params;
use wacore::store::{
    Device,
    error::{Result as StoreResult, StoreError},
    traits::DeviceStore,
};

use super::{SqliteSession, rusqlite_err};

#[async_trait]
impl DeviceStore for SqliteSession {
    async fn save(&self, device: &Device) -> StoreResult<()> {
        let json = serde_json::to_string(device)
            .map_err(|e| StoreError::Serialization(e.to_string().into()))?;
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        conn.execute(
            "INSERT OR REPLACE INTO device (id, data) VALUES (0, ?1)",
            params![json],
        )
        .map_err(rusqlite_err)?;

        Ok(())
    }

    async fn load(&self) -> StoreResult<Option<Device>> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let mut stmt = conn
            .prepare("SELECT data FROM device WHERE id = 0")
            .map_err(rusqlite_err)?;
        let mut rows = stmt
            .query_map([], |row| {
                let data: String = row.get(0)?;
                Ok(data)
            })
            .map_err(rusqlite_err)?;

        match rows.next() {
            Some(row) => {
                let json = row.map_err(rusqlite_err)?;
                let device: Device = serde_json::from_str(&json)
                    .map_err(|e| StoreError::Serialization(e.to_string().into()))?;
                Ok(Some(device))
            }
            None => Ok(None),
        }
    }

    async fn exists(&self) -> StoreResult<bool> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let count: i32 = conn
            .query_row("SELECT COUNT(*) FROM device WHERE id = 0", [], |row| {
                row.get(0)
            })
            .map_err(rusqlite_err)?;

        Ok(count > 0)
    }

    async fn create(&self) -> StoreResult<i32> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let exists: bool = conn
            .query_row("SELECT COUNT(*) FROM device WHERE id = 0", [], |row| {
                row.get::<_, i32>(0)
            })
            .is_ok_and(|c| c > 0);
        if exists {
            return Ok(0);
        }

        let device = Device::new();
        let json = serde_json::to_string(&device)
            .map_err(|e| StoreError::Serialization(e.to_string().into()))?;
        conn.execute(
            "INSERT INTO device (id, data) VALUES (0, ?1)",
            params![json],
        )
        .map_err(rusqlite_err)?;

        Ok(0)
    }
}
