use std::{future::Future, pin::Pin};

use rusqlite::params;
use wacore_binary::Jid;

use crate::{ChatEntry, Result, Session};

use super::SqliteSession;

impl Session for SqliteSession {
    fn get_chat(
        &self,
        jid: &Jid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<ChatEntry>>> + Send + '_>> {
        let jid_str = jid.to_string();

        Box::pin(async move {
            let conn = self.conn.lock().expect("sqlite lock poisoned");
            let mut stmt = conn.prepare("SELECT jid, name, kind FROM chats WHERE jid = ?1")?;

            let mut rows = stmt.query_map(params![jid_str], |row| {
                Ok(ChatEntry {
                    jid: row.get::<_, String>(0)?.parse().unwrap_or_default(),
                    name: row.get(1)?,
                    kind: row.get(2)?,
                })
            })?;
            match rows.next() {
                Some(row) => Ok(Some(row?)),
                None => Ok(None),
            }
        })
    }

    fn cache_chat(
        &self,
        chat: &ChatEntry,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let jid = chat.jid.to_string();
        let name = chat.name.clone();
        let kind = chat.kind.clone();

        Box::pin(async move {
            let conn = self.conn.lock().expect("sqlite lock poisoned");
            conn.execute(
                "INSERT OR REPLACE INTO chats (jid, name, kind) VALUES (?1, ?2, ?3)",
                params![jid, name, kind],
            )?;

            Ok(())
        })
    }

    fn get_chats(&self) -> Pin<Box<dyn Future<Output = Result<Vec<ChatEntry>>> + Send + '_>> {
        Box::pin(async move {
            let conn = self.conn.lock().expect("sqlite lock poisoned");
            let mut stmt = conn.prepare("SELECT jid, name, kind FROM chats")?;
            let rows = stmt.query_map([], |row| {
                Ok(ChatEntry {
                    jid: row.get::<_, String>(0)?.parse().unwrap_or_default(),
                    name: row.get(1)?,
                    kind: row.get(2)?,
                })
            })?;

            let mut result = Vec::new();
            for row in rows {
                result.push(row?);
            }

            Ok(result)
        })
    }

    fn remove_chat(&self, jid: &Jid) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let jid_str = jid.to_string();

        Box::pin(async move {
            let conn = self.conn.lock().expect("sqlite lock poisoned");
            conn.execute("DELETE FROM chats WHERE jid = ?1", params![jid_str])?;

            Ok(())
        })
    }

    fn is_contact(&self, jid: &Jid) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + '_>> {
        let jid_str = jid.to_string();

        Box::pin(async move {
            let conn = self.conn.lock().expect("sqlite lock poisoned");

            let count: i32 = conn.query_row(
                "SELECT COUNT(*) FROM contacts WHERE jid = ?1",
                params![jid_str],
                |row| row.get(0),
            )?;
            Ok(count > 0)
        })
    }
}
