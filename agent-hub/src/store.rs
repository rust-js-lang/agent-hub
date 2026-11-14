//! The hub's memory: agents and every message, in one SQLite file, so the
//! history outlives the hub.

use std::path::Path;

use std::hash::{BuildHasher, Hasher};

use protocol::{
    Agent, AwaySummary, Channel, Digest, DigestLine, EVERYONE, Hold, Inbox, Message, ReadInbox,
    Register, Retired, Send,
};
use rusqlite::{Connection, OptionalExtension, Row, params};
pub use sqlite_idempotency::{Outcome, Receipt};

pub struct Store {
    db: Connection,
}

mod managed;
pub use managed::SessionError;

const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS agents (
        name TEXT PRIMARY KEY,
        machine TEXT NOT NULL,
        last_seen INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sender TEXT NOT NULL,
        recipient TEXT NOT NULL,
        body TEXT NOT NULL,
        sent_at INTEGER NOT NULL,
        read_at INTEGER
    );
    CREATE INDEX IF NOT EXISTS unread ON messages (recipient, read_at, id);
    CREATE TABLE IF NOT EXISTS members (
        channel TEXT NOT NULL,
        agent TEXT NOT NULL,
        PRIMARY KEY (channel, agent)
    );
    CREATE TABLE IF NOT EXISTS settings (
        name TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS holds (
        name TEXT PRIMARY KEY,
        holder TEXT NOT NULL,
        since INTEGER NOT NULL,
        until INTEGER NOT NULL
    );
";

const MESSAGE_COLUMNS: &str =
    "id, sender, recipient, body, sent_at, read_at, person, channel, away, urgent";

/// Every inbox operation is scoped to the current life of its recipient.
const CURRENT_MAIL: &str = "recipient = ?1 AND id > (SELECT since_id FROM agents WHERE name = ?1)";

const AGENT_QUERY: &str = "
    SELECT name, machine, last_seen,
        (SELECT count(*) FROM messages WHERE recipient = agents.name AND read_at IS NULL AND away = 0 AND id > agents.since_id),
        person, retired_at
    FROM agents
";

/// Columns a store made by a build before this one lacks, each with its
/// type: who is the person, which messages they sent, and the channel a
/// message was sent to.
const ADDED: &[(&str, &str, &str)] = &[
    ("agents", "person", "INTEGER NOT NULL DEFAULT 0"),
    ("messages", "person", "INTEGER NOT NULL DEFAULT 0"),
    ("messages", "channel", "TEXT"),
    ("agents", "retired_at", "INTEGER"),
    ("agents", "since_id", "INTEGER NOT NULL DEFAULT 0"),
    ("messages", "away", "INTEGER NOT NULL DEFAULT 0"),
    ("messages", "urgent", "INTEGER NOT NULL DEFAULT 0"),
    ("agents", "watching", "INTEGER NOT NULL DEFAULT 0"),
    ("agents", "generation", "TEXT NOT NULL DEFAULT ''"),
];

/// A key no one would guess, twelve digits in three groups, `4821-7730-1182`:
/// of the randomness std seeds each `RandomState` with.
fn new_key() -> String {
    let random = || {
        std::collections::hash_map::RandomState::new()
            .build_hasher()
            .finish()
    };
    let digits = format!(
        "{:012}",
        (random() ^ random().rotate_left(32)) % 1_000_000_000_000
    );
    format!("{}-{}-{}", &digits[..4], &digits[4..8], &digits[8..])
}

impl Store {
    pub fn replay_receipt<T: serde::de::DeserializeOwned>(
        &self,
        receipt: Option<&Receipt>,
    ) -> Result<Option<T>, WriteError> {
        receipt
            .map(|receipt| sqlite_idempotency::lookup(&self.db, receipt))
            .transpose()
            .map(Option::flatten)
            .map_err(WriteError::from)
    }
    /// The store in the file at `path`, made with its tables if it's new.
    pub fn open(path: &Path) -> rusqlite::Result<Store> {
        Store::with(Connection::open(path)?)
    }

    /// A store that lasts as long as it does, for tests.
    pub fn in_memory() -> rusqlite::Result<Store> {
        Store::with(Connection::open_in_memory()?)
    }

    fn with(mut db: Connection) -> rusqlite::Result<Store> {
        let tx = db.transaction()?;
        tx.execute_batch(SCHEMA)?;
        sqlite_idempotency::initialize(&tx).map_err(initialization_error)?;
        for (table, column, kind) in ADDED {
            let has: bool = tx.query_row(
                "SELECT count(*) > 0 FROM pragma_table_info(?1) WHERE name = ?2",
                params![table, column],
                |row| row.get(0),
            )?;
            if !has {
                tx.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {column} {kind}"))?;
            }
        }
        tx.execute_batch(managed::SCHEMA)?;
        tx.commit()?;
        Ok(Store { db })
    }

    /// The key the person signs in to the dashboard with, made the first
    /// time it's asked and kept: what makes a message theirs.
    pub fn person_key(&self) -> rusqlite::Result<String> {
        let kept: Option<String> = self
            .db
            .query_row(
                "SELECT value FROM settings WHERE name = 'person_key'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(key) = kept {
            return Ok(key);
        }
        let key = new_key();
        self.db.execute(
            "INSERT INTO settings (name, value) VALUES ('person_key', ?1)",
            [&key],
        )?;
        Ok(key)
    }

    /// `name` is the person, seen now on the dashboard.
    pub fn see_person(&self, name: &str, now: u64) -> rusqlite::Result<()> {
        self.db.execute(
            "INSERT INTO agents (name, machine, last_seen, person) VALUES (?1, 'dashboard', ?2, 1)
             ON CONFLICT (name) DO UPDATE SET machine = 'dashboard', last_seen = ?2, person = 1",
            params![name, now],
        )?;
        Ok(())
    }

    /// The agent is seen now, on its machine.
    pub fn see(&self, agent: &Register, now: u64) -> rusqlite::Result<()> {
        self.db.execute(
            "INSERT INTO agents (name, machine, last_seen) VALUES (?1, ?2, ?3)
             ON CONFLICT (name) DO UPDATE SET machine = ?2, last_seen = ?3,
                 since_id = CASE WHEN retired_at IS NULL THEN since_id ELSE (SELECT COALESCE(MAX(id), 0) FROM messages) END,
                 retired_at = NULL",
            params![agent.name, agent.machine, now],
        )?;
        Ok(())
    }

    /// The agent named `name`, if the hub has seen it. It isn't
    /// [`Agent::watching`]: the hub knows that, not the store.
    pub fn agent(&self, name: &str) -> rusqlite::Result<Option<Agent>> {
        self.db
            .query_row(&format!("{AGENT_QUERY} WHERE name = ?1"), [name], agent)
            .optional()
    }

    /// Every agent, by name.
    pub fn agents(&self) -> rusqlite::Result<Vec<Agent>> {
        let mut query = self.db.prepare(&format!(
            "{AGENT_QUERY} WHERE retired_at IS NULL ORDER BY name"
        ))?;
        query.query_map([], agent)?.collect()
    }

    /// Stores the message, sent now, the person's where `person` says.
    pub fn send(&self, send: &Send, now: u64, person: bool) -> rusqlite::Result<Message> {
        self.deliver(send, &send.to, None, now, person)
    }

    pub fn send_receipted(
        &mut self,
        send: &Send,
        now: u64,
        person: bool,
        receipt: Option<&Receipt>,
    ) -> Result<Outcome<Message>, WriteError> {
        sqlite_idempotency::execute::<_, WriteError>(&mut self.db, receipt, |tx| {
            Ok(insert_message(
                tx, send, &send.to, None, now, person, false, person,
            )?)
        })
    }

    /// The agents in `channel`, by name: every agent of `everyone`, those
    /// that joined of another. Never the person, who reads every channel
    /// on the dashboard.
    pub fn members(&self, channel: &str) -> rusqlite::Result<Vec<String>> {
        if channel == EVERYONE {
            let mut query = self.db.prepare(
                "SELECT name FROM agents WHERE person = 0 AND retired_at IS NULL ORDER BY name",
            )?;
            return query.query_map([], |row| row.get(0))?.collect();
        }
        let mut query = self
            .db
            .prepare("SELECT agent FROM members WHERE channel = ?1 ORDER BY agent")?;
        query.query_map([channel], |row| row.get(0))?.collect()
    }

    /// Every channel, `everyone` first, then those an agent joined, by name.
    pub fn channels(&self) -> rusqlite::Result<Vec<Channel>> {
        let mut names = vec![EVERYONE.to_string()];
        let mut query = self
            .db
            .prepare("SELECT DISTINCT channel FROM members ORDER BY channel")?;
        for name in query.query_map([], |row| row.get::<_, String>(0))? {
            names.push(name?);
        }
        (names.into_iter())
            .map(|name| {
                Ok(Channel {
                    members: self.members(&name)?,
                    name,
                })
            })
            .collect()
    }

    /// `agent` is in `channel` from now on.
    pub fn join(&self, channel: &str, agent: &str) -> rusqlite::Result<()> {
        self.db.execute(
            "INSERT OR IGNORE INTO members (channel, agent) VALUES (?1, ?2)",
            params![channel, agent],
        )?;
        Ok(())
    }

    /// `agent` is no longer in `channel`.
    pub fn leave(&self, channel: &str, agent: &str) -> rusqlite::Result<()> {
        self.db.execute(
            "DELETE FROM members WHERE channel = ?1 AND agent = ?2",
            params![channel, agent],
        )?;
        Ok(())
    }

    /// Stores a copy of the message for each of `recipients`, sent now to
    /// `channel`: all of them, or none.
    pub fn broadcast(
        &mut self,
        send: &Send,
        channel: &str,
        recipients: &[String],
        now: u64,
        person: bool,
    ) -> Result<Vec<Message>, WriteError> {
        let recipients: Vec<(String, bool)> = recipients
            .iter()
            .map(|name| (name.clone(), false))
            .collect();
        self.broadcast_with_away(send, channel, &recipients, now, person)
    }

    pub fn broadcast_with_away(
        &mut self,
        send: &Send,
        channel: &str,
        recipients: &[(String, bool)],
        now: u64,
        person: bool,
    ) -> Result<Vec<Message>, WriteError> {
        self.broadcast_receipted(send, channel, recipients, now, person, None)
            .map(Outcome::into_inner)
    }

    pub fn broadcast_receipted(
        &mut self,
        send: &Send,
        channel: &str,
        recipients: &[(String, bool)],
        now: u64,
        person: bool,
        receipt: Option<&Receipt>,
    ) -> Result<Outcome<Vec<Message>>, WriteError> {
        sqlite_idempotency::execute::<_, WriteError>(&mut self.db, receipt, |transaction| {
            let mut messages = Vec::new();
            for (recipient, away) in recipients {
                messages.push(insert_message(
                    transaction,
                    send,
                    recipient,
                    Some(channel),
                    now,
                    person,
                    *away && !person,
                    person,
                )?);
            }
            Ok(messages)
        })
    }

    fn deliver(
        &self,
        send: &Send,
        recipient: &str,
        channel: Option<&str>,
        now: u64,
        person: bool,
    ) -> rusqlite::Result<Message> {
        insert_message(
            &self.db, send, recipient, channel, now, person, false, person,
        )
    }

    /// Full inbox behavior retained for existing clients.
    pub fn read_inbox(&mut self, name: &str, max: u32, now: u64) -> rusqlite::Result<Inbox> {
        self.read_mail(
            &ReadInbox {
                name: name.to_string(),
                max,
                channel: None,
                sender: None,
                skip: false,
                digest: false,

                defer_ack: false,

                up_to: None,
            },
            now,
        )
    }

    /// Select and acknowledge together: a digest never acknowledges ordinary mail.
    pub fn read_mail(&mut self, read: &ReadInbox, now: u64) -> rusqlite::Result<Inbox> {
        let tx = self.db.transaction()?;
        let pending = format!("{CURRENT_MAIL} AND read_at IS NULL");
        let mut result = Inbox {
            messages: vec![],
            remaining: 0,
            digests: vec![],
            away: vec![],
            read_ids: vec![],
            skipped: 0,
        };
        let channel = read.channel.as_deref();
        if read.skip {
            result.read_ids = tx.prepare(&format!("SELECT id FROM messages WHERE {pending} AND (person = 0 AND urgent = 0) AND channel IS NOT NULL AND (?2 IS NULL OR channel = ?2) AND (?3 IS NULL OR sender = ?3) AND (?4 IS NULL OR id <= ?4) ORDER BY id"))?
                .query_map(params![read.name, channel, read.sender, read.up_to], |row| row.get(0))?.collect::<rusqlite::Result<_>>()?;
            result.skipped = result.read_ids.len() as u32;
        } else {
            if read.digest && channel.is_none() {
                let groups: Vec<(String, String, u32)> = tx.prepare(&format!("SELECT channel, sender, count(*) FROM messages WHERE {pending} AND away = 1 AND (person = 0 AND urgent = 0) GROUP BY channel, sender ORDER BY channel, sender"))?
                    .query_map([&read.name], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?.collect::<rusqlite::Result<_>>()?;
                for (channel, sender, count) in groups {
                    if let Some(summary) = result.away.iter_mut().find(|s| s.channel == channel) {
                        summary.count += count;
                        summary.senders.push((sender, count));
                    } else {
                        result.away.push(AwaySummary {
                            channel,
                            count,
                            senders: vec![(sender, count)],
                        });
                    }
                }
                result.read_ids = tx.prepare(&format!("SELECT id FROM messages WHERE {pending} AND away = 1 AND (person = 0 AND urgent = 0) ORDER BY id"))?
                    .query_map([&read.name], |row| row.get(0))?.collect::<rusqlite::Result<_>>()?;
            }
            // A legacy request reads every kind in full. A digest request reads
            // direct/human mail, unless it explicitly selects a channel.
            let filter = if read.digest && channel.is_none() {
                "(channel IS NULL OR (person = 1 OR urgent = 1))"
            } else {
                "1 = 1"
            };
            let sql = format!(
                "SELECT {MESSAGE_COLUMNS} FROM messages WHERE {pending}
                 AND (?2 IS NULL OR channel = ?2) AND {filter} ORDER BY id LIMIT ?3"
            );
            result.messages = tx
                .prepare(&sql)?
                .query_map(params![read.name, channel, read.max], message)?
                .collect::<rusqlite::Result<_>>()?;
            for message in &mut result.messages {
                result.read_ids.push(message.id);
                if !read.defer_ack {
                    message.read_at = Some(now);
                }
            }
        }
        if !read.defer_ack || read.skip {
            for id in &result.read_ids {
                tx.execute(
                    "UPDATE messages SET read_at = ?1 WHERE id = ?2",
                    params![now, id],
                )?;
            }
        }
        let filter = if read.digest && channel.is_none() {
            "(channel IS NULL OR (person = 1 OR urgent = 1))"
        } else {
            "1 = 1"
        };
        result.remaining = tx.query_row(
            &format!(
                "SELECT count(*) FROM messages WHERE {pending}
                      AND (?2 IS NULL OR channel = ?2) AND {filter} AND id NOT IN (SELECT CAST(value AS INTEGER) FROM json_each(?3))"
            ),
            params![read.name, channel, serde_json::to_string(&result.read_ids).map_err(json_error)?],
            |row| row.get(0),
        )?;
        if read.digest && channel.is_none() && !read.skip {
            let channels: Vec<(String, u32)> = tx.prepare(&format!("SELECT channel, count(*) FROM messages WHERE {pending} AND channel IS NOT NULL AND (person = 0 AND urgent = 0) AND away = 0 GROUP BY channel ORDER BY channel"))?
                .query_map([&read.name], |row| Ok((row.get(0)?, row.get(1)?)))?.collect::<rusqlite::Result<_>>()?;
            for (channel, unread) in channels {
                let first = tx.prepare(&format!("SELECT id, sender, body FROM messages WHERE {pending} AND channel = ?2 AND (person = 0 AND urgent = 0) AND away = 0 ORDER BY id LIMIT 5"))?
                    .query_map(params![read.name, channel], |row| {
                        let body: String = row.get(2)?;
                        Ok(DigestLine { id: row.get(0)?, from: row.get(1)?, first_line: body.lines().next().unwrap_or_default().chars().take(100).collect() })
                    })?.collect::<rusqlite::Result<_>>()?;
                result.digests.push(Digest {
                    channel: channel.clone(),
                    unread,
                    newest_id: tx.query_row(&format!("SELECT COALESCE(MAX(id), 0) FROM messages WHERE {pending} AND channel = ?2 AND (person = 0 AND urgent = 0)"), params![read.name, channel], |row| row.get(0))?,
                    first,
                });
            }
        }
        tx.commit()?;
        Ok(result)
    }

    /// Opening a single copy cannot acknowledge another recipient's mail.
    pub fn read_message(
        &mut self,
        name: &str,
        id: u64,
        now: u64,
    ) -> rusqlite::Result<Option<Message>> {
        self.read_message_deferred(name, id, now, false)
    }
    pub fn read_message_deferred(
        &mut self,
        name: &str,
        id: u64,
        now: u64,
        defer_ack: bool,
    ) -> rusqlite::Result<Option<Message>> {
        let tx = self.db.transaction()?;
        let found = tx
            .query_row(
                &format!("SELECT {MESSAGE_COLUMNS} FROM messages WHERE {CURRENT_MAIL} AND id = ?2"),
                params![name, id],
                message,
            )
            .optional()?;
        let found = if let Some(mut message) = found {
            if !defer_ack && message.read_at.is_none() {
                tx.execute(
                    "UPDATE messages SET read_at = ?1 WHERE id = ?2",
                    params![now, id],
                )?;
                message.read_at = Some(now);
            }
            Some(message)
        } else {
            None
        };
        tx.commit()?;
        Ok(found)
    }

    /// Tombstone, memberships and holds commit together.
    pub fn retire(&mut self, name: &str, now: u64) -> rusqlite::Result<Option<Retired>> {
        let tx = self.db.transaction()?;
        let changed = tx.execute("UPDATE agents SET retired_at = ?2, watching = 0 WHERE name = ?1 AND retired_at IS NULL AND person = 0", params![name, now])?;
        if changed == 0 {
            return Ok(None);
        }
        let channels = tx.execute("DELETE FROM members WHERE agent = ?1", [name])? as u32;
        let holds = tx.execute("DELETE FROM holds WHERE holder = ?1", [name])? as u32;
        let retired = Retired { channels, holds };
        tx.execute("INSERT INTO settings (name,value) VALUES (?1,?2) ON CONFLICT(name) DO UPDATE SET value=?2", params![format!("retired:{name}"), serde_json::to_string(&retired).map_err(json_error)?])?;
        tx.commit()?;
        Ok(Some(retired))
    }

    pub fn retirement_result(&self, name: &str) -> rusqlite::Result<Option<Retired>> {
        let text: Option<String> = self
            .db
            .query_row(
                "SELECT value FROM settings WHERE name=?1",
                [format!("retired:{name}")],
                |r| r.get(0),
            )
            .optional()?;
        text.map(|text| serde_json::from_str(&text).map_err(json_error))
            .transpose()
    }

    /// Candidates only: the hub must exclude connected watches while retiring.
    /// Human broadcasts protect a name just as a direct message does.
    pub fn forgettable(&self, cutoff: u64) -> rusqlite::Result<Vec<String>> {
        self.db.prepare("SELECT name FROM agents WHERE person = 0 AND retired_at IS NULL AND last_seen <= ?1 AND NOT EXISTS (SELECT 1 FROM messages WHERE recipient = agents.name AND id > agents.since_id AND read_at IS NULL AND (channel IS NULL OR person = 1)) ORDER BY name")?
            .query_map([cutoff], |row| row.get(0))?.collect()
    }

    /// Updates activity after a watch disconnects, without reviving a tombstone.
    pub fn touch(&self, name: &str, now: u64) -> rusqlite::Result<()> {
        self.db.execute(
            "UPDATE agents SET last_seen = ?2 WHERE name = ?1 AND retired_at IS NULL",
            params![name, now],
        )?;
        Ok(())
    }

    /// Unread actionable mail, grouped for a watch notice without bodies.
    pub fn notice(&self, name: &str, channels: bool) -> rusqlite::Result<Vec<(String, bool, u32)>> {
        self.db.prepare(&format!("SELECT sender, person, count(*) FROM messages WHERE {CURRENT_MAIL} AND read_at IS NULL AND away = 0 AND (?2 OR channel IS NULL OR (person = 1 OR urgent = 1)) GROUP BY sender, person ORDER BY person DESC, sender"))?
            .query_map(params![name, channels], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?.collect()
    }

    /// SQLite is the sole authority for holds, including across restarts.
    pub fn holds(&self) -> rusqlite::Result<Vec<Hold>> {
        read_holds(&self.db)
    }

    pub fn held(&mut self, now: u64) -> rusqlite::Result<Vec<Hold>> {
        let tx = self.db.transaction()?;
        tx.execute("DELETE FROM holds WHERE until <= ?1", [now])?;
        let holds = read_holds(&tx)?;
        tx.commit()?;
        Ok(holds)
    }

    pub fn take_hold(
        &mut self,
        take: &protocol::Take,
        now: u64,
    ) -> Result<protocol::Taken, WriteError> {
        self.take_hold_receipted(take, now, None)
            .map(Outcome::into_inner)
    }
    pub fn take_hold_receipted(
        &mut self,
        take: &protocol::Take,
        now: u64,
        receipt: Option<&Receipt>,
    ) -> Result<Outcome<protocol::Taken>, WriteError> {
        sqlite_idempotency::execute::<_, WriteError>(&mut self.db, receipt, |tx| {
            let mut holds = read_holds(tx)?
                .into_iter()
                .map(|hold| (hold.name.clone(), hold))
                .collect();
            let taken = crate::holds::take_hold(&mut holds, take, now);
            if taken.yours {
                write_holds(tx, holds.values())?;
            }
            Ok(taken)
        })
    }

    pub fn release_hold(
        &mut self,
        release: &protocol::Release,
        now: u64,
    ) -> Result<Result<Vec<Hold>, String>, WriteError> {
        self.release_hold_receipted(release, now, None)
            .map(|result| result.map(Outcome::into_inner))
    }
    pub fn release_hold_receipted(
        &mut self,
        release: &protocol::Release,
        now: u64,
        receipt: Option<&Receipt>,
    ) -> Result<Result<Outcome<Vec<Hold>>, String>, WriteError> {
        let result =
            sqlite_idempotency::execute(&mut self.db, receipt, |tx| -> Result<_, ReleaseError> {
                let mut holds = read_holds(tx)?
                    .into_iter()
                    .map(|hold| (hold.name.clone(), hold))
                    .collect();
                crate::holds::release_hold(&mut holds, release, now)
                    .map_err(ReleaseError::Refused)?;
                let held = crate::holds::held(&mut holds, now);
                write_holds(tx, holds.values())?;
                Ok(held)
            });
        match result {
            Ok(held) => Ok(Ok(held)),
            Err(ReleaseError::Refused(reason)) => Ok(Err(reason)),
            Err(ReleaseError::Store(error)) => Err(error),
        }
    }

    pub fn keep_holds<'a>(
        &mut self,
        holds: impl Iterator<Item = &'a Hold>,
    ) -> rusqlite::Result<()> {
        let tx = self.db.transaction()?;
        write_holds(&tx, holds)?;
        tx.commit()
    }

    pub fn acknowledge(&mut self, name: &str, ids: &[u64], now: u64) -> rusqlite::Result<Vec<u64>> {
        let tx = self.db.transaction()?;
        let mut changed = Vec::new();
        for id in ids {
            if tx.execute(&format!("UPDATE messages SET read_at = ?3 WHERE {CURRENT_MAIL} AND id = ?2 AND read_at IS NULL"), params![name, id, now])? > 0 { changed.push(*id); }
        }
        tx.commit()?;
        Ok(changed)
    }

    pub fn newest_unread(&self, name: &str, channels: bool) -> rusqlite::Result<u64> {
        self.db.query_row(&format!("SELECT COALESCE(MAX(id), 0) FROM messages WHERE {CURRENT_MAIL} AND read_at IS NULL AND away = 0 AND (?2 OR channel IS NULL OR person = 1 OR urgent = 1)"), params![name, channels], |row| row.get(0))
    }

    pub fn set_watching(&self, name: &str, watching: bool) -> rusqlite::Result<()> {
        self.db.execute(
            "UPDATE agents SET watching = ?2 WHERE name = ?1 AND retired_at IS NULL",
            params![name, watching],
        )?;
        Ok(())
    }
    pub fn watched(&self) -> rusqlite::Result<Vec<String>> {
        self.db
            .prepare("SELECT name FROM agents WHERE watching = 1 AND retired_at IS NULL")?
            .query_map([], |r| r.get(0))?
            .collect()
    }
    pub fn note_alive(&self, now: u64) -> rusqlite::Result<()> {
        self.db.execute("INSERT INTO settings (name,value) VALUES ('last_alive',?1) ON CONFLICT(name) DO UPDATE SET value = ?1", [now.to_string()])?;
        Ok(())
    }
    pub fn resume_holds(&mut self, now: u64) -> rusqlite::Result<()> {
        let tx = self.db.transaction()?;
        let last: Option<u64> = tx
            .query_row(
                "SELECT CAST(value AS INTEGER) FROM settings WHERE name = 'last_alive'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(last) = last {
            let pause = now.saturating_sub(last);
            tx.execute("DELETE FROM holds WHERE until <= ?1", [last])?;
            if pause <= 600_000 {
                tx.execute("UPDATE holds SET until = until + ?1", [pause])?;
            }
        }
        tx.execute("INSERT INTO settings (name,value) VALUES ('last_alive',?1) ON CONFLICT(name) DO UPDATE SET value = ?1", [now.to_string()])?;
        tx.commit()
    }
    pub fn health(&self) -> rusqlite::Result<()> {
        self.db
            .query_row("SELECT count(*) FROM agents", [], |r| r.get::<_, u64>(0))?;
        Ok(())
    }

    pub fn announce(&mut self, body: &str, now: u64) -> rusqlite::Result<Vec<Message>> {
        let tx = self.db.transaction()?;
        let names: Vec<String> = tx
            .prepare("SELECT name FROM agents WHERE person = 0 AND retired_at IS NULL")?
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        let send = Send {
            from: "hub".to_string(),
            to: "everyone".to_string(),
            body: body.to_string(),
            key: None,
        };
        let mut messages = Vec::new();
        for name in names {
            messages.push(insert_message(
                &tx,
                &send,
                &name,
                Some("everyone"),
                now,
                false,
                false,
                true,
            )?);
        }
        tx.commit()?;
        Ok(messages)
    }

    /// The newest `limit` messages, newest first: all of them, or those
    /// `agent` sent or was sent.
    pub fn history(&self, agent: Option<&str>, limit: u32) -> rusqlite::Result<Vec<Message>> {
        let mut query = self.db.prepare(&format!(
            "SELECT {MESSAGE_COLUMNS} FROM messages
             WHERE ?1 IS NULL OR sender = ?1 OR recipient = ?1
             ORDER BY id DESC LIMIT ?2"
        ))?;
        query.query_map(params![agent, limit], message)?.collect()
    }
}

fn json_error(error: serde_json::Error) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}

/// Structured errors for the keyed-write boundary; HTTP mapping never depends
/// on a driver's wrapper or on human-readable text.
#[derive(Debug)]
pub enum WriteError {
    Storage(rusqlite::Error),
    Idempotency(sqlite_idempotency::Error),
}
impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(error) => error.fmt(f),
            Self::Idempotency(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for WriteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Storage(error) => error,
            Self::Idempotency(error) => error,
        })
    }
}
impl From<rusqlite::Error> for WriteError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Storage(error)
    }
}
impl From<sqlite_idempotency::Error> for WriteError {
    fn from(error: sqlite_idempotency::Error) -> Self {
        match error {
            sqlite_idempotency::Error::Database(error) => Self::Storage(error),
            error => Self::Idempotency(error),
        }
    }
}

/// Only store construction retains the historical rusqlite result type.
/// Keyed request errors travel structurally through WriteError instead.
fn initialization_error(error: sqlite_idempotency::Error) -> rusqlite::Error {
    match error {
        sqlite_idempotency::Error::Database(error) => error,
        error => rusqlite::Error::ToSqlConversionFailure(Box::new(error)),
    }
}
enum ReleaseError {
    Store(WriteError),
    Refused(String),
}
impl From<rusqlite::Error> for ReleaseError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Store(error.into())
    }
}
impl From<sqlite_idempotency::Error> for ReleaseError {
    fn from(error: sqlite_idempotency::Error) -> Self {
        Self::Store(error.into())
    }
}
#[allow(clippy::too_many_arguments)]
fn insert_message(
    db: &Connection,
    send: &Send,
    recipient: &str,
    channel: Option<&str>,
    now: u64,
    person: bool,
    away: bool,
    urgent: bool,
) -> rusqlite::Result<Message> {
    db.execute("INSERT INTO messages (sender,recipient,body,sent_at,person,channel,away,urgent) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)", params![send.from,recipient,send.body,now,person,channel,away,urgent])?;
    Ok(Message {
        id: db.last_insert_rowid() as u64,
        from: send.from.clone(),
        to: recipient.to_string(),
        body: send.body.clone(),
        sent_at: now,
        read_at: None,
        person,
        channel: channel.map(str::to_string),
        away,
        urgent,
    })
}

fn read_holds(db: &Connection) -> rusqlite::Result<Vec<Hold>> {
    db.prepare("SELECT name, holder, since, until FROM holds ORDER BY name")?
        .query_map([], |row| {
            Ok(Hold {
                name: row.get(0)?,
                holder: row.get(1)?,
                since: row.get(2)?,
                until: row.get(3)?,
            })
        })?
        .collect()
}

fn write_holds<'a>(db: &Connection, holds: impl Iterator<Item = &'a Hold>) -> rusqlite::Result<()> {
    db.execute("DELETE FROM holds", [])?;
    for hold in holds {
        db.execute(
            "INSERT INTO holds (name, holder, since, until) VALUES (?1, ?2, ?3, ?4)",
            params![hold.name, hold.holder, hold.since, hold.until],
        )?;
    }
    Ok(())
}

fn agent(row: &Row) -> rusqlite::Result<Agent> {
    Ok(Agent {
        name: row.get(0)?,
        machine: row.get(1)?,
        last_seen: row.get(2)?,
        watching: false,
        unread: row.get(3)?,
        person: row.get(4)?,
        away: false,
        retired_at: row.get(5)?,
    })
}

fn message(row: &Row) -> rusqlite::Result<Message> {
    Ok(Message {
        id: row.get(0)?,
        from: row.get(1)?,
        to: row.get(2)?,
        body: row.get(3)?,
        sent_at: row.get(4)?,
        read_at: row.get(5)?,
        person: row.get(6)?,
        channel: row.get(7)?,
        away: row.get(8)?,
        urgent: row.get(9)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn register(store: &Store, name: &str, now: u64) {
        let agent = Register {
            name: name.to_string(),
            machine: "m1".to_string(),
            reconnect: false,
        };
        store.see(&agent, now).unwrap();
    }

    fn send(store: &Store, from: &str, to: &str, body: &str, now: u64) -> Message {
        let send = Send {
            from: from.to_string(),
            to: to.to_string(),
            body: body.to_string(),
            key: None,
        };
        store.send(&send, now, false).unwrap()
    }

    fn test_receipt(
        actor: &str,
        nonce: &str,
        operation: &str,
        body: &impl serde::Serialize,
    ) -> Result<Receipt, sqlite_idempotency::Error> {
        let bytes =
            serde_json::to_vec(&(operation, body)).map_err(sqlite_idempotency::Error::Encode)?;
        Receipt::new(actor, nonce, &crate::updates::sha256(&bytes))
    }
    #[test]
    fn the_persons_key_is_made_once_and_kept() {
        let store = Store::in_memory().unwrap();
        let key = store.person_key().unwrap();
        assert_eq!(key.len(), 14);
        assert!(
            key.split('-')
                .all(|group| group.len() == 4 && group.chars().all(|c| c.is_ascii_digit()))
        );
        assert_eq!(store.person_key().unwrap(), key);
        assert_ne!(new_key(), new_key());

        store.see_person("tu", 5).unwrap();
        register(&store, "a", 6);
        let tu = store.agent("tu").unwrap().unwrap();
        assert!(tu.person);
        assert_eq!(tu.machine, "dashboard");
        assert!(!store.agent("a").unwrap().unwrap().person);
        let sent = Send {
            from: "tu".to_string(),
            to: "a".to_string(),
            body: "from me".to_string(),
            key: None,
        };
        assert!(store.send(&sent, 7, true).unwrap().person);
        assert!(store.history(None, 10).unwrap()[0].person);
    }

    #[test]
    fn a_store_of_a_build_before_gains_what_was_added() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE agents (name TEXT PRIMARY KEY, machine TEXT NOT NULL, last_seen INTEGER NOT NULL);
             CREATE TABLE messages (id INTEGER PRIMARY KEY AUTOINCREMENT, sender TEXT NOT NULL,
                 recipient TEXT NOT NULL, body TEXT NOT NULL, sent_at INTEGER NOT NULL, read_at INTEGER);
             INSERT INTO agents VALUES ('a', 'm1', 1), ('b', 'm1', 1);
             INSERT INTO messages (sender, recipient, body, sent_at) VALUES ('a', 'b', 'old', 2);",
        )
        .unwrap();
        let store = Store::with(db).unwrap();
        assert!(!store.agent("a").unwrap().unwrap().person);
        let history = store.history(None, 10).unwrap();
        assert_eq!(
            (history[0].body.as_str(), history[0].person),
            ("old", false)
        );
    }

    #[test]
    fn a_channel_is_its_members_and_everyone_is_every_agent() {
        let mut store = Store::in_memory().unwrap();
        for name in ["a", "b", "c"] {
            register(&store, name, 1);
        }
        store.see_person("tu", 1).unwrap();
        assert_eq!(store.members(EVERYONE).unwrap(), ["a", "b", "c"]);
        assert!(store.members("team").unwrap().is_empty());
        store.join("team", "c").unwrap();
        store.join("team", "a").unwrap();
        store.join("team", "a").unwrap();
        assert_eq!(store.members("team").unwrap(), ["a", "c"]);
        let names =
            |channels: Vec<Channel>| channels.into_iter().map(|c| c.name).collect::<Vec<_>>();
        assert_eq!(names(store.channels().unwrap()), [EVERYONE, "team"]);
        store.leave("team", "a").unwrap();
        assert_eq!(store.members("team").unwrap(), ["c"]);

        let send = Send {
            from: "tu".to_string(),
            to: "team".to_string(),
            body: "all of you".to_string(),
            key: None,
        };
        let recipients = ["b".to_string(), "c".to_string()];
        let sent = store
            .broadcast(&send, "team", &recipients, 5, true)
            .unwrap();
        assert_eq!(sent.len(), 2);
        assert_eq!((sent[0].to.as_str(), sent[1].to.as_str()), ("b", "c"));
        assert!(
            sent.iter()
                .all(|m| m.person && m.channel.as_deref() == Some("team") && m.sent_at == 5)
        );
        let inbox = store.read_inbox("c", 10, 6).unwrap();
        assert_eq!(inbox.messages[0].channel.as_deref(), Some("team"));
        assert_eq!(store.agent("b").unwrap().unwrap().unread, 1);
    }

    #[test]
    fn what_is_held_is_kept_as_all_that_is() {
        let mut store = Store::in_memory().unwrap();
        assert!(store.holds().unwrap().is_empty());
        let hold = |name: &str, holder: &str| Hold {
            name: name.to_string(),
            holder: holder.to_string(),
            since: 1,
            until: 9,
        };
        let held = [hold("deploy", "b"), hold("check", "a")];
        store.keep_holds(held.iter()).unwrap();
        assert_eq!(
            store.holds().unwrap(),
            [hold("check", "a"), hold("deploy", "b")]
        );
        store.keep_holds(held[..1].iter()).unwrap();
        assert_eq!(store.holds().unwrap(), [hold("deploy", "b")]);
    }

    #[test]
    fn an_inbox_is_read_oldest_first_once() {
        let mut store = Store::in_memory().unwrap();
        register(&store, "a", 1);
        register(&store, "b", 1);
        let first = send(&store, "a", "b", "one", 2);
        let second = send(&store, "a", "b", "two", 3);
        send(&store, "b", "a", "back", 4);

        assert_eq!(store.agent("b").unwrap().unwrap().unread, 2);
        let inbox = store.read_inbox("b", 1, 5).unwrap();
        assert_eq!(inbox.remaining, 1);
        assert_eq!(inbox.messages.len(), 1);
        assert_eq!(inbox.messages[0].id, first.id);
        assert_eq!(inbox.messages[0].read_at, Some(5));

        let inbox = store.read_inbox("b", 10, 6).unwrap();
        assert_eq!(inbox.remaining, 0);
        assert_eq!(inbox.messages[0].id, second.id);
        assert!(store.read_inbox("b", 10, 7).unwrap().messages.is_empty());
        assert_eq!(store.agent("a").unwrap().unwrap().unread, 1);
    }

    #[test]
    fn history_is_newest_first_and_filters_by_agent() {
        let store = Store::in_memory().unwrap();
        send(&store, "a", "b", "1", 1);
        send(&store, "b", "c", "2", 2);
        send(&store, "c", "a", "3", 3);
        let bodies =
            |messages: Vec<Message>| messages.into_iter().map(|m| m.body).collect::<Vec<_>>();
        assert_eq!(bodies(store.history(None, 10).unwrap()), ["3", "2", "1"]);
        assert_eq!(bodies(store.history(Some("a"), 10).unwrap()), ["3", "1"]);
        assert_eq!(bodies(store.history(None, 2).unwrap()), ["3", "2"]);
    }

    #[test]
    fn seeing_an_agent_again_moves_it() {
        let store = Store::in_memory().unwrap();
        register(&store, "a", 1);
        store
            .see(
                &Register {
                    name: "a".to_string(),
                    machine: "m6".to_string(),
                    reconnect: false,
                },
                9,
            )
            .unwrap();
        let agents = store.agents().unwrap();
        assert_eq!(agents.len(), 1);
        assert_eq!((agents[0].machine.as_str(), agents[0].last_seen), ("m6", 9));
        assert!(store.agent("nobody").unwrap().is_none());
    }
    fn digest_request(name: &str) -> ReadInbox {
        ReadInbox {
            name: name.to_string(),
            max: 20,
            channel: None,
            sender: None,
            skip: false,
            digest: true,

            defer_ack: false,

            up_to: None,
        }
    }

    #[test]
    fn a_new_life_cannot_read_old_mail_even_at_the_same_timestamp() {
        let mut store = Store::in_memory().unwrap();
        register(&store, "a", 1);
        register(&store, "b", 1);
        store.join("team", "b").unwrap();
        let old = send(&store, "a", "b", "old life", 5);
        store.retire("b", 5).unwrap().unwrap();
        assert!(!store.members(EVERYONE).unwrap().contains(&"b".to_string()));
        assert!(store.members("team").unwrap().is_empty());
        register(&store, "b", 5);
        let new = send(&store, "a", "b", "new life", 5);
        assert_eq!(store.agent("b").unwrap().unwrap().unread, 1);
        assert!(store.read_message("b", old.id, 6).unwrap().is_none());
        assert!(store.read_message("a", new.id, 6).unwrap().is_none());
        let inbox = store.read_mail(&digest_request("b"), 6).unwrap();
        assert_eq!(
            inbox.messages.iter().map(|m| m.id).collect::<Vec<_>>(),
            [new.id]
        );
        assert!(
            store
                .history(None, 10)
                .unwrap()
                .iter()
                .find(|m| m.id == old.id)
                .unwrap()
                .read_at
                .is_none()
        );
        assert_eq!(
            store.read_message("b", new.id, 7).unwrap().unwrap().read_at,
            Some(6)
        );
    }

    #[test]
    fn digests_never_acknowledge_mail_and_bulk_skip_never_drops_direct_or_human_mail() {
        let mut store = Store::in_memory().unwrap();
        for name in ["a", "b", "c"] {
            register(&store, name, 1);
        }
        let direct = send(&store, "a", "b", "direct", 2);
        let broadcast = |from: &str| Send {
            from: from.to_string(),
            to: "team".to_string(),
            body: "ask\nlong body".to_string(),
            key: None,
        };
        for _ in 0..7 {
            store
                .broadcast(&broadcast("a"), "team", &["b".to_string()], 3, false)
                .unwrap();
        }
        store
            .broadcast(&broadcast("c"), "team", &["b".to_string()], 3, false)
            .unwrap();
        let human = store
            .broadcast(&broadcast("tu"), "team", &["b".to_string()], 3, true)
            .unwrap()[0]
            .id;
        let first = store.read_mail(&digest_request("b"), 4).unwrap();
        assert_eq!(
            first.messages.iter().map(|m| m.id).collect::<Vec<_>>(),
            [direct.id, human]
        );
        assert_eq!(first.digests[0].unread, 8);
        assert_eq!(first.digests[0].first.len(), 5);
        assert_eq!(first.digests[0].first[0].first_line, "ask");
        assert_eq!(store.agent("b").unwrap().unwrap().unread, 8);
        assert_eq!(
            store.read_mail(&digest_request("b"), 5).unwrap().digests[0].unread,
            8
        );
        let direct = send(&store, "a", "b", "another direct", 6);
        let human = store
            .broadcast(&broadcast("tu"), "team", &["b".to_string()], 6, true)
            .unwrap()[0]
            .id;
        let mut skip = digest_request("b");
        skip.skip = true;
        skip.sender = Some("a".to_string());
        assert_eq!(store.read_mail(&skip, 7).unwrap().skipped, 7);
        assert!(
            store
                .history(None, 30)
                .unwrap()
                .iter()
                .filter(|m| [direct.id, human].contains(&m.id))
                .all(|m| m.read_at.is_none())
        );
        skip.sender = None;
        skip.channel = Some("team".to_string());
        assert_eq!(store.read_mail(&skip, 8).unwrap().skipped, 1);
        let inbox = store.read_mail(&digest_request("b"), 9).unwrap();
        assert_eq!(inbox.messages.len(), 2);
        assert!(inbox.digests.is_empty());
    }

    #[test]
    fn away_summaries_are_once_only_but_legacy_clients_read_the_full_bodies() {
        let mut store = Store::in_memory().unwrap();
        register(&store, "b", 1);
        let send = Send {
            from: "a".to_string(),
            to: "team".to_string(),
            body: "body".to_string(),
            key: None,
        };
        let away = store
            .broadcast_with_away(&send, "team", &[("b".to_string(), true)], 2, false)
            .unwrap()[0]
            .id;
        let urgent = store
            .broadcast_with_away(&send, "team", &[("b".to_string(), true)], 3, true)
            .unwrap()[0]
            .clone();
        assert!(!urgent.away);
        assert_eq!(store.agent("b").unwrap().unwrap().unread, 1);
        let inbox = store.read_mail(&digest_request("b"), 4).unwrap();
        assert_eq!(inbox.away[0].count, 1);
        assert_eq!(inbox.messages[0].id, urgent.id);
        assert!(inbox.read_ids.contains(&away));
        assert!(
            store
                .read_mail(&digest_request("b"), 5)
                .unwrap()
                .away
                .is_empty()
        );
        assert!(store.read_message("b", away, 6).unwrap().is_some());
        store
            .broadcast_with_away(&send, "team", &[("b".to_string(), true)], 7, false)
            .unwrap();
        let legacy = store.read_inbox("b", 20, 8).unwrap();
        assert_eq!(legacy.messages.len(), 1);
        assert_eq!(legacy.messages[0].body, "body");
        assert!(legacy.away.is_empty());
    }

    #[test]
    fn inbox_acknowledgements_roll_back_if_any_update_fails() {
        let mut store = Store::in_memory().unwrap();
        register(&store, "b", 1);
        let send = Send {
            from: "a".to_string(),
            to: "team".to_string(),
            body: "away".to_string(),
            key: None,
        };
        store
            .broadcast_with_away(&send, "team", &[("b".to_string(), true)], 2, false)
            .unwrap();
        send_direct_for_rollback(&store);
        store.db.execute_batch("CREATE TRIGGER fail_direct BEFORE UPDATE OF read_at ON messages WHEN OLD.channel IS NULL BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
        assert!(store.read_mail(&digest_request("b"), 4).is_err());
        assert!(
            store
                .history(None, 20)
                .unwrap()
                .iter()
                .all(|m| m.read_at.is_none())
        );
        store.db.execute_batch("DROP TRIGGER fail_direct").unwrap();
        assert_eq!(
            store.read_mail(&digest_request("b"), 5).unwrap().away[0].count,
            1
        );
    }

    fn send_direct_for_rollback(store: &Store) {
        send(store, "a", "b", "direct", 3);
    }

    #[test]
    fn retirement_is_atomic_and_persists_without_destroying_history() {
        let path = std::env::temp_dir().join(format!(
            "cam-retire-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        {
            let mut store = Store::open(&path).unwrap();
            register(&store, "b", 1);
            store.join("team", "b").unwrap();
            send(&store, "a", "b", "never read", 2);
            let hold = Hold {
                name: "check".to_string(),
                holder: "b".to_string(),
                since: 1,
                until: 1000,
            };
            store.keep_holds(std::iter::once(&hold)).unwrap();
            store.db.execute_batch("CREATE TRIGGER fail_retire BEFORE DELETE ON members BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
            assert!(store.retire("b", 3).is_err());
            assert!(store.agent("b").unwrap().unwrap().retired_at.is_none());
            assert_eq!(store.holds().unwrap().len(), 1);
            assert_eq!(store.members("team").unwrap(), ["b"]);
            store.db.execute_batch("DROP TRIGGER fail_retire").unwrap();
            let retired = store.retire("b", 4).unwrap().unwrap();
            assert_eq!((retired.channels, retired.holds), (1, 1));
        }
        let store = Store::open(&path).unwrap();
        assert!(store.agents().unwrap().is_empty());
        assert!(store.holds().unwrap().is_empty());
        assert!(store.history(None, 10).unwrap()[0].read_at.is_none());
        drop(store);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn forgetting_protects_direct_and_human_mail_but_not_channel_backlogs() {
        let mut store = Store::in_memory().unwrap();
        for name in ["direct", "human", "channel", "empty"] {
            register(&store, name, 1);
        }
        store.see_person("tu", 1).unwrap();
        send(&store, "a", "direct", "direct", 2);
        let send = Send {
            from: "tu".to_string(),
            to: "team".to_string(),
            body: "human".to_string(),
            key: None,
        };
        store
            .broadcast(&send, "team", &["human".to_string()], 2, true)
            .unwrap();
        store
            .broadcast(&send, "team", &["channel".to_string()], 2, false)
            .unwrap();
        assert_eq!(store.forgettable(10).unwrap(), ["channel", "empty"]);
    }
    #[test]
    fn failed_hold_writes_leave_the_previous_owner_and_deadline_intact() {
        let mut store = Store::in_memory().unwrap();
        let take = protocol::Take {
            name: "check".to_string(),
            holder: "a".to_string(),
            minutes: 30,
        };
        let original = store.take_hold(&take, 1).unwrap().hold;
        store.db.execute_batch("CREATE TRIGGER fail_hold_insert BEFORE INSERT ON holds BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
        assert!(store.take_hold(&take, 10).is_err());
        assert_eq!(store.holds().unwrap(), std::slice::from_ref(&original));
        store.db.execute_batch("DROP TRIGGER fail_hold_insert; CREATE TRIGGER fail_hold_delete BEFORE DELETE ON holds BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
        let release = protocol::Release {
            name: "check".to_string(),
            holder: "a".to_string(),
        };
        assert!(store.release_hold(&release, 11).is_err());
        assert_eq!(store.holds().unwrap(), [original]);
        store
            .db
            .execute_batch("DROP TRIGGER fail_hold_delete")
            .unwrap();
        assert!(
            store
                .release_hold(&release, 12)
                .unwrap()
                .unwrap()
                .is_empty()
        );
    }
    #[test]
    fn deferred_delivery_never_acknowledges_unprinted_mail_and_ack_is_scoped() {
        let mut store = Store::in_memory().unwrap();
        register(&store, "a", 1);
        register(&store, "b", 1);
        let old = send(&store, "a", "b", "first", 2);
        let mut request = digest_request("b");
        request.defer_ack = true;
        let lost = store.read_mail(&request, 3).unwrap();
        assert_eq!(lost.messages[0].id, old.id);
        assert_eq!(store.agent("b").unwrap().unwrap().unread, 1);
        let new = send(&store, "a", "b", "arrived during retry", 4);
        let retry = store.read_mail(&request, 5).unwrap();
        assert_eq!(
            retry.messages.iter().map(|m| m.id).collect::<Vec<_>>(),
            [old.id, new.id]
        );
        assert!(retry.messages.iter().all(|m| m.read_at.is_none()));
        assert!(store.acknowledge("a", &[old.id], 6).unwrap().is_empty());
        assert_eq!(store.acknowledge("b", &[old.id], 6).unwrap(), [old.id]);
        assert!(store.acknowledge("b", &[old.id], 7).unwrap().is_empty());
        assert_eq!(store.agent("b").unwrap().unwrap().unread, 1);
        assert_eq!(store.read_mail(&request, 8).unwrap().messages[0].id, new.id);
    }

    #[test]
    fn bounded_skip_does_not_consume_arrivals_after_its_snapshot_or_system_announcements() {
        let mut store = Store::in_memory().unwrap();
        register(&store, "b", 1);
        let message = Send {
            from: "a".to_string(),
            to: "team".to_string(),
            body: "channel".to_string(),
            key: None,
        };
        let first = store
            .broadcast(&message, "team", &["b".to_string()], 2, false)
            .unwrap()[0]
            .id;
        let later = store
            .broadcast(&message, "team", &["b".to_string()], 3, false)
            .unwrap()[0]
            .id;
        let urgent = store.announce("upgrade", 3).unwrap()[0].id;
        let mut skip = digest_request("b");
        skip.skip = true;
        skip.up_to = Some(first);
        skip.channel = Some("team".to_string());
        assert_eq!(store.read_mail(&skip, 4).unwrap().skipped, 1);
        assert_eq!(store.read_mail(&skip, 5).unwrap().skipped, 0);
        let pending = store.history(None, 20).unwrap();
        assert!(
            pending
                .iter()
                .find(|m| m.id == later)
                .unwrap()
                .read_at
                .is_none()
        );
        assert!(
            pending
                .iter()
                .find(|m| m.id == urgent)
                .unwrap()
                .read_at
                .is_none()
        );
        let inbox = store.read_mail(&digest_request("b"), 6).unwrap();
        assert!(inbox.messages.iter().any(|m| m.id == urgent && m.urgent));
    }

    #[test]
    fn nonce_replay_is_atomic_and_cannot_be_reused_for_a_different_write() {
        let mut store = Store::in_memory().unwrap();
        register(&store, "b", 1);
        let send = Send {
            from: "a".to_string(),
            to: "b".to_string(),
            body: "once".to_string(),
            key: None,
        };
        let receipt = test_receipt("a", "unique", "send", &send).unwrap();
        let Outcome::Fresh(first) = store
            .send_receipted(&send, 2, false, Some(&receipt))
            .unwrap()
        else {
            panic!("first send must be fresh");
        };
        assert_eq!(
            store
                .send_receipted(&send, 3, false, Some(&receipt))
                .unwrap(),
            Outcome::Replayed(first)
        );
        assert_eq!(store.history(None, 10).unwrap().len(), 1);
        let other = test_receipt(
            "a",
            "unique",
            "send",
            &Send {
                body: "different".to_string(),
                ..send.clone()
            },
        )
        .unwrap();
        assert!(store.send_receipted(&send, 4, false, Some(&other)).is_err());
        store.db.execute_batch("CREATE TRIGGER fail_receipt BEFORE INSERT ON receipts BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
        let failure = test_receipt("a", "different-nonce", "send", &send).unwrap();
        assert!(
            store
                .send_receipted(&send, 5, false, Some(&failure))
                .is_err()
        );
        assert_eq!(
            store.history(None, 10).unwrap().len(),
            1,
            "a failed receipt must roll back the message too"
        );
        store.db.execute_batch("DROP TRIGGER fail_receipt").unwrap();
        let take = protocol::Take {
            name: "check".to_string(),
            holder: "a".to_string(),
            minutes: 30,
        };
        let key = test_receipt("a", "hold-nonce", "take", &take).unwrap();
        let Outcome::Fresh(first) = store.take_hold_receipted(&take, 10, Some(&key)).unwrap()
        else {
            panic!("first take must be fresh");
        };
        assert_eq!(
            store.take_hold_receipted(&take, 20, Some(&key)).unwrap(),
            Outcome::Replayed(first),
            "a retried hold must not extend twice"
        );
    }

    #[test]
    fn refused_release_does_not_consume_the_key_and_can_succeed_later() {
        let mut store = Store::in_memory().unwrap();
        store
            .take_hold(
                &protocol::Take {
                    name: "check".into(),
                    holder: "b".into(),
                    minutes: 30,
                },
                1,
            )
            .unwrap();
        let release = protocol::Release {
            name: "check".into(),
            holder: "a".into(),
        };
        let receipt = test_receipt("a", "release-once", "release", &release).unwrap();
        assert!(
            store
                .release_hold_receipted(&release, 2, Some(&receipt))
                .unwrap()
                .is_err()
        );
        assert!(
            store
                .replay_receipt::<Vec<Hold>>(Some(&receipt))
                .unwrap()
                .is_none()
        );
        store
            .release_hold(
                &protocol::Release {
                    name: "check".into(),
                    holder: "b".into(),
                },
                3,
            )
            .unwrap()
            .unwrap();
        assert!(matches!(
            store
                .release_hold_receipted(&release, 4, Some(&receipt))
                .unwrap()
                .unwrap(),
            Outcome::Fresh(_)
        ));
        assert!(matches!(
            store
                .release_hold_receipted(&release, 5, Some(&receipt))
                .unwrap()
                .unwrap(),
            Outcome::Replayed(_)
        ));
    }

    #[test]
    fn broadcast_receipts_report_origin_and_never_deliver_to_changed_recipients() {
        let mut store = Store::in_memory().unwrap();
        let send = Send {
            from: "a".into(),
            to: "#team".into(),
            body: "once".into(),
            key: None,
        };
        let receipt = test_receipt("a", "broadcast-once", "broadcast", &send).unwrap();
        let recipients = [("b".into(), false), ("c".into(), false)];
        let Outcome::Fresh(first) = store
            .broadcast_receipted(&send, "team", &recipients, 1, false, Some(&receipt))
            .unwrap()
        else {
            panic!("first broadcast must be fresh");
        };
        let replay = store
            .broadcast_receipted(
                &send,
                "team",
                &[("d".into(), false)],
                2,
                false,
                Some(&receipt),
            )
            .unwrap();
        assert_eq!(replay, Outcome::Replayed(first));
        let history = store.history(None, 100).unwrap();
        assert_eq!(history.len(), 2);
        assert!(history.iter().all(|message| message.to != "d"));
    }

    #[test]
    fn stopped_hold_clock_survives_mid_pause_expiry_and_recovery_is_incremental() {
        let mut store = Store::in_memory().unwrap();
        let hold = Hold {
            name: "check".to_string(),
            holder: "a".to_string(),
            since: 1,
            until: 102,
        };
        store.keep_holds(std::iter::once(&hold)).unwrap();
        store.note_alive(100).unwrap();
        store.resume_holds(107).unwrap();
        assert_eq!(store.holds().unwrap()[0].until, 109);
        store.resume_holds(107).unwrap();
        assert_eq!(
            store.holds().unwrap()[0].until,
            109,
            "same recovery time cannot extend twice"
        );
        store.resume_holds(110).unwrap();
        assert_eq!(
            store.holds().unwrap()[0].until,
            112,
            "a failed start adds only further downtime"
        );
        store.note_alive(110).unwrap();
        store.resume_holds(700_111).unwrap();
        assert_eq!(
            store.holds().unwrap()[0].until,
            112,
            "a pause over ten minutes adds nothing"
        );
        assert!(store.held(700_111).unwrap().is_empty());
    }
}
