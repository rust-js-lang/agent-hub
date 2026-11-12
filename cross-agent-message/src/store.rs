//! The hub's memory: agents and every message, in one SQLite file, so the
//! history outlives the hub.

use std::path::Path;

use std::hash::{BuildHasher, Hasher};

use protocol::{Agent, Hold, Inbox, Message, Register, Send};
use rusqlite::{Connection, OptionalExtension, Row, params};

pub struct Store {
    db: Connection,
}

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

const MESSAGE_COLUMNS: &str = "id, sender, recipient, body, sent_at, read_at, person";

const AGENT_QUERY: &str = "
    SELECT name, machine, last_seen,
        (SELECT count(*) FROM messages WHERE recipient = agents.name AND read_at IS NULL),
        person
    FROM agents
";

/// Columns a store made by a build before this one lacks: who is the
/// person, and which messages they sent.
const ADDED: &[(&str, &str)] = &[("agents", "person"), ("messages", "person")];

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
    /// The store in the file at `path`, made with its tables if it's new.
    pub fn open(path: &Path) -> rusqlite::Result<Store> {
        Store::with(Connection::open(path)?)
    }

    /// A store that lasts as long as it does, for tests.
    pub fn in_memory() -> rusqlite::Result<Store> {
        Store::with(Connection::open_in_memory()?)
    }

    fn with(db: Connection) -> rusqlite::Result<Store> {
        db.execute_batch(SCHEMA)?;
        for (table, column) in ADDED {
            let has: bool = db.query_row(
                "SELECT count(*) > 0 FROM pragma_table_info(?1) WHERE name = ?2",
                params![table, column],
                |row| row.get(0),
            )?;
            if !has {
                db.execute_batch(&format!(
                    "ALTER TABLE {table} ADD COLUMN {column} INTEGER NOT NULL DEFAULT 0"
                ))?;
            }
        }
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
             ON CONFLICT (name) DO UPDATE SET machine = ?2, last_seen = ?3",
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
        let mut query = self.db.prepare(&format!("{AGENT_QUERY} ORDER BY name"))?;
        query.query_map([], agent)?.collect()
    }

    /// Stores the message, sent now, the person's where `person` says.
    pub fn send(&self, send: &Send, now: u64, person: bool) -> rusqlite::Result<Message> {
        self.db.execute(
            "INSERT INTO messages (sender, recipient, body, sent_at, person) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![send.from, send.to, send.body, now, person],
        )?;
        Ok(Message {
            id: self.db.last_insert_rowid() as u64,
            from: send.from.clone(),
            to: send.to.clone(),
            body: send.body.clone(),
            sent_at: now,
            read_at: None,
            person,
        })
    }

    /// At most `max` of `name`'s unread messages, oldest first, read now.
    pub fn read_inbox(&mut self, name: &str, max: u32, now: u64) -> rusqlite::Result<Inbox> {
        let transaction = self.db.transaction()?;
        let mut messages: Vec<Message> = transaction
            .prepare(&format!(
                "SELECT {MESSAGE_COLUMNS} FROM messages
                 WHERE recipient = ?1 AND read_at IS NULL ORDER BY id LIMIT ?2"
            ))?
            .query_map(params![name, max], message)?
            .collect::<rusqlite::Result<_>>()?;
        for message in &mut messages {
            transaction.execute(
                "UPDATE messages SET read_at = ?1 WHERE id = ?2",
                params![now, message.id],
            )?;
            message.read_at = Some(now);
        }
        let remaining = transaction.query_row(
            "SELECT count(*) FROM messages WHERE recipient = ?1 AND read_at IS NULL",
            [name],
            |row| row.get(0),
        )?;
        transaction.commit()?;
        Ok(Inbox {
            messages,
            remaining,
        })
    }

    /// What was held when the hub last kept them: a hold is of a run that
    /// goes on while the hub is replaced, so a hub started again has it.
    pub fn holds(&self) -> rusqlite::Result<Vec<Hold>> {
        let mut query = self
            .db
            .prepare("SELECT name, holder, since, until FROM holds ORDER BY name")?;
        query
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

    /// Keeps `holds` as all that's held.
    pub fn keep_holds<'a>(
        &mut self,
        holds: impl Iterator<Item = &'a Hold>,
    ) -> rusqlite::Result<()> {
        let transaction = self.db.transaction()?;
        transaction.execute("DELETE FROM holds", [])?;
        for hold in holds {
            transaction.execute(
                "INSERT INTO holds (name, holder, since, until) VALUES (?1, ?2, ?3, ?4)",
                params![hold.name, hold.holder, hold.since, hold.until],
            )?;
        }
        transaction.commit()
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

fn agent(row: &Row) -> rusqlite::Result<Agent> {
    Ok(Agent {
        name: row.get(0)?,
        machine: row.get(1)?,
        last_seen: row.get(2)?,
        watching: false,
        unread: row.get(3)?,
        person: row.get(4)?,
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
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn register(store: &Store, name: &str, now: u64) {
        let agent = Register {
            name: name.to_string(),
            machine: "m1".to_string(),
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
                },
                9,
            )
            .unwrap();
        let agents = store.agents().unwrap();
        assert_eq!(agents.len(), 1);
        assert_eq!((agents[0].machine.as_str(), agents[0].last_seen), ("m6", 9));
        assert!(store.agent("nobody").unwrap().is_none());
    }
}
