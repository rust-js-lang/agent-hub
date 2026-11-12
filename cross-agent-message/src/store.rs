//! The hub's memory: agents and every message, in one SQLite file, so the
//! history outlives the hub.

use std::path::Path;

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
    CREATE TABLE IF NOT EXISTS holds (
        name TEXT PRIMARY KEY,
        holder TEXT NOT NULL,
        since INTEGER NOT NULL,
        until INTEGER NOT NULL
    );
";

const MESSAGE_COLUMNS: &str = "id, sender, recipient, body, sent_at, read_at";

const AGENT_QUERY: &str = "
    SELECT name, machine, last_seen,
        (SELECT count(*) FROM messages WHERE recipient = agents.name AND read_at IS NULL)
    FROM agents
";

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
        Ok(Store { db })
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

    /// Stores the message, sent now.
    pub fn send(&self, send: &Send, now: u64) -> rusqlite::Result<Message> {
        self.db.execute(
            "INSERT INTO messages (sender, recipient, body, sent_at) VALUES (?1, ?2, ?3, ?4)",
            params![send.from, send.to, send.body, now],
        )?;
        Ok(Message {
            id: self.db.last_insert_rowid() as u64,
            from: send.from.clone(),
            to: send.to.clone(),
            body: send.body.clone(),
            sent_at: now,
            read_at: None,
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
        };
        store.send(&send, now).unwrap()
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
