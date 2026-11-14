//! Durable scheduling over existing messages; triggers include legacy writers.

use managed_protocol::{
    Approval, Command, Delivery, DeliveryState, Lease, Response, Runtime, Session, Update,
};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, params};

use super::{MESSAGE_COLUMNS, Store, json_error, message};

pub const LEASE_MS: u64 = 60_000;

pub(super) const SCHEMA: &str = "
    UPDATE agents SET generation = lower(hex(randomblob(16))) WHERE generation = '';
    CREATE TABLE IF NOT EXISTS managed_sessions (
        name TEXT PRIMARY KEY, generation TEXT NOT NULL, machine TEXT NOT NULL,
        runtime TEXT NOT NULL, cwd TEXT NOT NULL, runtime_session_id TEXT,
        owner TEXT NOT NULL, lease_until INTEGER NOT NULL, active INTEGER NOT NULL DEFAULT 1,
        turns INTEGER NOT NULL DEFAULT 0, max_turns INTEGER NOT NULL,
        include_channels INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS managed_deliveries (
        message_id INTEGER PRIMARY KEY, name TEXT NOT NULL, generation TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'queued', runtime_turn_id TEXT,
        output TEXT NOT NULL DEFAULT '', error TEXT
    );
    CREATE INDEX IF NOT EXISTS managed_pending ON managed_deliveries(name,generation,state,message_id);
    CREATE TABLE IF NOT EXISTS managed_approvals (
        name TEXT NOT NULL, generation TEXT NOT NULL, id TEXT NOT NULL,
        message_id INTEGER NOT NULL, request TEXT NOT NULL, response TEXT,
        PRIMARY KEY(name,generation,id)
    );
    CREATE TRIGGER IF NOT EXISTS agent_generation_insert AFTER INSERT ON agents
        WHEN NEW.generation = '' BEGIN
        UPDATE agents SET generation = lower(hex(randomblob(16))) WHERE name = NEW.name;
    END;
    CREATE TRIGGER IF NOT EXISTS agent_generation_revive AFTER UPDATE OF retired_at ON agents
        WHEN OLD.retired_at IS NOT NULL AND NEW.retired_at IS NULL BEGIN
        UPDATE agents SET generation = lower(hex(randomblob(16))) WHERE name = NEW.name;
    END;
    CREATE TRIGGER IF NOT EXISTS managed_enqueue AFTER INSERT ON messages BEGIN
        INSERT INTO managed_deliveries(message_id,name,generation)
        SELECT NEW.id,s.name,s.generation FROM managed_sessions s JOIN agents a ON a.name=s.name
        WHERE s.name=NEW.recipient AND s.active=1 AND s.generation=a.generation AND a.retired_at IS NULL
          AND (NEW.channel IS NULL OR NEW.person=1 OR NEW.urgent=1 OR s.include_channels=1);
    END;
    CREATE TRIGGER IF NOT EXISTS managed_retire AFTER UPDATE OF retired_at ON agents
        WHEN NEW.retired_at IS NOT NULL BEGIN
        UPDATE managed_sessions SET active=0 WHERE name=NEW.name;
        UPDATE managed_deliveries SET state=CASE WHEN state='running' THEN 'uncertain' ELSE 'failed' END,
            error='Agent was retired' WHERE name=NEW.name AND state IN ('queued','running');
        DELETE FROM managed_approvals WHERE name=NEW.name;
    END;
";

#[derive(Debug)]
pub enum SessionError {
    Storage(rusqlite::Error),
    Refused(String),
}

impl From<rusqlite::Error> for SessionError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Storage(error)
    }
}

impl std::fmt::Display for SessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(error) => error.fmt(f),
            Self::Refused(reason) => f.write_str(reason),
        }
    }
}

impl std::error::Error for SessionError {}

fn refuse(reason: &str) -> SessionError {
    SessionError::Refused(reason.to_string())
}

fn runtime_name(runtime: Runtime) -> &'static str {
    match runtime {
        Runtime::Codex => "codex",
        Runtime::Claude => "claude",
    }
}

fn state_name(state: DeliveryState) -> &'static str {
    match state {
        DeliveryState::Queued => "queued",
        DeliveryState::Running => "running",
        DeliveryState::Completed => "completed",
        DeliveryState::Failed => "failed",
        DeliveryState::Uncertain => "uncertain",
    }
}

fn session(db: &Connection, name: &str) -> rusqlite::Result<Session> {
    let mut session = db.query_row(
        "SELECT name,generation,machine,runtime,cwd,runtime_session_id,lease_until,turns,max_turns,active FROM managed_sessions WHERE name=?1",
        [name], |row| Ok(Session {
            name: row.get(0)?, generation: row.get(1)?, machine: row.get(2)?,
            runtime: serde_json::from_value(serde_json::Value::String(row.get(3)?)).map_err(json_error)?,
            cwd: row.get(4)?, runtime_session_id: row.get(5)?, lease_until: row.get(6)?,
            turns: row.get(7)?, max_turns: row.get(8)?, active: row.get(9)?, approvals: Vec::new(),
        })
    )?;
    let mut query = db.prepare("SELECT id,request,response FROM managed_approvals WHERE name=?1 AND generation=?2 ORDER BY id")?;
    session.approvals = query
        .query_map(params![name, session.generation], |row| {
            let request: String = row.get(1)?;
            let response: Option<String> = row.get(2)?;
            Ok(Approval {
                id: row.get(0)?,
                request: serde_json::from_str(&request).map_err(json_error)?,
                response: response
                    .map(|value| serde_json::from_str(&value).map_err(json_error))
                    .transpose()?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?;
    Ok(session)
}

fn delivery(row: &Row) -> rusqlite::Result<Delivery> {
    Ok(Delivery {
        message: message(row)?,
        generation: row.get(10)?,
        state: serde_json::from_value(serde_json::Value::String(row.get(11)?))
            .map_err(json_error)?,
        runtime_turn_id: row.get(12)?,
        output: row.get(13)?,
        error: row.get(14)?,
    })
}

fn response(db: &Connection, name: &str) -> rusqlite::Result<Response> {
    let session = session(db, name)?;
    let columns = MESSAGE_COLUMNS
        .split(", ")
        .map(|column| format!("m.{column}"))
        .collect::<Vec<_>>()
        .join(",");
    let delivery = db.query_row(&format!(
        "SELECT {columns},d.generation,d.state,d.runtime_turn_id,d.output,d.error FROM managed_deliveries d JOIN messages m ON m.id=d.message_id
         WHERE d.name=?1 AND d.generation=?2
         ORDER BY CASE d.state WHEN 'uncertain' THEN 0 WHEN 'running' THEN 1 WHEN 'queued' THEN 2 ELSE 3 END,
             CASE WHEN d.state IN ('queued','running','uncertain') THEN d.message_id ELSE -d.message_id END LIMIT 1"
    ), params![name,session.generation], delivery).optional()?;
    Ok(Response { session, delivery })
}

fn authorize(db: &Connection, lease: &Lease, now: u64) -> Result<(), SessionError> {
    let valid: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM managed_sessions s JOIN agents a ON a.name=s.name
         WHERE s.name=?1 AND s.generation=?2 AND a.generation=?2 AND a.retired_at IS NULL
           AND s.owner=?3 AND s.lease_until>?4 AND s.active=1)",
        params![lease.name, lease.generation, lease.owner, now],
        |row| row.get(0),
    )?;
    if valid {
        Ok(())
    } else {
        Err(refuse(
            "Session ownership expired, changed, or was retired; stop the local runtime",
        ))
    }
}

fn running(tx: &Transaction<'_>, lease: &Lease, id: u64) -> Result<(), SessionError> {
    let valid: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM managed_deliveries WHERE message_id=?1 AND name=?2 AND generation=?3 AND state='running')",
        params![id,lease.name,lease.generation], |row| row.get(0))?;
    if valid {
        Ok(())
    } else {
        Err(refuse("This delivery is not running in this session"))
    }
}

impl Store {
    pub fn generation(&self, name: &str) -> rusqlite::Result<Option<String>> {
        self.db
            .query_row(
                "SELECT generation FROM agents WHERE name=?1 AND retired_at IS NULL",
                [name],
                |row| row.get(0),
            )
            .optional()
    }

    pub fn sessions(&self) -> rusqlite::Result<Vec<Response>> {
        let mut query = self
            .db
            .prepare("SELECT name FROM managed_sessions ORDER BY name")?;
        let names = query
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        names.iter().map(|name| response(&self.db, name)).collect()
    }

    pub fn session_command(
        &mut self,
        command: &Command,
        now: u64,
    ) -> Result<Response, SessionError> {
        let tx = self.db.transaction()?;
        let name = match command {
            Command::Open {
                name,
                owner,
                machine,
                runtime,
                cwd,
                max_turns,
                include_channels,
            } => {
                let generation: String = tx.query_row("SELECT generation FROM agents WHERE name=?1 AND person=0 AND retired_at IS NULL", [name], |row| row.get(0))?;
                let old: Option<(String,String,u64,String,String)> = tx.query_row(
                    "SELECT generation,owner,lease_until,runtime,cwd FROM managed_sessions WHERE name=?1", [name],
                    |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?))
                ).optional()?;
                if let Some((old_generation, old_owner, until, old_runtime, old_cwd)) = &old {
                    if old_owner != owner && *until > now {
                        return Err(refuse("Another worker owns this session"));
                    }
                    if old_generation == &generation
                        && (old_runtime != runtime_name(*runtime) || old_cwd != cwd)
                    {
                        return Err(refuse(
                            "Runtime and working directory are fixed for this agent generation; retire it to start another",
                        ));
                    }
                    if *until <= now {
                        tx.execute("UPDATE managed_deliveries SET state='uncertain',error='Worker stopped before confirming completion' WHERE name=?1 AND state='running'", [name])?;
                        tx.execute("DELETE FROM managed_approvals WHERE name=?1", [name])?;
                    }
                }
                tx.execute("INSERT INTO managed_sessions(name,generation,machine,runtime,cwd,owner,lease_until,max_turns,include_channels)
                    VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)
                    ON CONFLICT(name) DO UPDATE SET generation=?2,machine=?3,runtime=?4,cwd=?5,owner=?6,lease_until=?7,max_turns=?8,include_channels=?9,active=1,
                        runtime_session_id=CASE WHEN generation=?2 THEN runtime_session_id ELSE NULL END,
                        turns=CASE WHEN generation=?2 THEN turns ELSE 0 END",
                    params![name,generation,machine,runtime_name(*runtime),cwd,owner,now+LEASE_MS,max_turns,include_channels])?;
                name
            }
            Command::Poll { lease, claim } => {
                authorize(&tx, lease, now)?;
                tx.execute(
                    "UPDATE managed_sessions SET lease_until=?2 WHERE name=?1",
                    params![lease.name, now + LEASE_MS],
                )?;
                if *claim {
                    let blocked: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM managed_deliveries WHERE name=?1 AND generation=?2 AND state IN ('running','uncertain'))", params![lease.name,lease.generation], |row| row.get(0))?;
                    let budget: bool = tx.query_row(
                        "SELECT turns<max_turns FROM managed_sessions WHERE name=?1",
                        [&lease.name],
                        |row| row.get(0),
                    )?;
                    if !blocked && budget {
                        let changed = tx.execute("UPDATE managed_deliveries SET state='running' WHERE message_id=(SELECT MIN(message_id) FROM managed_deliveries WHERE name=?1 AND generation=?2 AND state='queued')", params![lease.name,lease.generation])?;
                        if changed != 0 {
                            tx.execute(
                                "UPDATE managed_sessions SET turns=turns+1 WHERE name=?1",
                                [&lease.name],
                            )?;
                        }
                    }
                }
                &lease.name
            }
            Command::Update { lease, update } => {
                authorize(&tx, lease, now)?;
                match update {
                    Update::SessionReady { session_id } => {
                        let old: Option<String> = tx.query_row(
                            "SELECT runtime_session_id FROM managed_sessions WHERE name=?1",
                            [&lease.name],
                            |row| row.get(0),
                        )?;
                        if old.as_ref().is_some_and(|old| old != session_id) {
                            return Err(refuse("Runtime resumed a different conversation"));
                        }
                        tx.execute(
                            "UPDATE managed_sessions SET runtime_session_id=?2 WHERE name=?1",
                            params![lease.name, session_id],
                        )?;
                    }
                    Update::Started {
                        message_id,
                        turn_id,
                    } => {
                        running(&tx, lease, *message_id)?;
                        tx.execute(
                            "UPDATE managed_deliveries SET runtime_turn_id=?2 WHERE message_id=?1",
                            params![message_id, turn_id],
                        )?;
                        tx.execute(
                            "UPDATE messages SET read_at=COALESCE(read_at,?2) WHERE id=?1",
                            params![message_id, now],
                        )?;
                    }
                    Update::Output { message_id, text } => {
                        running(&tx, lease, *message_id)?;
                        if text.len() > 128 * 1024 {
                            return Err(refuse("Turn output exceeds 128 KiB"));
                        }
                        tx.execute(
                            "UPDATE managed_deliveries SET output=?2 WHERE message_id=?1",
                            params![message_id, text],
                        )?;
                    }
                    Update::Finished {
                        message_id,
                        state,
                        error,
                    } => {
                        if !matches!(
                            state,
                            DeliveryState::Completed
                                | DeliveryState::Failed
                                | DeliveryState::Uncertain
                        ) {
                            return Err(refuse("Invalid terminal delivery state"));
                        }
                        let changed = tx.execute("UPDATE managed_deliveries SET state=?4,error=?5 WHERE message_id=?1 AND name=?2 AND generation=?3 AND state IN ('running',?4)", params![message_id,lease.name,lease.generation,state_name(*state),error])?;
                        if changed == 0 {
                            return Err(refuse(
                                "Delivery is not running or has a different outcome",
                            ));
                        }
                        tx.execute("DELETE FROM managed_approvals WHERE name=?1 AND generation=?2 AND message_id=?3", params![lease.name,lease.generation,message_id])?;
                    }
                    Update::Approval {
                        message_id,
                        id,
                        request,
                    } => {
                        running(&tx, lease, *message_id)?;
                        tx.execute("INSERT INTO managed_approvals(name,generation,id,message_id,request) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(name,generation,id) DO NOTHING", params![lease.name,lease.generation,id,message_id,request.to_string()])?;
                    }
                    Update::ApprovalResolved { id } => {
                        tx.execute("DELETE FROM managed_approvals WHERE name=?1 AND generation=?2 AND id=?3 AND response IS NOT NULL", params![lease.name,lease.generation,id])?;
                    }
                }
                &lease.name
            }
            Command::Release { lease } => {
                // Idempotent after a successful release, but never release another owner.
                tx.execute("UPDATE managed_sessions SET lease_until=0 WHERE name=?1 AND generation=?2 AND owner=?3", params![lease.name,lease.generation,lease.owner])?;
                tx.execute("UPDATE managed_deliveries SET state='uncertain',error='Worker stopped during this turn' WHERE name=?1 AND generation=?2 AND state='running' AND EXISTS(SELECT 1 FROM managed_sessions WHERE name=?1 AND owner=?3)", params![lease.name,lease.generation,lease.owner])?;
                tx.execute("DELETE FROM managed_approvals WHERE name=?1 AND generation=?2 AND EXISTS(SELECT 1 FROM managed_sessions WHERE name=?1 AND owner=?3)", params![lease.name,lease.generation,lease.owner])?;
                &lease.name
            }
            Command::Respond {
                name,
                generation,
                id,
                response,
                ..
            } => {
                let encoded = response.to_string();
                let changed = tx.execute("UPDATE managed_approvals SET response=?4 WHERE name=?1 AND generation=?2 AND id=?3 AND (response IS NULL OR response=?4)", params![name,generation,id,encoded])?;
                if changed == 0 {
                    return Err(refuse(
                        "Approval is no longer pending or already has another decision",
                    ));
                }
                name
            }
            Command::Resolve {
                name,
                generation,
                message_id,
                state,
                ..
            } => {
                if !matches!(
                    state,
                    DeliveryState::Completed | DeliveryState::Failed | DeliveryState::Queued
                ) {
                    return Err(refuse("Resolve to completed, failed, or queued"));
                }
                let changed = tx.execute("UPDATE managed_deliveries SET state=?4,error=NULL,runtime_turn_id=CASE WHEN ?4='queued' THEN NULL ELSE runtime_turn_id END WHERE name=?1 AND generation=?2 AND message_id=?3 AND state IN ('uncertain','failed',?4)", params![name,generation,message_id,state_name(*state)])?;
                if changed == 0 {
                    return Err(refuse(
                        "Only failed or uncertain deliveries can be resolved",
                    ));
                }
                tx.execute("DELETE FROM managed_approvals WHERE name=?1 AND generation=?2 AND message_id=?3", params![name,generation,message_id])?;
                name
            }
        };
        let result = response(&tx, name)?;
        tx.commit()?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::{ReadInbox, Register, Send};

    fn register(store: &Store, name: &str, now: u64) {
        store
            .see(
                &Register {
                    name: name.into(),
                    machine: "test".into(),
                    reconnect: false,
                },
                now,
            )
            .unwrap();
    }

    fn open(store: &mut Store, owner: &str, now: u64) -> Lease {
        let response = store
            .session_command(
                &Command::Open {
                    name: "b".into(),
                    owner: owner.into(),
                    machine: "test".into(),
                    runtime: Runtime::Codex,
                    cwd: "/tmp".into(),
                    max_turns: 10,
                    include_channels: false,
                },
                now,
            )
            .unwrap();
        Lease {
            name: "b".into(),
            generation: response.session.generation,
            owner: owner.into(),
        }
    }

    fn send(store: &mut Store, to: &str, text: &str) -> u64 {
        store
            .send_receipted(
                &Send {
                    from: "a".into(),
                    to: to.into(),
                    body: text.into(),
                    key: None,
                },
                5,
                false,
                None,
            )
            .unwrap()
            .into_inner()
            .id
    }

    fn poll(store: &mut Store, lease: &Lease, now: u64) -> Response {
        store
            .session_command(
                &Command::Poll {
                    lease: lease.clone(),
                    claim: true,
                },
                now,
            )
            .unwrap()
    }

    #[test]
    fn reads_do_not_consume_scheduled_work_and_busy_sessions_claim_only_once() {
        let mut store = Store::in_memory().unwrap();
        register(&store, "a", 1);
        register(&store, "b", 1);
        send(&mut store, "b", "Before management");
        let lease = open(&mut store, "owner-a", 2);
        let first = send(&mut store, "b", "First");
        let second = send(&mut store, "b", "Second");
        store.read_inbox("b", 100, 6).unwrap();
        let response = poll(&mut store, &lease, 7);
        assert_eq!(response.delivery.unwrap().message.id, first);
        assert_eq!(response.session.turns, 1);
        let again = poll(&mut store, &lease, 8);
        assert_eq!(again.delivery.unwrap().message.id, first);
        assert_eq!(again.session.turns, 1);
        store
            .session_command(
                &Command::Update {
                    lease: lease.clone(),
                    update: Update::Finished {
                        message_id: first,
                        state: DeliveryState::Completed,
                        error: None,
                    },
                },
                9,
            )
            .unwrap();
        assert_eq!(
            poll(&mut store, &lease, 10).delivery.unwrap().message.id,
            second
        );
    }

    #[test]
    fn expired_running_work_becomes_uncertain_and_blocks_replay() {
        let mut store = Store::in_memory().unwrap();
        register(&store, "a", 1);
        register(&store, "b", 1);
        let old = open(&mut store, "old", 2);
        let first = send(&mut store, "b", "First");
        poll(&mut store, &old, 5);
        send(&mut store, "b", "Second");
        assert!(
            store
                .session_command(
                    &Command::Open {
                        name: "b".into(),
                        owner: "new".into(),
                        machine: "test".into(),
                        runtime: Runtime::Codex,
                        cwd: "/tmp".into(),
                        max_turns: 10,
                        include_channels: false
                    },
                    6
                )
                .is_err()
        );
        let fresh = open(&mut store, "new", LEASE_MS + 6);
        let response = poll(&mut store, &fresh, LEASE_MS + 7);
        let uncertain = response.delivery.unwrap();
        assert_eq!(uncertain.message.id, first);
        assert_eq!(uncertain.state, DeliveryState::Uncertain);
        assert_eq!(response.session.turns, 1);
        assert!(
            store
                .session_command(
                    &Command::Update {
                        lease: old,
                        update: Update::Finished {
                            message_id: first,
                            state: DeliveryState::Completed,
                            error: None
                        }
                    },
                    LEASE_MS + 8
                )
                .is_err()
        );
    }

    #[test]
    fn stopped_workers_discard_approvals_without_releasing_a_different_owner() {
        let mut store = Store::in_memory().unwrap();
        register(&store, "a", 1);
        register(&store, "b", 1);
        let old = open(&mut store, "old", 2);
        let id = send(&mut store, "b", "Needs approval");
        poll(&mut store, &old, 3);
        store
            .session_command(
                &Command::Update {
                    lease: old.clone(),
                    update: Update::Approval {
                        message_id: id,
                        id: "request-1".into(),
                        request: serde_json::json!({"tool":"write"}),
                    },
                },
                4,
            )
            .unwrap();
        let released = store
            .session_command(&Command::Release { lease: old.clone() }, 5)
            .unwrap();
        assert!(released.session.approvals.is_empty());
        assert_eq!(released.delivery.unwrap().state, DeliveryState::Uncertain);
        let fresh = open(&mut store, "new", 6);
        let released_again = store
            .session_command(&Command::Release { lease: old }, 7)
            .unwrap();
        assert!(released_again.session.lease_until > 7);
        assert_eq!(
            poll(&mut store, &fresh, 8).delivery.unwrap().state,
            DeliveryState::Uncertain
        );
    }

    #[test]
    fn retirement_fences_old_identity_even_after_name_reuse() {
        let mut store = Store::in_memory().unwrap();
        register(&store, "a", 1);
        register(&store, "b", 1);
        let lease = open(&mut store, "owner", 2);
        let id = send(&mut store, "b", "Old work");
        poll(&mut store, &lease, 6);
        store.retire("b", 7).unwrap();
        register(&store, "b", 8);
        assert_ne!(
            store.generation("b").unwrap(),
            Some(lease.generation.clone())
        );
        assert!(
            store
                .session_command(
                    &Command::Poll {
                        lease: lease.clone(),
                        claim: true
                    },
                    9
                )
                .is_err()
        );
        let fresh = open(&mut store, "new", LEASE_MS + 9);
        assert!(poll(&mut store, &fresh, LEASE_MS + 10).delivery.is_none());
        assert!(
            store
                .read_message_deferred("b", id, 10, true)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn worker_checkpoint_is_fixed_and_updates_cannot_escape_its_delivery() {
        let mut store = Store::in_memory().unwrap();
        register(&store, "a", 1);
        register(&store, "b", 1);
        let lease = open(&mut store, "owner", 2);
        let id = send(&mut store, "b", "First");
        poll(&mut store, &lease, 6);
        let ready = |id: &str| Command::Update {
            lease: lease.clone(),
            update: Update::SessionReady {
                session_id: id.into(),
            },
        };
        store.session_command(&ready("conversation-1"), 7).unwrap();
        store.session_command(&ready("conversation-1"), 8).unwrap();
        assert!(store.session_command(&ready("another"), 9).is_err());
        assert!(
            store
                .session_command(
                    &Command::Update {
                        lease: lease.clone(),
                        update: Update::Started {
                            message_id: id + 1,
                            turn_id: "wrong".into()
                        }
                    },
                    10
                )
                .is_err()
        );
        store
            .session_command(
                &Command::Update {
                    lease: lease.clone(),
                    update: Update::Started {
                        message_id: id,
                        turn_id: "turn-1".into(),
                    },
                },
                11,
            )
            .unwrap();
        let mail = store
            .read_mail(
                &ReadInbox {
                    name: "b".into(),
                    max: 20,
                    channel: None,
                    sender: None,
                    skip: false,
                    digest: false,
                    defer_ack: true,
                    up_to: None,
                },
                12,
            )
            .unwrap();
        assert!(mail.messages.is_empty());
        assert_eq!(
            poll(&mut store, &lease, 12).delivery.unwrap().state,
            DeliveryState::Running
        );
    }

    #[test]
    fn ordinary_channel_mail_is_opt_in_but_human_channel_mail_is_scheduled() {
        let mut store = Store::in_memory().unwrap();
        register(&store, "a", 1);
        register(&store, "b", 1);
        let lease = open(&mut store, "owner", 2);
        let send = Send {
            from: "a".into(),
            to: "team".into(),
            body: "Channel".into(),
            key: None,
        };
        store
            .broadcast(&send, "team", &["b".into()], 3, false)
            .unwrap();
        assert!(poll(&mut store, &lease, 4).delivery.is_none());
        let human = store
            .broadcast(&send, "team", &["b".into()], 5, true)
            .unwrap();
        assert_eq!(
            poll(&mut store, &lease, 6).delivery.unwrap().message.id,
            human[0].id
        );
    }
}
