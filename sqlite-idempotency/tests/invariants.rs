use rusqlite::{Connection, Transaction};
use serde::{Deserialize, Serialize};
use sqlite_idempotency::{Error, Ledger, Outcome, Receipt, execute, initialize, lookup};
use std::{
    path::PathBuf,
    sync::{
        Arc, Barrier,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

fn database(path: Option<&std::path::Path>) -> Connection {
    let db = match path {
        Some(path) => Connection::open(path).unwrap(),
        None => Connection::open_in_memory().unwrap(),
    };
    db.busy_timeout(Duration::from_secs(5)).unwrap();
    initialize(&db).unwrap();
    db.execute_batch("CREATE TABLE IF NOT EXISTS effects (value INTEGER NOT NULL)")
        .unwrap();
    db
}
fn key(actor: &str, nonce: &str, operation: &str, value: i64) -> Receipt {
    Receipt::new(actor, nonce, &format!("{operation}:{value}")).unwrap()
}

fn write(tx: &Transaction<'_>, value: i64) -> Result<i64, Error> {
    tx.execute("INSERT INTO effects VALUES (?1)", [value])?;
    Ok(value)
}
fn count(db: &Connection, table: &str) -> u64 {
    db.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
        row.get(0)
    })
    .unwrap()
}
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "sqlite-idempotency-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn db(&self) -> PathBuf {
        self.0.join("state.db")
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn committed_response_is_replayed_without_calling_the_operation() {
    let mut db = database(None);
    let receipt = key("a", "one", "write", 7);
    assert_eq!(
        execute(&mut db, Some(&receipt), |tx| write(tx, 7)).unwrap(),
        Outcome::Fresh(7)
    );
    assert_eq!(
        execute::<i64, Error>(&mut db, Some(&receipt), |_| panic!(
            "replay must not execute"
        ))
        .unwrap(),
        Outcome::Replayed(7)
    );
    assert_eq!((count(&db, "effects"), count(&db, "receipts")), (1, 1));
}

#[test]
fn key_scope_is_actor_and_nonce_but_identity_includes_operation_and_payload() {
    let mut db = database(None);
    let first = key("a", "same", "write", 7);
    execute(&mut db, Some(&first), |tx| write(tx, 7)).unwrap();
    for conflict in [key("a", "same", "other", 7), key("a", "same", "write", 8)] {
        assert!(matches!(
            execute(&mut db, Some(&conflict), |tx| write(tx, 99)),
            Err(Error::Conflict)
        ));
    }
    let other_actor = key("b", "same", "write", 8);
    execute(&mut db, Some(&other_actor), |tx| write(tx, 8)).unwrap();
    assert_eq!((count(&db, "effects"), count(&db, "receipts")), (2, 2));
}

#[test]
fn absent_receipts_keep_transactions_but_do_not_deduplicate() {
    let mut db = database(None);
    for _ in 0..2 {
        execute(&mut db, None, |tx| write(tx, 7)).unwrap();
    }
    assert_eq!((count(&db, "effects"), count(&db, "receipts")), (2, 0));
}

#[derive(Debug)]
enum Rejected {
    Storage(Error),
    Business,
}
impl From<Error> for Rejected {
    fn from(error: Error) -> Self {
        Self::Storage(error)
    }
}

#[test]
fn a_business_error_rolls_back_effects_and_leaves_the_key_available() {
    let mut db = database(None);
    let receipt = key("a", "rejected", "write", 7);
    let result = execute::<i64, Rejected>(&mut db, Some(&receipt), |tx| {
        write(tx, 7)?;
        Err(Rejected::Business)
    });
    assert!(matches!(result, Err(Rejected::Business)));
    assert_eq!((count(&db, "effects"), count(&db, "receipts")), (0, 0));
    assert_eq!(
        execute(&mut db, Some(&receipt), |tx| write(tx, 7))
            .unwrap()
            .into_inner(),
        7
    );
    // Custom error wrappers retain the underlying storage error too.
    let bad = key("a", "rejected", "different", 7);
    assert!(matches!(
        execute::<i64, Rejected>(&mut db, Some(&bad), |_| Ok(8)),
        Err(Rejected::Storage(Error::Conflict))
    ));
}

#[derive(Deserialize)]
struct Unencodable;
impl Serialize for Unencodable {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("injected encoding failure"))
    }
}

#[test]
fn response_encoding_failure_rolls_back_the_business_effect() {
    let mut db = database(None);
    let receipt = key("a", "encoding", "write", 7);
    let result = execute::<Unencodable, Error>(&mut db, Some(&receipt), |tx| {
        write(tx, 7)?;
        Ok(Unencodable)
    });
    assert!(matches!(result, Err(Error::Encode(_))));
    assert_eq!((count(&db, "effects"), count(&db, "receipts")), (0, 0));
    execute(&mut db, Some(&receipt), |tx| write(tx, 7)).unwrap();
}

#[test]
fn receipt_insertion_failure_rolls_back_the_business_effect() {
    let mut db = database(None);
    let receipt = key("a", "receipt-fails", "write", 7);
    db.execute_batch("CREATE TRIGGER fail_receipt BEFORE INSERT ON receipts BEGIN SELECT RAISE(ABORT, 'receipt failure'); END;").unwrap();
    assert!(matches!(
        execute(&mut db, Some(&receipt), |tx| write(tx, 7)),
        Err(Error::Database(_))
    ));
    assert_eq!((count(&db, "effects"), count(&db, "receipts")), (0, 0));
    db.execute_batch("DROP TRIGGER fail_receipt").unwrap();
    execute(&mut db, Some(&receipt), |tx| write(tx, 7)).unwrap();
}

#[test]
fn commit_failure_rolls_back_both_effect_and_receipt() {
    let mut db = database(None);
    db.execute_batch("PRAGMA foreign_keys=ON; CREATE TABLE parents(id INTEGER PRIMARY KEY); CREATE TABLE children(parent INTEGER REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED);").unwrap();
    let receipt = key("a", "commit-fails", "write", 7);
    let result = execute(&mut db, Some(&receipt), |tx| -> Result<_, Error> {
        tx.execute("INSERT INTO children VALUES (99)", [])?;
        write(tx, 7)
    });
    assert!(matches!(result, Err(Error::Database(_))));
    assert!(db.is_autocommit());
    assert_eq!(
        (
            count(&db, "effects"),
            count(&db, "receipts"),
            count(&db, "children")
        ),
        (0, 0, 0)
    );
    execute(&mut db, Some(&receipt), |tx| write(tx, 7)).unwrap();
}

#[test]
fn unwinding_callback_rolls_back_and_connection_can_be_reused() {
    let mut db = database(None);
    let receipt = key("a", "panic", "write", 7);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = execute::<i64, Error>(&mut db, Some(&receipt), |tx| {
            write(tx, 7)?;
            panic!("injected panic")
        });
    }));
    assert!(result.is_err());
    assert!(db.is_autocommit());
    assert_eq!((count(&db, "effects"), count(&db, "receipts")), (0, 0));
    execute(&mut db, Some(&receipt), |tx| write(tx, 7)).unwrap();
}

#[test]
fn corrupt_or_incompatible_saved_responses_fail_closed_without_running_the_operation() {
    let mut db = database(None);
    let receipt = key("a", "corrupt", "write", 7);
    for response in ["not json", "{\"unexpected\":true}"] {
        db.execute(
            "INSERT OR REPLACE INTO receipts VALUES (?1,?2,?3,?4)",
            rusqlite::params![
                receipt.actor(),
                receipt.nonce(),
                receipt.fingerprint(),
                response
            ],
        )
        .unwrap();
        assert!(matches!(
            execute::<i64, Error>(&mut db, Some(&receipt), |_| panic!("must fail closed")),
            Err(Error::Decode(_))
        ));
        assert_eq!(count(&db, "effects"), 0);
    }
}

#[test]
fn compatible_existing_receipt_rows_are_preserved_across_reopen() {
    let directory = Directory::new();
    let legacy_hash = "opaque-existing-identity";
    let receipt = Receipt::new("a", "legacy", legacy_hash).unwrap();
    {
        let db = Connection::open(directory.db()).unwrap();
        db.execute_batch("CREATE TABLE receipts(actor TEXT NOT NULL,nonce TEXT NOT NULL,fingerprint TEXT NOT NULL,response TEXT NOT NULL,PRIMARY KEY(actor,nonce));").unwrap();
        db.execute(
            "INSERT INTO receipts VALUES ('a','legacy',?1,'17')",
            [legacy_hash],
        )
        .unwrap();
    }
    let mut db = database(Some(&directory.db()));
    assert_eq!(
        execute::<i64, Error>(&mut db, Some(&receipt), |_| panic!(
            "legacy response must replay"
        ))
        .unwrap()
        .into_inner(),
        17
    );
    assert_eq!(count(&db, "effects"), 0);
    initialize(&db).unwrap();
    assert_eq!(lookup::<i64>(&db, &receipt).unwrap(), Some(17));
}

#[test]
fn advisory_miss_is_rechecked_in_the_transaction() {
    let directory = Directory::new();
    let mut first = database(Some(&directory.db()));
    let mut second = database(Some(&directory.db()));
    let receipt = key("a", "race", "write", 7);
    assert_eq!(lookup::<i64>(&first, &receipt).unwrap(), None);
    execute(&mut second, Some(&receipt), |tx| write(tx, 7)).unwrap();
    assert_eq!(
        execute::<i64, Error>(&mut first, Some(&receipt), |_| panic!("miss was stale"))
            .unwrap()
            .into_inner(),
        7
    );
    assert_eq!(count(&first, "effects"), 1);
}

#[test]
fn concurrent_connections_execute_one_effect_and_return_one_response() {
    let directory = Directory::new();
    drop(database(Some(&directory.db())));
    let start = Arc::new(Barrier::new(3));
    let calls = Arc::new(AtomicUsize::new(0));
    let connections = [
        database(Some(&directory.db())),
        database(Some(&directory.db())),
    ];
    let workers: Vec<_> = connections
        .into_iter()
        .map(|mut db| {
            let start = start.clone();
            let calls = calls.clone();
            std::thread::spawn(move || {
                let receipt = key("a", "concurrent", "write", 7);
                start.wait();
                execute(&mut db, Some(&receipt), |tx| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    write(tx, 7)
                })
                .unwrap()
            })
        })
        .collect();
    start.wait();
    let outcomes: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(
        outcomes.iter().filter(|outcome| outcome.is_fresh()).count(),
        1
    );
    for outcome in outcomes {
        assert_eq!(outcome.into_inner(), 7);
    }
    let db = database(Some(&directory.db()));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!((count(&db, "effects"), count(&db, "receipts")), (1, 1));
}

#[test]
fn busy_writer_never_runs_the_contending_operation_and_retry_replays() {
    let directory = Directory::new();
    let mut first = database(Some(&directory.db()));
    let mut second = database(Some(&directory.db()));
    second.busy_timeout(Duration::ZERO).unwrap();
    let receipt = key("a", "busy", "write", 7);
    execute(&mut first, Some(&receipt), |tx| {
        let result = execute::<i64, Error>(&mut second, Some(&receipt), |_| panic!("writer is reserved"));
        assert!(matches!(result, Err(Error::Database(rusqlite::Error::SqliteFailure(error, _))) if error.code == rusqlite::ErrorCode::DatabaseBusy));
        write(tx, 7)
    }).unwrap();
    assert_eq!(
        execute::<i64, Error>(&mut second, Some(&receipt), |_| panic!("must replay"))
            .unwrap()
            .into_inner(),
        7
    );
    assert_eq!(count(&second, "effects"), 1);
}

#[test]
fn nonce_limits_are_bytes_and_explicit_fingerprints_support_legacy_adapters() {
    assert!(matches!(
        Receipt::new("a", "valid", ""),
        Err(Error::InvalidFingerprint)
    ));
    assert!(matches!(
        Receipt::new("a", "", "fingerprint"),
        Err(Error::InvalidNonce)
    ));
    assert!(Receipt::new("a", &"x".repeat(128), "fingerprint").is_ok());
    assert!(matches!(
        Receipt::new("a", &"x".repeat(129), "fingerprint"),
        Err(Error::InvalidNonce)
    ));
    assert!(Receipt::new("a", &"é".repeat(64), "fingerprint").is_ok());
    assert!(matches!(
        Receipt::new("a", &"é".repeat(65), "fingerprint"),
        Err(Error::InvalidNonce)
    ));
    let receipt = Receipt::new("a", "one", "legacy-identity").unwrap();
    assert_eq!(
        (receipt.actor(), receipt.nonce(), receipt.fingerprint()),
        ("a", "one", "legacy-identity")
    );
}

#[test]
fn incompatible_receipt_tables_are_rejected_at_initialization() {
    for schema in [
        "CREATE TABLE receipts(id INTEGER,total INTEGER)",
        "CREATE TABLE receipts(actor TEXT NOT NULL,nonce TEXT NOT NULL,fingerprint TEXT NOT NULL,response TEXT NOT NULL)",
        "CREATE TABLE receipts(actor TEXT NOT NULL,nonce TEXT NOT NULL,fingerprint TEXT NOT NULL,response TEXT NOT NULL,PRIMARY KEY(nonce,actor))",
        "CREATE TABLE receipts(actor TEXT NOT NULL,nonce TEXT NOT NULL,fingerprint TEXT,response TEXT NOT NULL,PRIMARY KEY(actor,nonce))",
        "CREATE TABLE receipts(actor TEXT NOT NULL,nonce TEXT NOT NULL,fingerprint TEXT NOT NULL,response TEXT NOT NULL,extra TEXT NOT NULL,PRIMARY KEY(actor,nonce))",
    ] {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(schema).unwrap();
        assert!(
            matches!(initialize(&db), Err(Error::IncompatibleSchema(_))),
            "{schema}"
        );
        let stored: String = db
            .query_row(
                "SELECT sql FROM sqlite_master WHERE name='receipts'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored, schema, "rejecting a table must not rewrite it");
    }
}

#[test]
fn table_names_are_quoted_and_ledgers_have_independent_key_namespaces() {
    let mut db = database(None);
    let first = Ledger::new("requests").unwrap();
    let second = Ledger::new("receipt\"; DROP TABLE effects; --").unwrap();
    first.initialize(&db).unwrap();
    second.initialize(&db).unwrap();
    let receipt = key("a", "one", "write", 7);
    assert_eq!(
        first
            .execute(&mut db, Some(&receipt), |tx| write(tx, 7))
            .unwrap(),
        Outcome::Fresh(7)
    );
    assert_eq!(
        second
            .execute(&mut db, Some(&receipt), |tx| write(tx, 8))
            .unwrap(),
        Outcome::Fresh(8)
    );
    assert_eq!(
        first
            .execute::<i64, Error>(&mut db, Some(&receipt), |_| panic!("first ledger replays"))
            .unwrap(),
        Outcome::Replayed(7)
    );
    assert_eq!(
        second
            .execute::<i64, Error>(&mut db, Some(&receipt), |_| panic!("second ledger replays"))
            .unwrap(),
        Outcome::Replayed(8)
    );
    assert_eq!((count(&db, "effects"), count(&db, "receipts")), (2, 0));
    assert!(matches!(Ledger::new(""), Err(Error::InvalidTableName)));
    assert!(matches!(
        Ledger::new("invalid\0name"),
        Err(Error::InvalidTableName)
    ));
}

#[test]
fn temporary_tables_cannot_shadow_the_durable_ledger() {
    let directory = Directory::new();
    let mut first = database(Some(&directory.db()));
    first.execute_batch("CREATE TEMP TABLE receipts(actor TEXT NOT NULL,nonce TEXT NOT NULL,fingerprint TEXT NOT NULL,response TEXT NOT NULL,PRIMARY KEY(actor,nonce))").unwrap();
    initialize(&first).unwrap();
    let receipt = key("a", "one", "write", 7);
    execute(&mut first, Some(&receipt), |tx| write(tx, 7)).unwrap();
    assert_eq!(
        first
            .query_row("SELECT count(*) FROM temp.receipts", [], |row| row
                .get::<_, u64>(0))
            .unwrap(),
        0
    );
    let mut second = database(Some(&directory.db()));
    assert_eq!(
        execute::<i64, Error>(&mut second, Some(&receipt), |_| panic!(
            "receipt must be durable across connections"
        ))
        .unwrap(),
        Outcome::Replayed(7)
    );
    assert_eq!(count(&second, "effects"), 1);
}

#[test]
fn compatible_additive_columns_and_migration_rollback_preserve_the_contract() {
    let mut db = database(None);
    db.execute_batch("ALTER TABLE receipts ADD COLUMN recorded_at INTEGER; ALTER TABLE receipts ADD COLUMN producer TEXT NOT NULL DEFAULT 'unknown'").unwrap();
    initialize(&db).unwrap();
    let receipt = key("a", "one", "write", 7);
    execute(&mut db, Some(&receipt), |tx| write(tx, 7)).unwrap();
    assert_eq!(lookup::<i64>(&db, &receipt).unwrap(), Some(7));
    let added: (Option<i64>, String) = db
        .query_row("SELECT recorded_at,producer FROM receipts", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .unwrap();
    assert_eq!(added, (None, "unknown".into()));

    let mut fresh = Connection::open_in_memory().unwrap();
    let tx = fresh.transaction().unwrap();
    initialize(&tx).unwrap();
    tx.rollback().unwrap();
    let exists: u64 = fresh
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE name='receipts'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        exists, 0,
        "the application owns the surrounding migration transaction"
    );
}

#[test]
fn a_saved_null_response_is_a_replay_not_a_miss() {
    let mut db = database(None);
    let receipt = key("a", "null", "write", 7);
    let first = execute::<Option<i64>, Error>(&mut db, Some(&receipt), |tx| {
        write(tx, 7)?;
        Ok(None)
    })
    .unwrap();
    assert_eq!(first, Outcome::Fresh(None));
    assert_eq!(
        execute::<Option<i64>, Error>(&mut db, Some(&receipt), |_| panic!(
            "null is a stored result"
        ))
        .unwrap(),
        Outcome::Replayed(None)
    );
    assert_eq!(count(&db, "effects"), 1);
}

#[test]
#[ignore = "subprocess fixture invoked by crash_recovery_preserves_the_atomic_boundary"]
fn crash_fixture() {
    let path = std::env::var_os("IDEMPOTENCY_CRASH_DB").unwrap();
    let stage = std::env::var("IDEMPOTENCY_CRASH_STAGE").unwrap();
    let mut db = database(Some(std::path::Path::new(&path)));
    let receipt = key("a", "crash", "write", 7);
    execute(&mut db, Some(&receipt), |tx| {
        write(tx, 17)?;
        if stage == "before-commit" {
            std::process::exit(73);
        }
        Ok::<_, Error>(17_i64)
    })
    .unwrap();
    // The transaction committed but no response was delivered to the caller.
    std::process::exit(74);
}

#[test]
fn crash_recovery_preserves_the_atomic_boundary() {
    for (stage, exit, expected) in [("before-commit", 73, 42), ("after-commit", 74, 17)] {
        let directory = Directory::new();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash_fixture", "--ignored", "--nocapture"])
            .env("IDEMPOTENCY_CRASH_DB", directory.db())
            .env("IDEMPOTENCY_CRASH_STAGE", stage)
            .stdout(std::process::Stdio::null())
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(exit));
        let mut db = database(Some(&directory.db()));
        assert_eq!(
            count(&db, "effects"),
            if stage == "before-commit" { 0 } else { 1 }
        );
        assert_eq!(
            count(&db, "receipts"),
            if stage == "before-commit" { 0 } else { 1 }
        );
        let receipt = key("a", "crash", "write", 7);
        assert_eq!(
            execute(&mut db, Some(&receipt), |tx| write(tx, 42))
                .unwrap()
                .into_inner(),
            expected
        );
        assert_eq!((count(&db, "effects"), count(&db, "receipts")), (1, 1));
    }
}
