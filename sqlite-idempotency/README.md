# sqlite-idempotency

A synchronous Rust library for committing SQLite effects and their replayable
response together. It depends on `rusqlite` and Serde, with no hub, supervisor,
HTTP or application-schema dependency.

```rust
use sqlite_idempotency::{Error, Outcome, Receipt, execute, initialize};
let mut db = rusqlite::Connection::open_in_memory()?;
initialize(&db)?; // May instead run inside your schema migration transaction.
db.execute_batch("CREATE TABLE counters(value INTEGER NOT NULL)")?;
let receipt = Receipt::new("customer-42", "request-123", "server-computed-request-digest")?;
let result = execute(&mut db, Some(&receipt), |transaction| -> Result<_, Error> {
    transaction.execute("INSERT INTO counters VALUES (1)", [])?;
    Ok(1_i64)
})?;
match result {
    Outcome::Fresh(value) => { /* New commit: best-effort notifications may use value. */ }
    Outcome::Replayed(value) => { /* Return the stored result, without notifying again. */ }
}
Ok::<(), Box<dyn std::error::Error>>(())
```

The crate owns `BEGIN IMMEDIATE`, the receipt lookup, operation invocation,
response encoding, receipt insertion and commit. The operation only borrows the
transaction. Its writes must use that transaction; manual transaction-control
SQL, changing connection policy, other connections and external effects are
outside the contract. External notifications can still be lost after commit;
use an application-owned transactional outbox when those need durable delivery.

The library reserves the `receipts` table with columns `actor`, `nonce`,
`fingerprint`, `response` and primary key `(actor, nonce)`. Existing compatible
tables and rows are adopted without rewriting them. Initialize it during your
schema setup, including inside a larger migration transaction if needed.
The application owns its other tables, connection, SQLite durability settings
and busy timeout. Nesting `execute` inside an existing transaction is unsupported.

The actor is a namespace chosen by the application, not authentication. A nonce
is 1–128 UTF-8 bytes. The fingerprint is opaque: the application must bind it to
the operation and original request using a stable rule. Do not recompute it
from a newly decoded request when new defaulted fields can change its identity.
The hub retains its SHA-256 of path, a NUL separator and the original wire bytes.

For all writers using this protocol, the write reservation precedes the lookup.
Concurrent connections therefore cannot both observe a miss and run the same
keyed operation. A contending connection waits according to its busy timeout,
or returns a storage/busy error without running its callback. The caller may
retry the whole request with the same receipt. An advisory `lookup` miss grants
no permission to write; `execute` always checks again inside its transaction.

Only `Ok` callback results are remembered. `Err` rolls back and leaves the key
available for a later successful request. An application that models a business
refusal as `Ok` deliberately remembers that answer too. Custom business errors
can implement `From<sqlite_idempotency::Error>`. Encoding, receipt-insertion and
commit failures also roll back; corrupt or incompatible saved responses fail
closed without re-running the operation. With `None`, the operation is still
transactional but has no deduplication or receipt.

Receipts are kept forever; there is no pruning or expiration in this extraction.
Deleting a receipt permits the same key to execute again. Applications must
retain them for as long as retries or replay of old requests remain possible.
Saved JSON is durable application state: its response type and fingerprint rule
must be readable across supported upgrades and rollback. Introduce incompatible
response formats only through a compatible rollout and a distinct identity.

Tests cover replay, conflicts, actor isolation, custom business errors, rollback
on callback/encoding/receipt/commit failure, panic unwinding, corrupt responses,
writer contention, concurrent connections and subprocess crashes before commit
and after commit with a lost response. The hub additionally tests notification
suppression and HTTP error mapping. `scripts/check-idempotency-compatibility.py`
creates all four kinds of receipt with a real previous hub and replays them
through the current hub, then checks current receipts through the old hub after
rollback, including byte-for-byte HTTP responses and unchanged database effects.
