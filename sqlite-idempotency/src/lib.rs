#![cfg_attr(not(test), deny(clippy::unwrap_used))]
//! Execute database effects and save their replayable response in one transaction.
//!
//! The caller owns the connection, schema migrations, busy timeout and SQLite
//! durability settings. All effects must use the borrowed transaction: external
//! I/O, other connections and manual transaction-control SQL are outside this
//! guarantee. Callbacks must not commit, roll back or change connection policy.

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Serialize, de::DeserializeOwned};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS receipts (
    actor TEXT NOT NULL, nonce TEXT NOT NULL, fingerprint TEXT NOT NULL, response TEXT NOT NULL,
    PRIMARY KEY (actor, nonce)
);";

#[derive(Debug)]
pub enum Error {
    Database(rusqlite::Error),
    InvalidNonce,
    Conflict,
    Encode(serde_json::Error),
    Decode(serde_json::Error),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(error) => error.fmt(f),
            Self::InvalidNonce => f.write_str("nonce must have 1 to 128 bytes"),
            Self::Conflict => f.write_str("nonce was already used for another request"),
            Self::Encode(error) => write!(f, "cannot encode idempotency data: {error}"),
            Self::Decode(error) => write!(f, "cannot decode idempotency response: {error}"),
        }
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            Self::Encode(error) | Self::Decode(error) => Some(error),
            _ => None,
        }
    }
}
impl From<rusqlite::Error> for Error {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database(error)
    }
}

/// A caller namespace, retry key and request identity. Keep response types and
/// fingerprint serialization stable for as long as these keys can be replayed.
#[derive(Debug, Clone)]
pub struct Receipt {
    actor: String,
    nonce: String,
    fingerprint: String,
}
impl Receipt {
    /// The fingerprint is an opaque identity supplied by the application. It
    /// must bind the operation and original request bytes using a stable rule.
    /// Keep that rule compatible across upgrades and rollback.
    pub fn new(actor: &str, nonce: &str, fingerprint: &str) -> Result<Self, Error> {
        if nonce.is_empty() || nonce.len() > 128 {
            return Err(Error::InvalidNonce);
        }
        Ok(Self {
            actor: actor.into(),
            nonce: nonce.into(),
            fingerprint: fingerprint.into(),
        })
    }
    pub fn actor(&self) -> &str {
        &self.actor
    }
    pub fn nonce(&self) -> &str {
        &self.nonce
    }
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }
}

/// Initialize the library-owned receipt table, also usable inside an application's
/// schema transaction. Existing compatible receipt rows are left untouched.
pub fn initialize(connection: &Connection) -> Result<(), Error> {
    connection.execute_batch(SCHEMA).map_err(Error::from)
}

/// Advisory lookup for returning an already-committed response before expensive
/// validation. A miss grants no right to execute: use `execute`, which rechecks
/// under a write reservation. This lookup never consumes or changes a receipt.
pub fn lookup<T: DeserializeOwned>(
    connection: &Connection,
    receipt: &Receipt,
) -> Result<Option<T>, Error> {
    let row: Option<(String, String)> = connection
        .query_row(
            "SELECT fingerprint,response FROM receipts WHERE actor=?1 AND nonce=?2",
            params![receipt.actor, receipt.nonce],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    match row {
        Some((fingerprint, response)) if fingerprint == receipt.fingerprint => {
            serde_json::from_str(&response)
                .map(Some)
                .map_err(Error::Decode)
        }
        Some(_) => Err(Error::Conflict),
        None => Ok(None),
    }
}

/// Whether a successful result was committed now or read from an old receipt.
/// Only Fresh should cause best-effort notifications outside the database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome<T> {
    Fresh(T),
    Replayed(T),
}
impl<T> Outcome<T> {
    pub fn into_inner(self) -> T {
        match self {
            Self::Fresh(value) | Self::Replayed(value) => value,
        }
    }
    pub fn is_fresh(&self) -> bool {
        matches!(self, Self::Fresh(_))
    }
}

/// Reserve the writer before looking up the key, preventing concurrent
/// connections from both observing a miss and running the operation. A matching
/// receipt returns its original response without calling `operation`.
///
/// Callback, encoding, receipt-insertion and commit failures roll back the
/// transaction. Custom application errors can implement `From<Error>`.
/// With no receipt, the operation remains transactional but is not deduplicated.
/// Never manually control the transaction or perform effects outside it.
///
/// ```compile_fail
/// let mut connection = rusqlite::Connection::open_in_memory().unwrap();
/// sqlite_idempotency::execute::<(), sqlite_idempotency::Error>(&mut connection, None, |transaction| {
///     transaction.commit()?; // The application only borrows this transaction.
///     Ok(())
/// });
/// ```
pub fn execute<T, E>(
    connection: &mut Connection,
    receipt: Option<&Receipt>,
    operation: impl FnOnce(&Transaction<'_>) -> Result<T, E>,
) -> Result<Outcome<T>, E>
where
    T: Serialize + DeserializeOwned,
    E: From<Error>,
{
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(Error::from)
        .map_err(E::from)?;
    if let Some(receipt) = receipt
        && let Some(response) = lookup(&transaction, receipt).map_err(E::from)?
    {
        transaction.commit().map_err(Error::from).map_err(E::from)?;
        return Ok(Outcome::Replayed(response));
    }
    let response = operation(&transaction)?;
    if let Some(receipt) = receipt {
        let json = serde_json::to_string(&response)
            .map_err(Error::Encode)
            .map_err(E::from)?;
        transaction
            .execute(
                "INSERT INTO receipts (actor,nonce,fingerprint,response) VALUES (?1,?2,?3,?4)",
                params![receipt.actor, receipt.nonce, receipt.fingerprint, json],
            )
            .map_err(Error::from)
            .map_err(E::from)?;
    }
    transaction.commit().map_err(Error::from).map_err(E::from)?;
    Ok(Outcome::Fresh(response))
}
