#![cfg_attr(not(test), deny(clippy::unwrap_used))]
//! Execute database effects and save their replayable response in one transaction.
//!
//! The caller owns the connection, schema migrations, busy timeout and SQLite
//! durability settings. All effects must use the borrowed transaction: external
//! I/O, other connections and manual transaction-control SQL are outside this
//! guarantee. Callbacks must not commit, roll back or change connection policy.

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Serialize, de::DeserializeOwned};

#[derive(Debug)]
pub enum Error {
    Database(rusqlite::Error),
    InvalidNonce,
    InvalidFingerprint,
    InvalidTableName,
    IncompatibleSchema(String),
    Conflict,
    Encode(serde_json::Error),
    Decode(serde_json::Error),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(error) => error.fmt(f),
            Self::InvalidNonce => f.write_str("nonce must have 1 to 128 bytes"),
            Self::InvalidFingerprint => f.write_str("fingerprint must not be empty"),
            Self::InvalidTableName => {
                f.write_str("receipt table name must be nonempty and contain no NUL")
            }
            Self::IncompatibleSchema(table) => write!(f, "incompatible idempotency table: {table}"),
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
        if fingerprint.is_empty() {
            return Err(Error::InvalidFingerprint);
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

/// A library-owned receipt table. Choose a distinct table for each independent
/// idempotency namespace within one database. Identifiers are safely quoted.
#[derive(Debug, Clone)]
pub struct Ledger {
    table: String,
    quoted: String,
}
impl Default for Ledger {
    fn default() -> Self {
        Self {
            table: "receipts".into(),
            quoted: "main.\"receipts\"".into(),
        }
    }
}
impl Ledger {
    pub fn new(table: &str) -> Result<Self, Error> {
        if table.is_empty() || table.contains('\0') {
            return Err(Error::InvalidTableName);
        }
        Ok(Self {
            table: table.into(),
            quoted: format!("main.\"{}\"", table.replace('"', "\"\"")),
        })
    }
    /// Adopt only compatible tables, without rewriting any existing rows. Extra
    /// columns must be nullable or defaulted so older writers remain compatible.
    pub fn initialize(&self, connection: &Connection) -> Result<(), Error> {
        connection.execute_batch(&format!("CREATE TABLE IF NOT EXISTS {} (actor TEXT NOT NULL, nonce TEXT NOT NULL, fingerprint TEXT NOT NULL, response TEXT NOT NULL, PRIMARY KEY (actor, nonce));", self.quoted))?;
        let mut query = connection.prepare(
            "SELECT name,type,\"notnull\",pk,dflt_value FROM pragma_table_info(?1, 'main')",
        )?;
        let columns = query
            .query_map([&self.table], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, bool>(2)?,
                    row.get::<_, u32>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let expected = [
            ("actor", 1),
            ("nonce", 2),
            ("fingerprint", 0),
            ("response", 0),
        ];
        let compatible = expected.iter().all(|(name, pk)| {
            columns.iter().any(|(actual, kind, required, position, _)| {
                actual.eq_ignore_ascii_case(name)
                    && kind.eq_ignore_ascii_case("TEXT")
                    && *required
                    && position == pk
            })
        }) && columns.iter().all(|(name, _, required, pk, default)| {
            expected
                .iter()
                .any(|(known, _)| name.eq_ignore_ascii_case(known))
                || (*pk == 0 && (!required || default.is_some()))
        });
        if !compatible {
            return Err(Error::IncompatibleSchema(self.table.clone()));
        }
        Ok(())
    }
    /// Advisory lookup only. An absent row grants no permission to write;
    /// execute always rechecks inside the transaction's write reservation.
    pub fn lookup<T: DeserializeOwned>(
        &self,
        connection: &Connection,
        receipt: &Receipt,
    ) -> Result<Option<T>, Error> {
        let row: Option<(String, String)> = connection
            .query_row(
                &format!(
                    "SELECT fingerprint,response FROM {} WHERE actor=?1 AND nonce=?2",
                    self.quoted
                ),
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
}

/// Initialize the default `receipts` table, including inside a migration.
pub fn initialize(connection: &Connection) -> Result<(), Error> {
    Ledger::default().initialize(connection)
}
/// Advisory lookup using the default table; see Ledger::lookup.
pub fn lookup<T: DeserializeOwned>(
    connection: &Connection,
    receipt: &Receipt,
) -> Result<Option<T>, Error> {
    Ledger::default().lookup(connection, receipt)
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
    Ledger::default().execute(connection, receipt, operation)
}

impl Ledger {
    pub fn execute<T, E>(
        &self,
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
            && let Some(response) = self.lookup(&transaction, receipt).map_err(E::from)?
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
                    &format!(
                        "INSERT INTO {} (actor,nonce,fingerprint,response) VALUES (?1,?2,?3,?4)",
                        self.quoted
                    ),
                    params![receipt.actor, receipt.nonce, receipt.fingerprint, json],
                )
                .map_err(Error::from)
                .map_err(E::from)?;
        }
        transaction.commit().map_err(Error::from).map_err(E::from)?;
        Ok(Outcome::Fresh(response))
    }
}
