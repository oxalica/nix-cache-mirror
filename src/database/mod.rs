use chrono::SecondsFormat;
use failure::Fail;
use rusqlite::{self, named_params, params, types, Connection, TransactionBehavior, NO_PARAMS};
use static_assertions::*;
use std::{convert::TryInto, path::Path};

type Result<T> = std::result::Result<T, Error>;

pub mod model;
use self::model::*;

impl types::FromSql for RootStatus {
    fn column_result(value: types::ValueRef) -> types::FromSqlResult<Self> {
        let v: String = types::FromSql::column_result(value)?;
        Ok(match &*v {
            "P" => Self::Pending,
            "D" => Self::Downloading,
            "A" => Self::Available,
            s => panic!("Unknown RootStatus '{}'", s),
        })
    }
}

impl types::ToSql for RootStatus {
    fn to_sql(&self) -> rusqlite::Result<types::ToSqlOutput<'_>> {
        match self {
            RootStatus::Pending => "P",
            RootStatus::Downloading => "D",
            RootStatus::Available => "A",
        }
        .to_sql()
    }
}

impl types::FromSql for NarStatus {
    fn column_result(value: types::ValueRef) -> types::FromSqlResult<Self> {
        let v: String = types::FromSql::column_result(value)?;
        Ok(match &*v {
            "P" => Self::Pending,
            "A" => Self::Available,
            "T" => Self::Trashed,
            s => panic!("Unknown NarStatus '{}'", s),
        })
    }
}

impl types::ToSql for NarStatus {
    fn to_sql(&self) -> rusqlite::Result<types::ToSqlOutput<'_>> {
        match self {
            NarStatus::Pending => "P",
            NarStatus::Available => "A",
            NarStatus::Trashed => "T",
        }
        .to_sql()
    }
}

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Sqlite error: {}", 0)]
    SqliteError(rusqlite::Error),
    #[fail(display = "Invalid database: {}", 0)]
    InvalidDatabase(String),
    #[fail(display = "Row not found")]
    NotFound,
    #[fail(display = "Parse error: {}", 0)]
    ParseError(failure::Error),
}

impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Self {
        match err {
            rusqlite::Error::QueryReturnedNoRows => Self::NotFound,
            e => Self::SqliteError(e),
        }
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(err: std::num::TryFromIntError) -> Self {
        Self::ParseError(err.into())
    }
}

#[derive(Debug)]
pub struct Database {
    conn: Connection,
}

assert_not_impl_any!(Database: Sync);

impl Database {
    const APPLICATION_ID: i32 = 0x2237186b;
    const USER_VERSION: i32 = 1;
    const INIT_SQL: &'static str = include_str!("./init.sql");
    const RUN_SQL: &'static str = include_str!("./run.sql");

    pub fn open_in_memory() -> Result<Self> {
        Self {
            conn: Connection::open_in_memory()?,
        }
        .check_init()
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self {
            conn: Connection::open(path.as_ref())?,
        }
        .check_init()
    }

    fn query_version(&self) -> Result<(i32, i32)> {
        self.conn
            .query_row(
                r"SELECT * FROM main.pragma_application_id, main.pragma_user_version",
                NO_PARAMS,
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(Into::into)
    }

    fn check_init(self) -> Result<Self> {
        let (app_id, user_ver) = self.query_version()?;
        if (app_id, user_ver) == (0, 0) {
            self.conn.execute_batch(Self::INIT_SQL)?;
        }
        let (app_id, user_ver) = self.query_version()?;
        if (app_id, user_ver) != (Self::APPLICATION_ID, Self::USER_VERSION) {
            return Err(Error::InvalidDatabase(format!(
                "Invalid database, expect (app_id, user_ver): {:?}, found {:?}",
                (Self::APPLICATION_ID, Self::USER_VERSION),
                (app_id, user_ver),
            )));
        }
        self.conn.execute_batch(Self::RUN_SQL)?;
        Ok(self)
    }

    pub(crate) fn insert_root(
        &mut self,
        root: &Root,
        root_hashes: impl IntoIterator<Item = StorePathHash>,
    ) -> Result<i64> {
        let txn = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        txn.execute_named(
            r"
            INSERT INTO root
                (channel_url, cache_url, git_revision, fetch_time, status)
                VALUES
                (:channel_url, :cache_url, :git_revision, :fetch_time, :status)
            ",
            named_params! {
                ":channel_url": root.channel_url,
                ":cache_url": root.cache_url,
                ":git_revision": root.git_revision,
                ":fetch_time": root
                    .fetch_time
                    .as_ref()
                    .map(|t| t.to_rfc3339_opts(SecondsFormat::Secs, true)),
                ":status": root.status,
            },
        )?;
        let root_id = txn.last_insert_rowid();

        let mut stmt = txn.prepare_cached(
            r"
            INSERT INTO root_nar (root_id, nar_id)
            SELECT :root_id, id
                FROM nar
                WHERE hash = :hash
            ",
        )?;

        for hash in root_hashes {
            stmt.execute_named(named_params! {
                ":root_id": root_id,
                ":hash": hash.as_str(),
            })?;
        }

        drop(stmt);
        txn.commit()?;
        Ok(root_id)
    }

    /// References must be already present in database.
    pub(crate) fn insert_or_ignore_nars<N, I>(&mut self, status: NarStatus, nars: I) -> Result<()>
    where
        I: IntoIterator<Item = N>,
        N: std::borrow::Borrow<Nar>,
    {
        let txn = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;

        {
            let mut stmt_insert_nar = txn.prepare_cached(
                r"
                INSERT INTO nar
                    ( store_root, hash, name
                    , url, compression
                    , file_hash, file_size, nar_hash, nar_size
                    , deriver, sig, ca
                    , status )
                    VALUES
                    ( :store_root, :hash, :name
                    , :url, :compression
                    , :file_hash, :file_size, :nar_hash, :nar_size
                    , :deriver, :sig, :ca
                    , :status )
                    ON CONFLICT DO NOTHING
                ",
            )?;

            let mut stmt_insert_ref = txn.prepare_cached(
                r"
                INSERT INTO nar_ref (nar_id, ref_id)
                SELECT :nar_id, id
                    FROM nar
                    WHERE hash = :hash
                ",
            )?;

            for nar in nars {
                let nar = nar.borrow();
                let ret = stmt_insert_nar.execute_named(named_params! {
                    ":store_root": nar.store_path.root(),
                    ":hash": nar.store_path.hash_str(),
                    ":name": nar.store_path.name(),

                    ":url": nar.meta.url,
                    ":compression": nar.meta.compression,

                    ":file_hash": nar.meta.file_hash,
                    ":file_size": nar.meta.file_size.map(|s| s as i64),
                    ":nar_hash": nar.meta.nar_hash,
                    ":nar_size": nar.meta.nar_size as i64,

                    ":deriver": nar.meta.deriver,
                    ":sig": nar.meta.sig,
                    ":ca": nar.meta.ca,

                    ":status": status,
                });

                match ret {
                    Ok(0) => {}
                    Ok(1) => {
                        let nar_id = txn.last_insert_rowid();
                        for hash in nar.ref_hashes() {
                            // Self reference works here.
                            stmt_insert_ref.execute_named(named_params! {
                                ":nar_id": nar_id,
                                ":hash": hash.expect("Invalid nar to insert").as_str(),
                            })?;
                        }
                    }
                    Ok(_) => unreachable!(),
                    Err(err) => return Err(err.into()),
                }
            }
        }

        txn.commit()?;
        Ok(())
    }

    pub(crate) fn select_nar_id_by_hash(&self, hash: &StorePathHash) -> Result<Option<i64>> {
        match self.conn.query_row_and_then(
            r"SELECT id FROM nar WHERE hash = ? AND status != 'T'",
            params![hash.as_str()],
            |row| Ok(row.get(0)?),
        ) {
            Ok(id) => Ok(Some(id)),
            Err(Error::NotFound) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub(crate) fn select_all_nar(
        &self,
        status: NarStatus,
        mut f: impl FnMut(i64, Nar),
    ) -> Result<()> {
        let mut stmt = self.conn.prepare_cached(
            r"
            SELECT  id, store_root, hash, name,
                    url, compression,
                    file_hash, file_size, nar_hash, nar_size,
                    deriver, sig, ca,
                    (SELECT COALESCE(GROUP_CONCAT(ref.hash || '-' || ref.name, ' '), '')
                        FROM nar_ref
                        JOIN nar AS ref ON ref.id = ref_id
                        WHERE nar_id = nar.id
                    ) AS refs
                FROM nar
                WHERE status = ?
            ",
        )?;

        stmt.query_and_then(params![status], |row| -> Result<_> {
            Ok((
                row.get("id")?,
                Nar {
                    store_path: format!(
                        "{}/{}-{}",
                        row.get::<_, String>("store_root")?,
                        row.get::<_, String>("hash")?,
                        row.get::<_, String>("name")?,
                    )
                    .try_into()
                    .map_err(Error::ParseError)?,
                    meta: NarMeta {
                        url: row.get("url")?,
                        compression: row.get("compression")?,
                        file_hash: row.get("file_hash")?,
                        file_size: row.get::<_, Option<i64>>("file_size")?.map(|s| s as u64),
                        nar_hash: row.get("nar_hash")?,
                        nar_size: row.get::<_, i64>("nar_size")? as u64,
                        deriver: row.get("deriver")?,
                        sig: row.get("sig")?,
                        ca: row.get("ca")?,
                    },
                    references: row.get("refs")?,
                },
            ))
        })?
        .map(|r| r.map(|(id, nar)| f(id, nar)))
        .collect::<Result<()>>()?;

        Ok(())
    }
}

// FIXME: More test
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile;

    #[test]
    fn test_init_sql() {
        let _ = Database::open_in_memory().unwrap();

        let file = tempfile::NamedTempFile::new().unwrap();
        // Initialize
        let _ = Database::open(file.path()).unwrap();
        // Reopen
        let _ = Database::open(file.path()).unwrap();
    }
}
