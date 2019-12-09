use failure::{format_err, Fail};
use rusqlite::{self, named_params, params, types, Connection, TransactionBehavior, NO_PARAMS};
use serde_json::{self, Value as JsonValue};
use static_assertions::*;
use std::{convert::TryInto, path::Path};

type Result<T> = std::result::Result<T, Error>;

pub mod model;
use self::model::*;

impl types::FromSql for RootStatus {
    fn column_result(value: types::ValueRef) -> types::FromSqlResult<Self> {
        let v: String = types::FromSql::column_result(value)?;
        Ok(match &*v {
            "pending" => Self::Pending,
            "indexed" => Self::Indexed,
            "downloading" => Self::Downloading,
            "available" => Self::Available,
            s => panic!("Unknown RootStatus '{}'", s),
        })
    }
}

impl types::ToSql for RootStatus {
    fn to_sql(&self) -> rusqlite::Result<types::ToSqlOutput<'_>> {
        match self {
            RootStatus::Pending => "pending",
            RootStatus::Indexed => "indexed",
            RootStatus::Downloading => "downloading",
            RootStatus::Available => "available",
        }
        .to_sql()
    }
}

impl types::FromSql for NarStatus {
    fn column_result(value: types::ValueRef) -> types::FromSqlResult<Self> {
        let v: String = types::FromSql::column_result(value)?;
        Ok(match &*v {
            "pending" => Self::Pending,
            "available" => Self::Available,
            s => panic!("Unknown NarStatus '{}'", s),
        })
    }
}

impl types::ToSql for NarStatus {
    fn to_sql(&self) -> rusqlite::Result<types::ToSqlOutput<'_>> {
        match self {
            NarStatus::Pending => "pending",
            NarStatus::Available => "available",
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
        Ok(self)
    }

    /*
    pub(crate) fn select_roots(&self) -> Result<Vec<Generation>> {
        let mut stmt = self.conn.prepare_cached(
            r"
            SELECT * FROM generation
                ORDER BY id DESC
            ",
        )?;
        let ret = stmt
            .query_map(NO_PARAMS, |row| {
                Ok(Root {
                    id: row.get("id")?,
                    start_time: row.get("start_time")?,
                    end_time: row.get("end_time")?,
                    cache_url: row.get("cache_url")?,
                    extra_info: row.get("extra_info")?,
                    status: row.get("status")?,
                })
            })?
            .map(|r| r.map_err(Into::into))
            .collect::<Result<Vec<_>>>()?;
        Ok(ret)
    }
    */

    pub(crate) fn insert_root(
        &mut self,
        meta: &JsonValue,
        status: RootStatus,
        root_nar_ids: impl IntoIterator<Item = i64>,
    ) -> Result<i64> {
        let txn = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        txn.execute_named(
            r"
            INSERT INTO root
                (meta, status)
                VALUES
                (:meta, :status)
            ",
            named_params! {
                ":meta": meta,
                ":status": status,
            },
        )?;
        let root_id = txn.last_insert_rowid();

        let mut stmt = txn.prepare_cached(
            r"
            INSERT INTO root_nar
                (root_id, nar_id)
                VALUES
                (:root_id, :nar_id)
            ",
        )?;

        for nar_id in root_nar_ids {
            stmt.execute_named(named_params! {
                ":root_id": root_id,
                ":nar_id": nar_id,
            })?;
        }

        drop(stmt);
        txn.commit()?;
        Ok(root_id)
    }

    pub(crate) fn insert_or_ignore_nar(
        &mut self,
        nars: impl IntoIterator<Item = Nar>,
    ) -> Result<Vec<i64>> {
        let txn = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;

        let mut stmt_insert = txn.prepare_cached(
            r"
            INSERT OR IGNORE INTO nar
                (store_path, meta, status)
                VALUES
                (:store_path, :meta, :status)
            ",
        )?;

        let mut stmt_select = txn.prepare_cached(
            r"
            SELECT id
                FROM nar
                WHERE store_path = ?
            ",
        )?;

        let mut ids = vec![];
        for nar in nars {
            let ret = stmt_insert.execute_named(named_params! {
                ":store_path": nar.store_path.path(),
                ":meta": serde_json::to_string(&nar.meta).unwrap(),
                ":status": nar.status,
            });
            let nar_id = match ret {
                Ok(0) => stmt_select.query_row(params![nar.store_path.path()], |row| row.get(0))?,
                Ok(_) => txn.last_insert_rowid(),
                Err(err) => return Err(err.into()),
            };
            ids.push(nar_id);
        }

        drop(stmt_select);
        drop(stmt_insert);
        txn.commit()?;
        Ok(ids)
    }

    pub(crate) fn select_all_nar(&self, mut f: impl FnMut(Nar)) -> Result<()> {
        let mut stmt = self.conn.prepare_cached(
            r"
            SELECT store_path, meta, status
                FROM nar
            ",
        )?;

        stmt.query_and_then(NO_PARAMS, |row| -> Result<_> {
            Ok(Nar {
                store_path: row
                    .get::<_, String>("store_path")?
                    .try_into()
                    .map_err(Error::ParseError)?,
                meta: serde_json::from_str(&row.get::<_, String>("meta")?)
                    .map_err(|err| Error::ParseError(format_err!("Invalid nar meta: {}", err)))?,
                status: row.get("status")?,
            })
        })?
        .map(|r| r.map(&mut f))
        .collect::<Result<()>>()?;

        Ok(())
    }

    /*
    pub(crate) fn insert_generation(
        &mut self,
        cache_url: &str,
        extra_info: &GenerationExtraInfo,
        roots: impl IntoIterator<Item = StorePath>,
    ) -> Result<i64> {
        let txn = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;

        txn.execute_named(
            r"
            INSERT INTO generation
                (cache_url, extra_info)
                VALUES
                (:cache_url, :extra_info)
            ",
            named_params! {
                ":cache_url": cache_url,
                ":extra_info": extra_info,
            },
        )?;
        let gen_id = txn.last_insert_rowid();

        let mut stmt = txn.prepare_cached(
            r"
            INSERT INTO generation_root
                (generation_id, hash, name)
                VALUES
                (:generation_id, :hash, :name)
            ",
        )?;

        for store_path in roots.into_iter() {
            stmt.execute_named(named_params! {
                ":generation_id": gen_id,
                ":hash": store_path.hash,
                ":name": store_path.name,
            })?;
        }

        drop(stmt);
        txn.commit()?;
        Ok(gen_id)
    }
    */
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
