use failure::Fail;
use rusqlite::{self, named_params, types, Connection, TransactionBehavior, NO_PARAMS};
use serde_json;
use static_assertions::*;
use std::path::Path;

type Result<T> = std::result::Result<T, Error>;

pub mod model;
pub use self::model::*;

impl types::FromSql for GenerationStatus {
    fn column_result(value: types::ValueRef) -> types::FromSqlResult<Self> {
        let v: String = types::FromSql::column_result(value)?;
        Ok(match &*v {
            "canceled" => Self::Canceled,
            "pending" => Self::Pending,
            "indexing" => Self::Indexing,
            "downloading" => Self::Downloading,
            "finished" => Self::Finished,
            _ => unreachable!(),
        })
    }
}

impl types::FromSql for GenerationExtraInfo {
    fn column_result(value: types::ValueRef) -> types::FromSqlResult<Self> {
        let v: String = types::FromSql::column_result(value)?;
        Ok(serde_json::from_str(&v).map_err(|err| types::FromSqlError::Other(Box::new(err)))?)
    }
}

impl types::ToSql for GenerationExtraInfo {
    fn to_sql(&self) -> rusqlite::Result<types::ToSqlOutput<'_>> {
        let v = serde_json::to_string(self).unwrap();
        Ok(types::ToSqlOutput::Owned(types::Value::Text(v)))
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
}

impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Self {
        match err {
            rusqlite::Error::QueryReturnedNoRows => Self::NotFound,
            e => Self::SqliteError(e),
        }
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

    pub(crate) fn select_generations(&self) -> Result<Vec<Generation>> {
        let mut stmt = self.conn.prepare_cached(
            r"
            SELECT * FROM generation
                ORDER BY id DESC
            ",
        )?;
        let ret = stmt
            .query_map(NO_PARAMS, |row| {
                Ok(Generation {
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

    pub(crate) fn select_all_nar_info(&self, mut f: impl FnMut(NarInfo)) -> Result<()> {
        let mut stmt = self.conn.prepare_cached(
            r"
            SELECT hash, name, compression,
                    file_hash, file_size, nar_hash, nar_size,
                    (SELECT group_concat(referenced.hash || '-' || referenced.name, ' ')
                        FROM nar_reference AS rel
                        JOIN nar_info AS referenced ON (referenced.id = rel.reference_id)
                        WHERE rel.nar_id = nar_info.id
                    ) AS refs,
                    deriver, sig
                FROM nar_info
                WHERE available
            ",
        )?;

        stmt.query_and_then(NO_PARAMS, |row| {
            Ok(NarInfo {
                hash: row.get("hash")?,
                name: row.get("name")?,
                compression: row.get("compression")?,
                file_hash: row.get("file_hash")?,
                file_size: row.get::<_, i64>("file_size")? as u64,
                nar_hash: row.get("nar_hash")?,
                nar_size: row.get::<_, i64>("nar_size")? as u64,
                references: row.get("refs")?,
                deriver: row.get("deriver")?,
                sig: row.get("sig")?,
            })
        })?
        .map(|ret| ret.map(|info| f(info)))
        .collect::<Result<()>>()?;

        Ok(())
    }

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
}

// FIXME: More test
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_sql() {
        let _db = Database::open_in_memory().unwrap();
    }
}
