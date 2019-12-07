use crate::database::{Database, Error as DBError};
use std::{collections::HashMap, convert::TryInto, ops::Range};

const NAR_HASH_LEN: usize = 32;

#[derive(Debug)]
pub struct NarInfoCache {
    buf: String,
    cache: HashMap<[u8; NAR_HASH_LEN], CacheItem>,
}

#[derive(Debug)]
struct CacheItem {
    info_range: Range<usize>,
    file_size: u64,
}

impl NarInfoCache {
    pub fn init(db: &Database) -> Result<Self, DBError> {
        use std::fmt::Write;

        let mut buf = String::new();
        let mut cache = HashMap::new();
        db.select_all_nar_info(|info| {
            let start = buf.len();
            write!(&mut buf, "{}", info).unwrap();
            cache.insert(
                info.hash.as_bytes().try_into().unwrap(),
                CacheItem {
                    info_range: start..buf.len(),
                    file_size: info.file_size,
                },
            );
        })?;

        Ok(Self { buf, cache })
    }

    pub fn get_info(&self, hash: &str) -> Option<&str> {
        if hash.len() != NAR_HASH_LEN {
            return None;
        }
        self.cache
            .get(hash.as_bytes())
            .map(|item| &self.buf[item.info_range.start..item.info_range.end])
    }

    pub fn get_file_size(&self, hash: &str) -> Option<u64> {
        if hash.len() != NAR_HASH_LEN {
            return None;
        }
        self.cache.get(hash.as_bytes()).map(|item| item.file_size)
    }
}
