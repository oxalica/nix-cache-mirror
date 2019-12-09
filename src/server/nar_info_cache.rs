use crate::database::{Database, Error as DBError};
use std::{collections::HashMap, ops::Range};

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
        db.select_all_nar(|mut nar| {
            nar.meta.url = format!("nar/{}", nar.store_path.hash_str());

            let start = buf.len();
            write!(&mut buf, "{}", nar.format_nar_info()).unwrap();
            let end = buf.len();

            cache.insert(
                *nar.store_path.hash(),
                CacheItem {
                    info_range: start..end,
                    file_size: nar.meta.file_size,
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
