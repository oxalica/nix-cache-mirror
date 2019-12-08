use chrono::{DateTime, Utc};
use failure::{ensure, Error};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, PartialEq, Eq)]
pub struct Generation {
    pub id: i64,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,

    pub cache_url: String,
    pub extra_info: GenerationExtraInfo,

    pub status: GenerationStatus,
}

#[derive(Debug, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GenerationExtraInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_paths: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_file_size: Option<i64>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum GenerationStatus {
    Canceled,
    Pending,
    Indexing,
    Downloading,
    Finished,
}

#[derive(Debug)]
pub struct NarInfo {
    pub hash: String,
    pub name: String,

    pub compression: String,
    pub file_hash: String,
    pub file_size: u64,
    pub nar_hash: String,
    pub nar_size: u64,
    pub references: Option<String>,
    pub deriver: Option<String>,
    pub sig: String,
}

impl fmt::Display for NarInfo {
    #[rustfmt::skip]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StorePath: /nix/store/{}-{}\n", self.hash, self.name)?;
        write!(f, "URL: nar/{}\n", self.hash)?;
        write!(f, "Compression: {}\n", self.compression)?;
        write!(f, "FileHash: {}\n", self.file_hash)?;
        write!(f, "FileSize: {}\n", self.file_size)?;
        write!(f, "NarHash: {}\n", self.nar_hash)?;
        write!(f, "NarSize: {}\n", self.nar_size)?;
        // Always present but may be empty
        write!(f, "References: {}\n", self.references.as_ref().map(|s| &**s).unwrap_or(""))?;
        if let Some(deriver) = &self.deriver {
            write!(f, "Deriver: {}\n", deriver)?;
        }
        write!(f, "Sig: {}\n", self.sig)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct StorePath {
    pub hash: String,
    pub name: String,
}

// https://github.com/NixOS/nix/blob/abb8ef619ba2fab3ae16fb5b5430215905bac723/src/libstore/store-api.cc#L85
impl std::str::FromStr for StorePath {
    type Err = Error;

    fn from_str(path: &str) -> Result<Self, Self::Err> {
        const PREFIX: &[u8] = b"/nix/store/";
        const NIX_PATH_HASH_LEN: usize = 32;
        const SEP_POS: usize = PREFIX.len() + NIX_PATH_HASH_LEN;
        const MIN_LEN: usize = SEP_POS + 1 + 1;
        const MAX_LEN: usize = 212;

        fn is_valid_hash(s: &[u8]) -> bool {
            s.iter().all(|&b| match b {
                b'e' | b'o' | b'u' | b't' => false,
                b'a'..=b'z' | b'0'..=b'9' => true,
                _ => false,
            })
        }

        fn is_valid_name(s: &[u8]) -> bool {
            const VALID_CHARS: &[u8] = b"+-._?=";
            s.iter()
                .all(|&b| b.is_ascii_alphanumeric() || VALID_CHARS.contains(&b))
        }

        ensure!(
            MIN_LEN <= path.len() && path.len() <= MAX_LEN,
            "Length {} is not in range [{}, {}]",
            path.len(),
            MIN_LEN,
            MAX_LEN,
        );
        ensure!(path.is_ascii(), "Not ascii string: {}", path);
        ensure!(
            path.as_bytes()[SEP_POS] == b'-',
            "Hash seperator `-` not found",
        );

        let hash = &path[PREFIX.len()..SEP_POS];
        let name = &path[SEP_POS + 1..];
        ensure!(is_valid_hash(hash.as_bytes()), "Invalid hash '{}'", hash);
        ensure!(is_valid_name(name.as_bytes()), "Invalid name '{}'", name);

        // Already checked
        Ok(Self {
            hash: hash.to_owned(),
            name: name.to_owned(),
        })
    }
}
