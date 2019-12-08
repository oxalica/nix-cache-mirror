use chrono::{DateTime, Utc};
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
pub struct GenerationExtraInfo {
    pub channel_url: Option<String>,
    pub total_paths: Option<i64>,
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
