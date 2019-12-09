use failure::{ensure, Error};
use ifmt::iwrite;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{convert::TryFrom, fmt};

#[derive(Debug)]
pub struct Root {
    pub id: i64,
    pub meta: JsonValue,
    pub status: RootStatus,
}

/*
#[derive(Debug, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RootMeta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_paths: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_file_size: Option<i64>,
}
*/

#[derive(Debug, PartialEq, Eq)]
pub enum RootStatus {
    Pending,
    Downloading,
    Available,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Nar {
    pub store_path: StorePath,
    pub meta: NarMeta,
    pub status: NarStatus,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NarMeta {
    pub compression: String,
    pub cache_url: String,
    pub file_hash: String,
    pub file_size: u64,
    pub nar_hash: String,
    pub nar_size: u64,
    pub references: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deriver: Option<String>,
    pub sig: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum NarStatus {
    Pending,
    Available,
}

impl Nar {
    pub fn format_nar_info<'a>(&'a self, url: impl fmt::Display + 'a) -> impl fmt::Display + 'a {
        struct Fmt<'a, U>(&'a Nar, U);

        impl<'a, U: fmt::Display> fmt::Display for Fmt<'a, U> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let (nar, meta) = (&self.0, &self.0.meta);
                let url = &self.1;

                iwrite!(
                    f,
                    "StorePath: {nar.store_path}\n\
                     URL: {url}\n\
                     Compression: {meta.compression}\n\
                     FileHash: {meta.file_hash}\n\
                     FileSize: {meta.file_size}\n\
                     NarHash: {meta.nar_hash}\n\
                     NarSize: {meta.nar_size}\n\
                     References: {meta.references}\n\
                     Sig: {meta.sig}\n\
                     "
                )?;

                if let Some(deriver) = &meta.deriver {
                    write!(f, "Deriver: {}\n", deriver)?;
                }

                Ok(())
            }
        }

        Fmt(self, url)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct StorePath {
    path: String,
}

impl StorePath {
    const STORE_PREFIX: &'static str = "/nix/store/";
    pub const STORE_HASH_LEN: usize = 32;
    const SEP_POS: usize = Self::STORE_PREFIX.len() + Self::STORE_HASH_LEN;
    const MIN_LEN: usize = Self::SEP_POS + 1 + 1;
    const MAX_LEN: usize = 212;

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn hash_str(&self) -> &str {
        &self.path[Self::STORE_PREFIX.len()..Self::SEP_POS]
    }

    pub fn hash(&self) -> &[u8; Self::STORE_HASH_LEN] {
        use std::convert::TryInto;

        self.path[Self::STORE_PREFIX.len()..Self::SEP_POS]
            .as_bytes()
            .try_into()
            .unwrap()
    }

    pub fn name(&self) -> &str {
        &self.path[Self::SEP_POS + 1..]
    }
}

impl TryFrom<String> for StorePath {
    type Error = Error;

    // https://github.com/NixOS/nix/blob/abb8ef619ba2fab3ae16fb5b5430215905bac723/src/libstore/store-api.cc#L85
    fn try_from(path: String) -> Result<Self, Self::Error> {
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
            Self::MIN_LEN <= path.len() && path.len() <= Self::MAX_LEN,
            "Length {} is not in range [{}, {}]",
            path.len(),
            Self::MIN_LEN,
            Self::MAX_LEN,
        );
        ensure!(path.is_ascii(), "Not ascii string: {}", path);
        ensure!(
            path.as_bytes()[Self::SEP_POS] == b'-',
            "Hash seperator `-` not found",
        );

        let hash = &path[Self::STORE_PREFIX.len()..Self::SEP_POS];
        let name = &path[Self::SEP_POS + 1..];
        ensure!(is_valid_hash(hash.as_bytes()), "Invalid hash '{}'", hash);
        ensure!(is_valid_name(name.as_bytes()), "Invalid name '{}'", name);

        // Already checked
        Ok(Self {
            path: path.to_owned(),
        })
    }
}

impl fmt::Display for StorePath {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self.path(), f)
    }
}
