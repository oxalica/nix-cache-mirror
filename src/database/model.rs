use chrono::{DateTime, Utc};
use failure::{format_err, Error};
use serde::{Deserialize, Serialize};
use std::{borrow::Borrow, convert::TryFrom, fmt};

#[derive(Debug, Default)]
pub struct Root {
    pub channel_url: Option<String>,
    pub cache_url: Option<String>,
    pub git_revision: Option<String>,
    pub fetch_time: Option<DateTime<Utc>>,
    pub status: RootStatus,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum RootStatus {
    Pending,
    Downloading,
    Available,
}

impl Default for RootStatus {
    fn default() -> Self {
        Self::Pending
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Nar {
    pub store_path: StorePath,
    pub meta: NarMeta,
    pub references: String,
}

// https://github.com/NixOS/nix/blob/61e816217bfdfffd39c130c7cd24f07e640098fc/src/libstore/schema.sql
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NarMeta {
    pub url: String,

    pub compression: Option<String>,
    pub file_hash: Option<String>,
    pub file_size: Option<u64>,
    pub nar_hash: String,
    pub nar_size: u64,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub deriver: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sig: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca: Option<String>,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum NarStatus {
    Pending,
    Available,
    Trashed,
}

impl Default for NarStatus {
    fn default() -> Self {
        Self::Pending
    }
}

impl Nar {
    pub fn ref_paths(&self) -> impl Iterator<Item = Result<StorePath, Error>> + '_ {
        // Yield nothing on empty string.
        self.references.split_terminator(" ").map(move |basename| {
            StorePath::try_from(format!("{}/{}", self.store_path.root(), basename))
        })
    }

    pub fn format_nar_info<'a>(&'a self) -> impl fmt::Display + 'a {
        struct Fmt<'a>(&'a Nar);

        impl fmt::Display for Fmt<'_> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let (nar, meta) = (&self.0, &self.0.meta);
                write!(f, "StorePath: {}\n", nar.store_path)?;
                write!(f, "URL: {}\n", meta.url)?;
                if let Some(comp) = &meta.compression {
                    write!(f, "Compression: {}\n", comp)?;
                }
                if let Some(hash) = &meta.file_hash {
                    write!(f, "FileHash: {}\n", hash)?;
                }
                if let Some(size) = &meta.file_size {
                    write!(f, "FileSize: {}\n", size)?;
                }
                write!(f, "NarHash: {}\n", meta.nar_hash)?;
                write!(f, "NarSize: {}\n", meta.nar_size)?;
                write!(f, "References: {}\n", nar.references)?;
                if let Some(sig) = &meta.sig {
                    write!(f, "Sig: {}\n", sig)?;
                }
                if let Some(deriver) = &meta.deriver {
                    write!(f, "Deriver: {}\n", deriver)?;
                }
                if let Some(ca) = &meta.ca {
                    write!(f, "CA: {}\n", ca)?;
                }
                Ok(())
            }
        }

        Fmt(self)
    }

    pub fn parse_nar_info(info: &str) -> Result<Self, Error> {
        Self::parse_nar_info_inner(info).map_err(|err| format_err!("Invalid narinfo: {}", err))
    }

    fn parse_nar_info_inner(info: &str) -> Result<Self, &'static str> {
        let (
            mut store_path,
            mut url,
            mut compression,
            mut file_hash,
            mut file_size,
            mut nar_hash,
            mut nar_size,
            mut references,
            mut deriver,
            mut sig,
            mut ca,
        ) = Default::default();

        for line in info.lines() {
            if line.is_empty() {
                continue;
            }

            let sep = line.find(": ").ok_or("Missing colon")?;
            let (k, v) = (&line[..sep], &line[sep + 2..]);
            match k {
                "StorePath" => {
                    store_path = Some(StorePath::try_from(v).map_err(|_| "Invalid StorePath")?);
                }
                "URL" => url = Some(v),
                "Compression" => compression = Some(v),
                "FileHash" => file_hash = Some(v),
                "FileSize" => file_size = Some(v.parse().map_err(|_| "Invalid FileSize")?),
                "NarHash" => nar_hash = Some(v),
                "NarSize" => nar_size = Some(v.parse().map_err(|_| "Invalid NarSize")?),
                "References" => references = Some(v),
                "Deriver" => deriver = Some(v),
                "Sig" => sig = Some(v),
                "CA" => ca = Some(v),
                _ => return Err("Unknown field"),
            }
        }

        Ok(Nar {
            store_path: store_path.ok_or("Missing StorePath")?,
            meta: NarMeta {
                compression: compression.map(|s| s.to_owned()),
                url: url.ok_or("Missing URL")?.to_owned(),
                file_hash: file_hash.map(|s| s.to_owned()),
                file_size: file_size,
                nar_hash: nar_hash.ok_or("Missing NarHash")?.to_owned(),
                nar_size: nar_size.ok_or("Missing NarSize")?,
                deriver: deriver.map(|s| s.to_owned()),
                sig: sig.map(|s| s.to_owned()),
                ca: ca.map(|s| s.to_owned()),
            },
            references: references.ok_or("Missing References")?.to_owned(),
        })
    }
}

#[derive(PartialEq, Eq, Hash, Clone, Copy)]
pub struct StorePathHash([u8; Self::LEN]);

impl StorePathHash {
    pub const LEN: usize = 32;

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).unwrap()
    }
}

impl fmt::Display for StorePathHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl fmt::Debug for StorePathHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("StorePathHash")
            .field(&self.to_string())
            .finish()
    }
}

impl Borrow<[u8]> for StorePathHash {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct StorePath {
    path: String,
}

// FIXME: Allow non-default store root.
impl StorePath {
    const STORE_PREFIX: &'static str = "/nix/store/";
    const SEP_POS: usize = Self::STORE_PREFIX.len() + StorePathHash::LEN;
    const MIN_LEN: usize = Self::SEP_POS + 1 + 1;
    const MAX_LEN: usize = 212;

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn root(&self) -> &str {
        &Self::STORE_PREFIX[..Self::STORE_PREFIX.len() - 1]
    }

    pub fn hash_str(&self) -> &str {
        &self.path[Self::STORE_PREFIX.len()..Self::SEP_POS]
    }

    pub fn hash(&self) -> StorePathHash {
        StorePathHash(
            <[u8; StorePathHash::LEN]>::try_from(
                self.path[Self::STORE_PREFIX.len()..Self::SEP_POS].as_bytes(),
            )
            .unwrap(),
        )
    }

    pub fn name(&self) -> &str {
        &self.path[Self::SEP_POS + 1..]
    }
}

impl TryFrom<String> for StorePath {
    type Error = Error;

    // https://github.com/NixOS/nix/blob/abb8ef619ba2fab3ae16fb5b5430215905bac723/src/libstore/store-api.cc#L85
    fn try_from(path: String) -> Result<Self, Self::Error> {
        use failure::ensure;

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

impl TryFrom<&'_ str> for StorePath {
    type Error = Error;

    fn try_from(path: &'_ str) -> Result<Self, Self::Error> {
        Self::try_from(path.to_owned())
    }
}

impl fmt::Display for StorePath {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self.path(), f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use insta::assert_snapshot;

    #[test]
    fn test_nar_info_format() {
        let mut nar = Nar {
            store_path: StorePath::try_from(
                "/nix/store/yhzvzdq82lzk0kvrp3i79yhjnhps6qpk-hello-2.10",
            )
            .unwrap(),
            meta: NarMeta {
                url: "some/url".to_owned(),
                compression: Some("xz".to_owned()),
                file_hash: Some("file:hash".to_owned()),
                file_size: Some(123),
                nar_hash: "nar:hash".to_owned(),
                nar_size: 456,
                deriver: Some("some.drv".to_owned()),
                sig: Some("s:i/g 2".to_owned()),
                ca: Some("fixed:hash".to_owned()),
            },
            references: "ref1 ref2".to_owned(),
        };

        assert_snapshot!(nar.format_nar_info().to_string(), @r###"
        StorePath: /nix/store/yhzvzdq82lzk0kvrp3i79yhjnhps6qpk-hello-2.10
        URL: some/url
        Compression: xz
        FileHash: file:hash
        FileSize: 123
        NarHash: nar:hash
        NarSize: 456
        References: ref1 ref2
        Sig: s:i/g 2
        Deriver: some.drv
        CA: fixed:hash
        "###);

        nar.references = String::new();
        nar.meta.deriver = None;
        nar.meta.ca = None;
        assert_snapshot!(nar.format_nar_info().to_string(), @r###"
        StorePath: /nix/store/yhzvzdq82lzk0kvrp3i79yhjnhps6qpk-hello-2.10
        URL: some/url
        Compression: xz
        FileHash: file:hash
        FileSize: 123
        NarHash: nar:hash
        NarSize: 456
        References: 
        Sig: s:i/g 2
        "###);
    }

    #[test]
    fn test_nar_info_parse() {
        let raw = r###"
StorePath: /nix/store/yhzvzdq82lzk0kvrp3i79yhjnhps6qpk-hello-2.10
URL: some/url
Compression: xz
FileHash: file:hash
FileSize: 123
NarHash: nar:hash
NarSize: 456
References: ref1 ref2
Sig: s:i/g 2
Deriver: some.drv
CA: fixed:hash
"###;

        let nar = Nar {
            store_path: StorePath::try_from(
                "/nix/store/yhzvzdq82lzk0kvrp3i79yhjnhps6qpk-hello-2.10",
            )
            .unwrap(),
            meta: NarMeta {
                url: "some/url".to_owned(),
                compression: Some("xz".to_owned()),
                file_hash: Some("file:hash".to_owned()),
                file_size: Some(123),
                nar_hash: "nar:hash".to_owned(),
                nar_size: 456,
                deriver: Some("some.drv".to_owned()),
                sig: Some("s:i/g 2".to_owned()),
                ca: Some("fixed:hash".to_owned()),
            },
            references: "ref1 ref2".to_owned(),
        };

        assert_eq!(Nar::parse_nar_info(raw).unwrap(), nar);
    }
}
