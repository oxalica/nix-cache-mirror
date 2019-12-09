use failure::Error;
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
    Indexed,
    Downloading,
    Available,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Nar {
    pub store_path: StorePath,
    pub meta: NarMeta,
    pub status: NarStatus,
}

// https://github.com/NixOS/nix/blob/e5320a87ce75bbd2dd88f57c3b470a396195e849/src/libstore/schema.sql
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NarMeta {
    pub compression: String,
    pub url: String,
    pub file_hash: String,
    pub file_size: u64,
    pub nar_hash: String,
    pub nar_size: u64,
    pub references: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deriver: Option<String>,
    pub sig: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum NarStatus {
    Pending,
    Available,
}

impl Nar {
    pub fn format_nar_info<'a>(&'a self) -> impl fmt::Display + 'a {
        struct Fmt<'a>(&'a Nar);

        impl fmt::Display for Fmt<'_> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let (nar, meta) = (&self.0, &self.0.meta);

                iwrite!(
                    f,
                    "StorePath: {nar.store_path}\n\
                     URL: {meta.url}\n\
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
                if let Some(ca) = &meta.ca {
                    write!(f, "CA: {}\n", ca)?;
                }

                Ok(())
            }
        }

        Fmt(self)
    }

    pub fn parse_nar_info(info: &str, status: NarStatus) -> Result<Self, &'static str> {
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
                    store_path =
                        Some(StorePath::try_from(v.to_owned()).map_err(|_| "Invalid StorePath")?);
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
                compression: compression.ok_or("Missing StorePath")?.to_owned(),
                url: url.ok_or("Missing URL")?.to_owned(),
                file_hash: file_hash.ok_or("Missing FileHash")?.to_owned(),
                file_size: file_size.ok_or("Missing FileSize")?,
                nar_hash: nar_hash.ok_or("Missing NarHash")?.to_owned(),
                nar_size: nar_size.ok_or("Missing NarSize")?,
                references: references.ok_or("Missing References")?.to_owned(),
                deriver: deriver.map(|s| s.to_owned()),
                sig: sig.ok_or("Missing Sig")?.to_owned(),
                ca: ca.map(|s| s.to_owned()),
            },
            status,
        })
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
                "/nix/store/yhzvzdq82lzk0kvrp3i79yhjnhps6qpk-hello-2.10".to_owned(),
            )
            .unwrap(),
            meta: NarMeta {
                url: "some/url".to_owned(),
                compression: "xz".to_owned(),
                file_hash: "file:hash".to_owned(),
                file_size: 123,
                nar_hash: "nar:hash".to_owned(),
                nar_size: 456,
                references: "ref1 ref2".to_owned(),
                deriver: Some("some.drv".to_owned()),
                sig: "s:i/g".to_owned(),
                ca: Some("fixed:hash".to_owned()),
            },
            status: NarStatus::Pending,
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
        Sig: s:i/g
        Deriver: some.drv
        CA: fixed:hash
        "###);

        nar.meta.references = String::new();
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
        Sig: s:i/g
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
Sig: s:i/g
Deriver: some.drv
CA: fixed:hash
"###;

        let nar = Nar {
            store_path: StorePath::try_from(
                "/nix/store/yhzvzdq82lzk0kvrp3i79yhjnhps6qpk-hello-2.10".to_owned(),
            )
            .unwrap(),
            meta: NarMeta {
                url: "some/url".to_owned(),
                compression: "xz".to_owned(),
                file_hash: "file:hash".to_owned(),
                file_size: 123,
                nar_hash: "nar:hash".to_owned(),
                nar_size: 456,
                references: "ref1 ref2".to_owned(),
                deriver: Some("some.drv".to_owned()),
                sig: "s:i/g".to_owned(),
                ca: Some("fixed:hash".to_owned()),
            },
            status: NarStatus::Pending,
        };

        assert_eq!(Nar::parse_nar_info(raw, NarStatus::Pending), Ok(nar));
    }
}
