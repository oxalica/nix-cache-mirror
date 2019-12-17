use crate::{
    database::{model::*, Database, Error as DBError},
    spawn,
};
use chrono::{DateTime, Utc};
use failure::{ensure, format_err, Error, ResultExt as _};
use futures::{
    compat::{Future01CompatExt as _, Stream01CompatExt as _},
    prelude::*,
};
use futures_intrusive::sync::Semaphore;
use lazy_static::lazy_static;
use log;
use reqwest::{
    r#async::{Client, ClientBuilder},
    Proxy,
};
use std::{convert::TryFrom, env};
use tokio::timer;
use xz2;

mod fetch_meta_rec;

type Result<T> = std::result::Result<T, Error>;

lazy_static! {
    static ref CLIENT: Client = {
        let mut b = ClientBuilder::new();
        if let Ok(proxy) = env::var("https_proxy").or(env::var("HTTPS_PROXY")) {
            b = b.proxy(Proxy::https(&proxy).expect("Invalid https_proxy"));
        }
        if let Ok(proxy) = env::var("http_proxy").or(env::var("HTTP_PROXY")) {
            b = b.proxy(Proxy::https(&proxy).expect("Invalid http_proxy"));
        }
        if let Ok(proxy) = env::var("all_proxy").or(env::var("ALL_PROXY")) {
            b = b.proxy(Proxy::all(&proxy).expect("Invalid all_proxy"));
        }
        b.build().expect("Cannot build reqwest client")
    };
}

async fn get_all_to_vec(url: &str) -> Result<Vec<u8>> {
    let resp = CLIENT.get(url).send().compat().await?.error_for_status()?;
    let mut stream = resp.into_body().compat();
    let mut buf: Vec<u8> = vec![];
    while let Some(chunk) = stream.next().await {
        buf.extend(chunk?);
    }
    Ok(buf)
}

async fn get_all_to_string(uri: &str) -> Result<String> {
    Ok(String::from_utf8(get_all_to_vec(uri).await?)?)
}

async fn get_git_revision(uri: &str) -> Result<String> {
    let mut rev = get_all_to_string(uri).await?;
    ensure!(
        rev.len() == 40 && rev.chars().all(|c| c.is_ascii_alphanumeric()),
        "Invalid git revision",
    );
    rev.make_ascii_lowercase();
    Ok(rev)
}

#[derive(Debug)]
pub struct NixChannelInfo {
    pub channel_url: String,
    pub cache_url: String,
    pub git_revision: String,
    pub fetch_time: DateTime<Utc>,
    pub root_paths: Vec<StorePath>,
}

pub async fn get_nix_channel(channel_url: &str, cache_url: Option<&str>) -> Result<NixChannelInfo> {
    let revision_url = format!("{}/git-revision", channel_url);
    let store_path_url = format!("{}/store-paths.xz", channel_url);
    let cache_url_url = format!("{}/binary-cache-url", channel_url);

    log::info!("Fetching metadata");
    let git_revision1 = get_git_revision(&revision_url)
        .await
        .context("Cannot get git revision")?;

    let fetch_time = Utc::now();

    let cache_url = match cache_url {
        Some(url) => url.to_owned(),
        None => get_all_to_string(&cache_url_url)
            .await
            .context("Cannot get binary cache url")?,
    };

    log::info!("Fetching root store paths");
    let root_paths = get_store_paths(&store_path_url)
        .await
        .context("Cannot get root store paths")?;

    log::info!("Checking git revision");
    let git_revision2 = get_git_revision(&revision_url).await?;
    log::info!("rev = {}", git_revision1);

    ensure!(
        git_revision1 == git_revision2,
        "Revision mismatch, before = {}, after = {}",
        git_revision1,
        git_revision2,
    );

    Ok(NixChannelInfo {
        channel_url: channel_url.to_owned(),
        cache_url,
        git_revision: git_revision1,
        fetch_time: fetch_time,
        root_paths,
    })
}

async fn get_store_paths(url: &str) -> Result<Vec<StorePath>> {
    use std::io::{BufRead, BufReader, Cursor};
    use xz2::read::XzDecoder;

    let resp = get_all_to_vec(&url).await?;
    BufReader::new(XzDecoder::new(Cursor::new(resp)))
        .lines()
        .map(|line| -> Result<StorePath> {
            let line = line?;
            Ok(StorePath::try_from(&*line)
                .with_context(|err| format_err!("Invalid store path '{}': {}", line, err))?)
        })
        .collect()
}

pub async fn add_root_rec(
    db: &mut Database,
    root: &Root,
    cache_url: &str,
    root_paths: impl IntoIterator<Item = StorePath>,
) -> Result<i64> {
    let root_ids =
        fetch_meta_rec::fetch_meta_rec(db, cache_url, root_paths.into_iter().collect()).await?;
    log::info!("Saving root with {} root paths", root_ids.len());
    let id = db.insert_root(root, root_ids)?;
    log::info!("New root {} added", id);
    Ok(id)
}

pub async fn add_nix_channel_rec(
    db: &mut Database,
    channel_url: &str,
    cache_url: Option<&str>,
) -> Result<i64> {
    let info = get_nix_channel(channel_url, cache_url).await?;
    let root = Root {
        channel_url: Some(info.channel_url),
        cache_url: Some(info.cache_url),
        git_revision: Some(info.git_revision),
        fetch_time: Some(info.fetch_time),
        status: RootStatus::Pending,
    };
    add_root_rec(db, &root, root.cache_url.as_ref().unwrap(), info.root_paths).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_on;

    #[test]
    fn test_parse_nix_path() {
        let p = |s: &str| StorePath::try_from(s);

        assert!(p("").is_err());
        assert!(p("/nix/store💗️").is_err());
        assert!(p("/nix/store/").is_err());
        assert!(p("/nix/store/00000000000000000000000000000000").is_err());
        assert!(p("/nix/store/0000000000000000000000000000000💗️").is_err());
        assert!(p("/nix/store/00000000000000000000000000000000-").is_err());
        assert!(p("/nix/store/00000000000000000000000000000000-💗️").is_err());

        let s = "/nix/store/5yr2767rqnvwvsfy445ny41lk67fcjjh-VSCode_1.40.1_linux-x64.tar.gz";
        let store_path = p(s).unwrap();
        assert_eq!(store_path.path(), s);
        assert_eq!(
            std::borrow::Borrow::<[u8]>::borrow(&store_path.hash()),
            b"5yr2767rqnvwvsfy445ny41lk67fcjjh"
        );
        assert_eq!(store_path.hash_str(), "5yr2767rqnvwvsfy445ny41lk67fcjjh");
        assert_eq!(store_path.name(), "VSCode_1.40.1_linux-x64.tar.gz");
        assert_eq!(store_path.to_string(), s);
    }

    #[test]
    #[ignore]
    fn test_get_channel() {
        crate::tests::init_logger();
        block_on(async {
            let channel_url = "https://nixos.org/channels/nixos-unstable";
            let mut channel_info = get_nix_channel(channel_url, None).await.unwrap();
            assert!(channel_info.root_paths.len() > 1000);
            channel_info.root_paths = vec![];
            eprintln!("{:?} store paths", channel_info);
        });
    }
}
