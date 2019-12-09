use crate::{
    database::{model::*, Database},
    spawn,
};
use chrono;
use failure::{ensure, format_err, Error, ResultExt as _};
use futures::{
    compat::{Future01CompatExt as _, Stream01CompatExt as _},
    prelude::*,
};
use lazy_static::lazy_static;
use log;
use reqwest::{
    r#async::{Client, ClientBuilder},
    Proxy,
};
use serde_json::{json, Value as JsonValue};
use std::{convert::TryFrom, env};
use tokio::timer;
use xz2;

mod semaphore;

type Result<T> = std::result::Result<T, Error>;

lazy_static! {
    static ref CLIENT: Client = {
        let mut b = ClientBuilder::new();
        if let Ok(proxy) = env::var("https_proxy").or(env::var("HTTPS_PROXY")) {
            b = b.proxy(Proxy::https(&proxy).expect("Invalid https proxy"));
        }
        b.build().expect("Cannot build reqwest client")
        // ClientBuilder::new()
        //     .use_sys_proxy()
        //     .build()
        //     .expect("Cannot build reqwest client")
    };
}

async fn get_all_to_vec(url: &str) -> Result<Vec<u8>> {
    let resp = CLIENT.get(url).send().compat().await?;
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
    pub cache_url: String,
    pub root_paths: Vec<StorePath>,
    pub meta: JsonValue,
}

pub async fn get_nix_channel(channel_url: &str, cache_url: Option<&str>) -> Result<NixChannelInfo> {
    use chrono::{SecondsFormat, Utc};

    let revision_url = format!("{}/git-revision", channel_url);
    let store_path_url = format!("{}/store-paths.xz", channel_url);
    let cache_url_url = format!("{}/binary-cache-url", channel_url);

    log::info!("Fetching metadata");
    let git_revision1 = get_git_revision(&revision_url)
        .await
        .context("Cannot get git revision")?;

    let fetch_time = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);

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
        meta: json!({
            "channel_url": channel_url,
            "cache_url": cache_url,
            "git_revision": git_revision1,
            "fetch_time": fetch_time,
        }),
        cache_url,
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
            Ok(StorePath::try_from(line.to_owned())
                .with_context(|err| format_err!("Invalid store path '{}': {}", line, err))?)
        })
        .collect()
}

pub async fn add_root_rec(db: &mut Database, channel_info: NixChannelInfo) -> Result<i64> {
    let root_ids = fetch_meta_rec(db, &channel_info.cache_url, channel_info.root_paths).await?;
    let cnt = root_ids.len();
    let mut meta = channel_info.meta;
    meta.as_object_mut()
        .unwrap()
        .insert("root_path_cnt".to_owned(), cnt.into());

    log::info!("Saving root with {} root paths", cnt);
    let id = db.insert_root(&meta, RootStatus::Pending, root_ids)?;
    log::info!("New root {} added", id);
    Ok(id)
}

pub async fn add_nix_channel_rec(
    db: &mut Database,
    channel_url: &str,
    cache_url: Option<&str>,
) -> Result<i64> {
    let chan_info = get_nix_channel(channel_url, cache_url).await?;
    add_root_rec(db, chan_info).await
}

async fn fetch_meta_rec(
    db: &mut Database,
    cache_url: &str,
    root_paths: Vec<StorePath>,
) -> Result<Vec<i64>> {
    use futures::channel::{mpsc, oneshot};
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    #[derive(Debug)]
    struct QueueData(Nar, mpsc::Sender<QueueData>);

    async fn fetcher(
        semaphore: Arc<semaphore::Semaphore>,
        cache_url: Arc<str>,
        hash: String,
        queue: mpsc::Sender<QueueData>,
    ) {
        let ret: Result<()> = async {
            let info_url = format!("{}/{}.narinfo", cache_url, hash);
            let guard = semaphore.acquire().await;
            let resp = get_all_to_string(&info_url).await?;
            drop(guard);

            let nar = Nar::parse_nar_info(&resp, NarStatus::Pending)
                .map_err(|err| format_err!("Invalid narinfo from {}: {}", info_url, err))?;
            // This only fails when main future done with errors.
            // So just ignore to suppress more errors.
            let _ = queue.clone().send(QueueData(nar, queue)).await;
            Ok(())
        }
            .await;
        if let Err(err) = ret {
            log::error!("Cannot get narinfo: {}", err);
        }
    }

    #[derive(Debug)]
    struct Progress {
        fetched: AtomicUsize,
        total: AtomicUsize,
    }

    const PROGRESS_TIMER_DELAY: Duration = Duration::from_secs(1);
    async fn progress_logger(progress: Arc<Progress>, mut end: oneshot::Receiver<()>) {
        // While timeout
        while timer::Timeout::new((&mut end).compat(), PROGRESS_TIMER_DELAY)
            .compat()
            .await
            .is_err()
        {
            log::info!(
                "Fetching meta [{}/{}]",
                progress.fetched.load(Ordering::Relaxed),
                progress.total.load(Ordering::Relaxed),
            );
        }
    }

    log::info!("Recursively fetching {} narinfo", root_paths.len());

    let progress = Arc::new(Progress {
        total: root_paths.len().into(),
        fetched: 0.into(),
    });
    let (end_tx, end_rx) = oneshot::channel();
    if log::log_enabled!(log::Level::Info) {
        spawn(progress_logger(progress.clone(), end_rx));
    }

    let cache_url: Arc<str> = cache_url.into();

    const FETCH_META_REC_CHANNEL_EXTRA_LEN: usize = 64;
    let chan_len = root_paths.len() + FETCH_META_REC_CHANNEL_EXTRA_LEN;
    let (tx, mut rx) = mpsc::channel(chan_len);

    const MAX_CONCURRENT_FETCH: usize = 128;
    let semaphore = Arc::new(semaphore::Semaphore::new(MAX_CONCURRENT_FETCH));

    // Some(nar) for fetched.
    // None      for fetching.
    let mut mp = std::collections::HashMap::new();

    for path in root_paths {
        spawn(fetcher(
            semaphore.clone(),
            cache_url.clone(),
            path.hash_str().to_owned(),
            tx.clone(),
        ));
        mp.insert(*path.hash(), None);
    }
    drop(tx);

    while let Some(QueueData(nar, tx)) = rx.next().await {
        if !nar.meta.references.is_empty() {
            let self_hash = nar.store_path.hash();
            for basename in nar.meta.references.split(' ') {
                let path = StorePath::try_from(format!("/nix/store/{}", basename)).with_context(
                    |err| {
                        format_err!(
                            "Invalid references from {}: {}",
                            nar.store_path.hash_str(),
                            err,
                        )
                    },
                )?;

                // New path
                if path.hash() != self_hash && mp.entry(*path.hash()).or_insert(None).is_none() {
                    progress.total.fetch_add(1, Ordering::Relaxed);
                    spawn(fetcher(
                        semaphore.clone(),
                        cache_url.clone(),
                        path.hash_str().to_owned(),
                        tx.clone(),
                    ));
                }
            }
        }
        drop(tx);

        progress.fetched.fetch_add(1, Ordering::Relaxed);
        mp.insert(*nar.store_path.hash(), Some(nar));
    }

    let _ = end_tx.send(());
    ensure!(mp.values().all(|nar| nar.is_some()), "Some error occurred");
    log::info!("{} paths fetched", progress.total.load(Ordering::Relaxed));

    let nar_ids = db.insert_or_ignore_nar(mp.into_iter().map(|(_, nar)| nar.unwrap()))?;
    Ok(nar_ids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_on;
    use insta::assert_debug_snapshot;

    #[test]
    fn test_parse_nix_path() {
        let p = |s: &str| StorePath::try_from(s.to_owned());

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
        assert_eq!(store_path.hash(), b"5yr2767rqnvwvsfy445ny41lk67fcjjh");
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

    #[test]
    #[ignore]
    fn test_fetch_meta_rec() {
        crate::tests::init_logger();
        block_on(async {
            let cache_url = "https://cache.nixos.org";
            let root_paths = vec![
                // hello -> [hello, glibc]
                StorePath::try_from(
                    "/nix/store/yhzvzdq82lzk0kvrp3i79yhjnhps6qpk-hello-2.10".to_owned(),
                )
                .unwrap(),
                // openssl.src -> []
                StorePath::try_from(
                    "/nix/store/fv8g2yczna9d78d150km0h73fkijw021-openssl-1.1.1d.tar.gz".to_owned(),
                )
                .unwrap(),
            ];

            let mut db = Database::open_in_memory().unwrap();
            let mut ids = fetch_meta_rec(&mut db, cache_url, root_paths)
                .await
                .unwrap();
            ids.sort();
            assert_eq!(ids, vec![1, 2, 3]);

            let mut nars = vec![];
            db.select_all_nar(|nar| nars.push(nar)).unwrap();
            nars.sort_by(|a, b| a.store_path.path().cmp(&b.store_path.path()));
            assert_debug_snapshot!(&nars, @r###"
            [
                Nar {
                    store_path: StorePath {
                        path: "/nix/store/fv8g2yczna9d78d150km0h73fkijw021-openssl-1.1.1d.tar.gz",
                    },
                    meta: NarMeta {
                        compression: "xz",
                        url: "nar/0zxydma1vh0gnncnkw3cxfpsl4y1rl5zsw0bprqyvb5zsklck6k5.nar.xz",
                        file_hash: "sha256:0zxydma1vh0gnncnkw3cxfpsl4y1rl5zsw0bprqyvb5zsklck6k5",
                        file_size: 8814400,
                        nar_hash: "sha256:0i6abchw6pa0p313ahhz0myrr2sbk1npxkkprbbw1qmz6javbc6x",
                        nar_size: 8845976,
                        references: "",
                        deriver: Some(
                            "0g93706i7g54hwilxkd9lhyfmmwy4jr6-openssl-1.1.1d.tar.gz.drv",
                        ),
                        sig: "cache.nixos.org-1:+t+LMZdteGZ6dasXA1yZqv61RpQBfp5C5gXSktiUun/A7REwDO1Zo/u388sTGF8Vg8GX2VdggWTG2WCi03ceCg==",
                        ca: None,
                    },
                    status: Pending,
                },
                Nar {
                    store_path: StorePath {
                        path: "/nix/store/xlxiw4rnxx2dksa91fizjzf7jb5nqghc-glibc-2.27",
                    },
                    meta: NarMeta {
                        compression: "xz",
                        url: "nar/0jh2rlji5ypmksarb1l48980dlizpw2i0a0cwa1wvmznclbf39r2.nar.xz",
                        file_hash: "sha256:0jh2rlji5ypmksarb1l48980dlizpw2i0a0cwa1wvmznclbf39r2",
                        file_size: 6350936,
                        nar_hash: "sha256:1sb4jk4d8p7j5lird5nf9h0l3l0wdiy4g7scbp6x8jjgfy9bb6ap",
                        nar_size: 28147008,
                        references: "xlxiw4rnxx2dksa91fizjzf7jb5nqghc-glibc-2.27",
                        deriver: Some(
                            "3v2v8gcgyrz0n1rkrm7qpr8x855fdc84-glibc-2.27.drv",
                        ),
                        sig: "cache.nixos.org-1:kpeoCBW1+6FDfUEGPZVgyNQ4/CvenOpLGa6MmJWAAKESZeti5VHFSSKjqQd2NeFyCIrBvO5D2SpGi2om/0brCg==",
                        ca: None,
                    },
                    status: Pending,
                },
                Nar {
                    store_path: StorePath {
                        path: "/nix/store/yhzvzdq82lzk0kvrp3i79yhjnhps6qpk-hello-2.10",
                    },
                    meta: NarMeta {
                        compression: "xz",
                        url: "nar/1xbx6mir1krb81rb6g2paz2mxgpjkxqc0v9i2pyl90zmjdxjv0ld.nar.xz",
                        file_hash: "sha256:1xbx6mir1krb81rb6g2paz2mxgpjkxqc0v9i2pyl90zmjdxjv0ld",
                        file_size: 41204,
                        nar_hash: "sha256:0v1pkm7xg0gp5avnd0qbnmmhcw97rwwwyfxf467imwcvvpyl54hz",
                        nar_size: 205920,
                        references: "xlxiw4rnxx2dksa91fizjzf7jb5nqghc-glibc-2.27 yhzvzdq82lzk0kvrp3i79yhjnhps6qpk-hello-2.10",
                        deriver: Some(
                            "dsjl0sbwpcrxfg85bq75y1j1hbwrxjy9-hello-2.10.drv",
                        ),
                        sig: "cache.nixos.org-1:ek9X+mtn4eOMwIfDIq4gyzO/pFOjOvTracg5+SPMAMcSRrNravyRPVyaOgmjy3vTXKC6AavAxfILAg7mpVnDDg==",
                        ca: None,
                    },
                    status: Pending,
                },
            ]
            "###);
        });
    }
}
