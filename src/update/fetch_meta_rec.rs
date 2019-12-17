use crate::{
    database::{model::*, Database},
    spawn,
    util::Semaphore,
};
use failure::{ensure, format_err, ResultExt as _};
use futures::{
    channel::{mpsc, oneshot},
    compat::Future01CompatExt as _,
    prelude::*,
};
use log;
use std::{
    collections::HashMap,
    hash::Hash,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::timer;

use super::{get_all_to_string, Result};

#[derive(Debug)]
struct Progress {
    state: Arc<ProgressState>,
    // Drop it to stop.
    stopper_tx: oneshot::Sender<()>,
}

#[derive(Debug)]
struct ProgressState {
    finished: AtomicU64,
    total: AtomicU64,
}

impl Progress {
    const PROGRESS_TIMER_DELAY: Duration = Duration::from_secs(1);

    async fn logger(state: Arc<ProgressState>, mut stopper_rx: oneshot::Receiver<()>) {
        // While timeout
        while timer::Timeout::new((&mut stopper_rx).compat(), Self::PROGRESS_TIMER_DELAY)
            .compat()
            .await
            .err()
            .map_or(false, |e| e.is_elapsed())
        {
            log::info!(
                "Fetching meta [{}/{}]",
                state.finished.load(Ordering::Relaxed),
                state.total.load(Ordering::Relaxed),
            );
        }
    }

    fn new() -> Self {
        let state = Arc::new(ProgressState {
            finished: 0.into(),
            total: 0.into(),
        });
        let (stopper_tx, stopper_rx) = oneshot::channel();
        if log::log_enabled!(log::Level::Info) {
            spawn(Self::logger(state.clone(), stopper_rx));
        }
        Self { state, stopper_tx }
    }

    fn total(&self) -> &AtomicU64 {
        &self.state.total
    }

    fn finished(&self) -> &AtomicU64 {
        &self.state.finished
    }
}

pub async fn fetch_meta_rec(
    db: &mut Database,
    cache_url: &str,
    root_paths: Vec<StorePath>,
) -> Result<Vec<i64>> {
    log::info!("Recursively fetching {} narinfo", root_paths.len());

    let cache_url: Arc<str> = cache_url.into();
    let progress = Progress::new();

    const MAX_CONCURRENT_FETCH: usize = 128;
    let request_sem = Arc::new(Semaphore::new(MAX_CONCURRENT_FETCH));

    struct Data(StorePathHash, Result<String>, mpsc::Sender<Data>);
    const FETCH_META_REC_CHANNEL_EXTRA_LEN: usize = 64;
    let (tx, mut rx) = mpsc::channel(root_paths.len() + FETCH_META_REC_CHANNEL_EXTRA_LEN);

    // None:      Fetching or already in database
    // Some(nar): Fetched
    let mut nars: HashMap<StorePathHash, Option<Nar>> = Default::default();

    type Graph = DepGraph<StorePathHash>;
    let mut g: Graph = DepGraph::new();

    let check_fetch = |g: &mut Graph,
                       db: &mut Database,
                       nars: &mut HashMap<StorePathHash, Option<Nar>>,
                       hash: StorePathHash,
                       tx: mpsc::Sender<Data>|
     -> Result<()> {
        if nars.contains_key(&hash) {
            // Already visited.
            return Ok(());
        }
        g.add_node(hash);
        if db.select_nar_id_by_hash(&hash)?.is_some() {
            nars.insert(hash, None);
            // Already in database.
            return Ok(());
        }
        nars.insert(hash, None);
        progress.total().fetch_add(1, Ordering::Relaxed);

        let request_sem = request_sem.clone();
        let tx = tx.clone();
        let info_url = format!("{}/{}.narinfo", cache_url, hash);
        spawn(async move {
            let ret = {
                let _guard = request_sem.acquire().await;
                get_all_to_string(&info_url).await
            };
            // Channel only fails when main future done with errors.
            // So just them ignore to suppress more errors.
            let _ = tx.clone().send(Data(hash, ret, tx)).await;
        });
        Ok(())
    };

    for path in root_paths {
        check_fetch(&mut g, db, &mut nars, path.hash(), tx.clone())?;
    }

    // Ensure all `tx` is hold by sub-fetchers, so that
    // the main mpsc channel will be closed when all sub-fetchers finished.
    drop(tx);

    while let Some(Data(hash, ret, tx)) = rx.next().await {
        (|| -> Result<()> {
            let nar = Nar::parse_nar_info(&ret?)?;
            let cur_hash = nar.store_path.hash();
            for path in nar.ref_paths() {
                let hash = path?.hash();
                if hash != cur_hash {
                    check_fetch(&mut g, db, &mut nars, hash, tx.clone())?;
                    g.add_dep(cur_hash, hash);
                }
            }
            *nars.get_mut(&cur_hash).expect("Already inserted") = Some(nar);
            Ok(())
        })()
        .with_context(|err| format_err!("Failed to get {}: {}", hash, err))?;
        progress.finished().fetch_add(1, Ordering::Relaxed);
    }

    let total = progress.total().load(Ordering::Relaxed);
    let finished = progress.finished().load(Ordering::Relaxed);
    ensure!(total == finished, "Some error occurred",);
    drop(progress);
    log::info!("Fetched {} paths", total);

    let mut ret = vec![];
    for cur_hash in g.topo_sort().iter().rev() {
        if let Some(nar) = nars.get_mut(cur_hash).expect("Must visited").take() {
            ret.push(db.insert_or_ignore_nar(&nar, NarStatus::Pending)?);
        }
    }
    Ok(ret)
}

struct DepGraph<V> {
    edges: HashMap<V, Vec<V>>,
    inds: HashMap<V, usize>,
}

impl<V: Hash + Eq + Copy> DepGraph<V> {
    fn new() -> Self {
        Self {
            edges: Default::default(),
            inds: Default::default(),
        }
    }

    fn add_node(&mut self, a: V) {
        assert!(
            self.edges.insert(a, Default::default()).is_none(),
            "Add duplicated nodes",
        );
        self.inds.insert(a, 0);
    }

    fn add_dep(&mut self, a: V, b: V) {
        self.edges.get_mut(&a).expect("No source vertex").push(b);
        *self.inds.get_mut(&b).expect("No dest vertex") += 1;
    }

    fn topo_sort(mut self) -> Vec<V> {
        let mut q = Vec::with_capacity(self.edges.len());
        for (k, &ind) in &self.inds {
            if ind == 0 {
                q.push(*k);
            }
        }

        let mut lpos = 0usize;
        while lpos != q.len() {
            let c = q[lpos];
            lpos += 1;
            for nxt in &self.edges[&c] {
                let p = self.inds.get_mut(nxt).unwrap();
                *p -= 1;
                if *p == 0 {
                    q.push(*nxt);
                }
            }
        }

        q
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_on;
    use insta::assert_debug_snapshot;
    use std::convert::TryFrom;

    #[test]
    #[ignore]
    fn test_fetch_meta_rec() {
        crate::tests::init_logger();
        block_on(async {
            let cache_url = "https://cache.nixos.org";
            let root_paths = vec![
                // hello -> [hello, glibc]
                StorePath::try_from("/nix/store/yhzvzdq82lzk0kvrp3i79yhjnhps6qpk-hello-2.10")
                    .unwrap(),
                // openssl.src -> []
                StorePath::try_from(
                    "/nix/store/fv8g2yczna9d78d150km0h73fkijw021-openssl-1.1.1d.tar.gz",
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
            db.select_all_nar(NarStatus::Pending, |_, nar| nars.push(nar))
                .unwrap();
            nars.sort_by(|a, b| a.store_path.path().cmp(&b.store_path.path()));
            assert_debug_snapshot!(&nars, @r###"
            [
                Nar {
                    store_path: StorePath {
                        path: "/nix/store/fv8g2yczna9d78d150km0h73fkijw021-openssl-1.1.1d.tar.gz",
                    },
                    meta: NarMeta {
                        url: "nar/0zxydma1vh0gnncnkw3cxfpsl4y1rl5zsw0bprqyvb5zsklck6k5.nar.xz",
                        compression: Some(
                            "xz",
                        ),
                        file_hash: Some(
                            "sha256:0zxydma1vh0gnncnkw3cxfpsl4y1rl5zsw0bprqyvb5zsklck6k5",
                        ),
                        file_size: Some(
                            8814400,
                        ),
                        nar_hash: "sha256:0i6abchw6pa0p313ahhz0myrr2sbk1npxkkprbbw1qmz6javbc6x",
                        nar_size: 8845976,
                        deriver: Some(
                            "0g93706i7g54hwilxkd9lhyfmmwy4jr6-openssl-1.1.1d.tar.gz.drv",
                        ),
                        sig: Some(
                            "cache.nixos.org-1:+t+LMZdteGZ6dasXA1yZqv61RpQBfp5C5gXSktiUun/A7REwDO1Zo/u388sTGF8Vg8GX2VdggWTG2WCi03ceCg==",
                        ),
                        ca: None,
                    },
                    references: "",
                },
                Nar {
                    store_path: StorePath {
                        path: "/nix/store/xlxiw4rnxx2dksa91fizjzf7jb5nqghc-glibc-2.27",
                    },
                    meta: NarMeta {
                        url: "nar/0jh2rlji5ypmksarb1l48980dlizpw2i0a0cwa1wvmznclbf39r2.nar.xz",
                        compression: Some(
                            "xz",
                        ),
                        file_hash: Some(
                            "sha256:0jh2rlji5ypmksarb1l48980dlizpw2i0a0cwa1wvmznclbf39r2",
                        ),
                        file_size: Some(
                            6350936,
                        ),
                        nar_hash: "sha256:1sb4jk4d8p7j5lird5nf9h0l3l0wdiy4g7scbp6x8jjgfy9bb6ap",
                        nar_size: 28147008,
                        deriver: Some(
                            "3v2v8gcgyrz0n1rkrm7qpr8x855fdc84-glibc-2.27.drv",
                        ),
                        sig: Some(
                            "cache.nixos.org-1:kpeoCBW1+6FDfUEGPZVgyNQ4/CvenOpLGa6MmJWAAKESZeti5VHFSSKjqQd2NeFyCIrBvO5D2SpGi2om/0brCg==",
                        ),
                        ca: None,
                    },
                    references: "xlxiw4rnxx2dksa91fizjzf7jb5nqghc-glibc-2.27",
                },
                Nar {
                    store_path: StorePath {
                        path: "/nix/store/yhzvzdq82lzk0kvrp3i79yhjnhps6qpk-hello-2.10",
                    },
                    meta: NarMeta {
                        url: "nar/1xbx6mir1krb81rb6g2paz2mxgpjkxqc0v9i2pyl90zmjdxjv0ld.nar.xz",
                        compression: Some(
                            "xz",
                        ),
                        file_hash: Some(
                            "sha256:1xbx6mir1krb81rb6g2paz2mxgpjkxqc0v9i2pyl90zmjdxjv0ld",
                        ),
                        file_size: Some(
                            41204,
                        ),
                        nar_hash: "sha256:0v1pkm7xg0gp5avnd0qbnmmhcw97rwwwyfxf467imwcvvpyl54hz",
                        nar_size: 205920,
                        deriver: Some(
                            "dsjl0sbwpcrxfg85bq75y1j1hbwrxjy9-hello-2.10.drv",
                        ),
                        sig: Some(
                            "cache.nixos.org-1:ek9X+mtn4eOMwIfDIq4gyzO/pFOjOvTracg5+SPMAMcSRrNravyRPVyaOgmjy3vTXKC6AavAxfILAg7mpVnDDg==",
                        ),
                        ca: None,
                    },
                    references: "xlxiw4rnxx2dksa91fizjzf7jb5nqghc-glibc-2.27 yhzvzdq82lzk0kvrp3i79yhjnhps6qpk-hello-2.10",
                },
            ]
            "###);
        });
    }
}
