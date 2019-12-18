use crate::{
    database::{model::*, Database},
    spawn,
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
    mem,
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
    stopper_tx: Option<oneshot::Sender<()>>,
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
        Self {
            state,
            stopper_tx: Some(stopper_tx),
        }
    }

    fn total(&self) -> &AtomicU64 {
        &self.state.total
    }

    fn finished(&self) -> &AtomicU64 {
        &self.state.finished
    }

    fn stop(&mut self) {
        self.stopper_tx = None;
    }
}

struct Fetcher<'db> {
    db: &'db mut Database,
    cache_url: Arc<str>,
    progress: Progress,
    nars: HashMap<StorePathHash, NarState>,
    dep_graph: DepGraph<StorePathHash>,
    done_tx: Option<mpsc::Sender<QueueData>>,
    done_rx: mpsc::Receiver<QueueData>,
    todo: Vec<StorePathHash>,
    permits: usize,
    root_hashes: Vec<StorePathHash>,
}

#[derive(Debug)]
enum NarState {
    Fetching,
    Fetched(Nar),
    Inserted(i64),
}

impl NarState {
    fn as_inserted(&self) -> Option<i64> {
        match self {
            Self::Inserted(id) => Some(*id),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct QueueData(StorePathHash, Result<String>, mpsc::Sender<QueueData>);

impl<'db> Fetcher<'db> {
    const MAX_CONCURRENT_FETCH: usize = 128;

    fn new(
        db: &'db mut Database,
        cache_url: Arc<str>,
        root_hashes: Vec<StorePathHash>,
    ) -> Result<Self> {
        let (done_tx, done_rx) = mpsc::channel(Self::MAX_CONCURRENT_FETCH);
        Ok(Self {
            db,
            cache_url,
            progress: Progress::new(),
            nars: Default::default(),
            dep_graph: Default::default(),
            done_tx: Some(done_tx),
            done_rx,
            todo: vec![],
            permits: Self::MAX_CONCURRENT_FETCH,
            root_hashes,
        })
    }

    fn check_add_todo(&mut self, hash: StorePathHash) -> Result<()> {
        if self.nars.contains_key(&hash) {
            // Already visited.
            return Ok(());
        }
        self.dep_graph.add_node(hash);
        if let Some(id) = self.db.select_nar_id_by_hash(&hash)? {
            self.nars.insert(hash, NarState::Inserted(id));
            // Already in database.
            return Ok(());
        }
        self.nars.insert(hash, NarState::Fetching);
        self.progress.total().fetch_add(1, Ordering::Relaxed);
        self.todo.push(hash);
        Ok(())
    }

    fn spawn_fetchers(&mut self, done_tx: &mpsc::Sender<QueueData>) {
        while self.permits != 0 {
            let hash = match self.todo.pop() {
                None => return,
                Some(hash) => hash,
            };
            self.permits -= 1;

            let info_url = format!("{}/{}.narinfo", self.cache_url, hash);
            let done_tx = done_tx.clone();
            spawn(async move {
                let ret = get_all_to_string(&info_url).await;
                // Channel only fails when main future done with errors.
                // So just them ignore to suppress more errors.
                let _ = done_tx.clone().send(QueueData(hash, ret, done_tx)).await;
            });
        }
    }

    fn parse_one(&mut self, ret: Result<String>) -> Result<()> {
        let nar = Nar::parse_nar_info(&ret?)?;
        let cur_hash = nar.store_path.hash();
        for hash in nar.ref_hashes() {
            let hash = hash?;
            if hash != cur_hash {
                self.check_add_todo(hash)?;
                self.dep_graph.add_dep(cur_hash, hash);
            }
        }
        *self.nars.get_mut(&cur_hash).expect("Already inserted") = NarState::Fetched(nar);
        Ok(())
    }

    async fn fetch_all(&mut self) -> Result<u64> {
        let root_hashes = mem::replace(&mut self.root_hashes, vec![]);
        for &hash in &root_hashes {
            self.check_add_todo(hash)?;
        }
        self.root_hashes = root_hashes;

        let done_tx = self.done_tx.take().expect("Cannot fetch all twice");
        self.spawn_fetchers(&done_tx);
        // Ensure all `done_tx` is hold by sub-fetchers, so that
        // the main mpsc channel will be closed when all sub-fetchers finished.
        drop(done_tx);

        while let Some(QueueData(hash, ret, done_tx)) = self.done_rx.next().await {
            self.permits += 1;

            self.parse_one(ret)
                .with_context(|err| format_err!("Failed to get {}: {}", hash, err))?;
            self.progress.finished().fetch_add(1, Ordering::Relaxed);

            self.spawn_fetchers(&done_tx);
        }
        self.progress.stop();

        let total = self.progress.total().load(Ordering::Relaxed);
        let finished = self.progress.finished().load(Ordering::Relaxed);
        ensure!(total == finished, "Some error occurred",);
        Ok(total)
    }

    fn save_all(self) -> Result<Vec<i64>> {
        // Avoid over capturing `self`
        let mut nars = self.nars;
        for &hash in self.dep_graph.topo_sort().iter().rev() {
            match &nars[&hash] {
                NarState::Inserted(_) => {}
                NarState::Fetched(nar) => {
                    let self_ref = nar.ref_hashes().any(|h| h.unwrap() == hash);
                    let ref_ids = nar
                        .ref_hashes()
                        .map(|h| h.unwrap())
                        .filter(|h| h != &hash)
                        .map(|h| nars[&h].as_inserted().unwrap());
                    let id = self.db.insert_or_ignore_nar(
                        NarStatus::Pending,
                        &nar.store_path,
                        &nar.meta,
                        self_ref,
                        ref_ids,
                    )?;
                    *nars.get_mut(&hash).unwrap() = NarState::Inserted(id);
                }
                NarState::Fetching => unreachable!("Everything is fetched"),
            }
        }
        let ret = self
            .root_hashes
            .iter()
            .map(|hash| nars[hash].as_inserted().expect("Should be inserted"))
            .collect();
        Ok(ret)
    }
}

pub async fn fetch_meta_rec(
    db: &mut Database,
    cache_url: &str,
    root_paths: Vec<StorePath>,
) -> Result<Vec<i64>> {
    log::info!("Recursively fetching {} narinfo", root_paths.len());
    let root_hashes = root_paths.into_iter().map(|path| path.hash()).collect();
    let mut fetcher = Fetcher::new(db, cache_url.into(), root_hashes)?;
    let total = fetcher.fetch_all().await?;
    log::info!("Fetched {} paths, saving...", total);
    let ids = fetcher.save_all()?;
    log::info!("All paths saved");
    Ok(ids)
}

struct DepGraph<V> {
    edges: HashMap<V, Vec<V>>,
    inds: HashMap<V, usize>,
}

impl<V: Hash + Eq> Default for DepGraph<V> {
    fn default() -> Self {
        Self {
            edges: Default::default(),
            inds: Default::default(),
        }
    }
}

impl<V: Hash + Eq + Copy> DepGraph<V> {
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
            // Only top-level.
            assert_eq!(ids, vec![2, 3]);

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
