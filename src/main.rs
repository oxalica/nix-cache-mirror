extern crate nix_cache_mirror;

use env_logger;
use futures::compat::Future01CompatExt as _;
use hyper::{self, service::service_fn, Server};
use nix_cache_mirror::{block_on, database::Database, server, update};
use std::{path::Path, sync::Arc};

fn main() {
    env_logger::init();

    add_channel();
    // add_raw_channel();
    // serve();
}

fn add_channel() {
    let mut db = Database::open("./data/unstable.sqlite").unwrap();
    block_on(async move {
        update::add_nix_channel_rec(&mut db, "https://nixos.org/channels/nixos-unstable", None)
            .await
            .unwrap();
    });
}

fn add_raw_channel() {
    use nix_cache_mirror::database::model::*;
    use std::convert::TryFrom;
    use update::NixChannelInfo;

    let mut db = Database::open("./data/raw.sqlite").unwrap();
    block_on(async move {
        let ids = update::add_root_rec(
            &mut db,
            NixChannelInfo {
                cache_url: "https://cache.nixos.org".to_owned(),
                root_paths: vec![StorePath::try_from(
                    "/nix/store/yhzvzdq82lzk0kvrp3i79yhjnhps6qpk-hello-2.10".to_owned(),
                )
                .unwrap()],
                meta: serde_json::json!({}),
            },
        )
        .await
        .unwrap();
        eprintln!("{:?}", ids);
    });
}

fn serve() {
    let listen_addr = ([127, 0, 0, 1], 3000).into();
    let db_path = Path::new("./data/simple.sqlite");
    let nar_file_dir = Path::new("./data/nar").to_path_buf();
    let want_mass_query = true;
    let priority = Some(40);

    let server_data = Arc::new({
        let db = Database::open(db_path).unwrap();
        log::info!("Initializing data");
        server::ServerData::init(&db, nar_file_dir, want_mass_query, priority).unwrap()
    });

    log::info!("Listening on http://{}", listen_addr);

    let server = Server::bind(&listen_addr).serve(move || {
        let server_data = server_data.clone();
        service_fn(move |req| server::serve(&server_data, req))
    });
    block_on(async { server.compat().await.unwrap() });
}
