extern crate nix_cache_mirror;

use env_logger;
use futures::compat::Future01CompatExt as _;
use hyper::{self, service::service_fn, Server};
use nix_cache_mirror::{block_on, database::Database, server, update};
use std::{path::Path, sync::Arc};

fn main() {
    env_logger::init();

    // add_channel();
    serve();
}

fn add_channel() {
    let mut db = Database::open("./data/db.sqlite").unwrap();
    block_on(async move {
        update::add_nix_channel_generation(
            &mut db,
            "https://nixos.org/channels/nixos-unstable",
            None,
        )
        .await
        .unwrap();
    });
}

fn serve() {
    let listen_addr = ([127, 0, 0, 1], 3000).into();
    let db_path = Path::new("./data/db.sqlite");
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
