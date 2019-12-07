use crate::database::Database;
use env_logger;
use futures01::future::Future as _;
use hyper::{self, service::service_fn, Server};
use std::{path::Path, sync::Arc};

mod database;
mod server;

fn main() {
    env_logger::init();

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

    hyper::rt::run(
        Server::bind(&listen_addr)
            .serve(move || {
                let server_data = server_data.clone();
                service_fn(move |req| server::serve(&server_data, req))
            })
            .map_err(|err| log::error!("Server error: {}", err)),
    );
}
