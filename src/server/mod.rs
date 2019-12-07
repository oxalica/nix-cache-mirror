use crate::database::Database;
use async_std;
use hyper::{
    body::{Body, Chunk},
    header, Method, StatusCode,
};
use log;
use std::{ops::Range, path::PathBuf};

mod nar_info_cache;
use self::nar_info_cache::NarInfoCache;

const SEND_FILE_BUFFER_LEN: usize = 64 << 20; // 64 KiB

type Request = hyper::Request<Body>;
type Response = hyper::Response<Body>;
type TryResponse = hyper::Result<Response>;

pub struct ServerData {
    nar_info_cache: NarInfoCache,
    nar_file_dir: PathBuf,
    nix_cache_info: String,
}

impl ServerData {
    pub fn init(
        db: &Database,
        nar_file_dir: PathBuf,
        want_mass_query: bool,
        priority: Option<i32>,
    ) -> Result<Self, crate::database::Error> {
        use std::fmt::Write;

        let mut nix_cache_info = "StoreDir: /nix/store\n".to_owned();
        if want_mass_query {
            write!(&mut nix_cache_info, "WantMassQuery: 1\n").unwrap();
        }
        if let Some(priority) = priority {
            write!(&mut nix_cache_info, "Priority: {}\n", priority).unwrap();
        }

        Ok(Self {
            nar_info_cache: NarInfoCache::init(db)?,
            nar_file_dir,
            nix_cache_info,
        })
    }
}

fn simple_response(status: StatusCode, body: &'static str) -> Response {
    let mut resp = Response::new(Body::from(body));
    *resp.status_mut() = status;
    resp
}

pub fn serve<'a>(data: &ServerData, req: Request) -> TryResponse {
    let method = req.method();
    match req.uri().path() {
        "/" => Ok(simple_response(StatusCode::OK, "It works")),

        "/nix-cache-info" => match method {
            &Method::GET => Ok(Response::new(Body::from(data.nix_cache_info.clone()))),
            _ => Ok(simple_response(StatusCode::METHOD_NOT_ALLOWED, "")),
        },

        s if s.starts_with("/nar/") => match method {
            &Method::GET | &Method::HEAD => {
                let hash = &s["/nar/".len()..];
                serve_nar_file(data, &req, hash, method == &Method::HEAD)
            }
            _ => Ok(simple_response(StatusCode::METHOD_NOT_ALLOWED, "")),
        },

        s if !s[1..].contains('/') && s.ends_with(".narinfo") => match method {
            &Method::GET => {
                let hash = &s[1..s.len() - ".narinfo".len()];
                serve_nar_info(data, &req, hash)
            }
            _ => Ok(simple_response(StatusCode::METHOD_NOT_ALLOWED, "")),
        },

        _ => Ok(simple_response(StatusCode::NOT_FOUND, "Not found")),
    }
}

fn serve_nar_info(data: &ServerData, _req: &Request, hash: &str) -> TryResponse {
    log::debug!("Get nar info: {}", hash);
    Ok(match data.nar_info_cache.get_info(hash) {
        Some(info) => {
            let mut resp = Response::new(Body::from(info.to_owned()));
            resp.headers_mut().insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("text/x-nix-narinfo"),
            );
            resp
        }
        None => simple_response(StatusCode::NOT_FOUND, "Not found"),
    })
}

fn parse_content_range(req: &Request, file_size: u64) -> Option<Range<u64>> {
    let s = req.headers().get(header::RANGE)?;
    let s = s.to_str().ok()?;
    if !s.starts_with("bytes=") {
        return None;
    }
    let s = &s["bytes=".len()..];

    let sep = s.find('-')?;
    let end = s[sep + 1..].find(',').unwrap_or(s.len());

    let lhs = s[..sep].parse::<u64>().ok()?.checked_sub(1)?;
    if sep + 1 == s.len() {
        if lhs < file_size {
            return Some(lhs..file_size);
        }
    } else {
        let rhs = s[sep + 1..end].parse::<u64>().ok()?;
        if lhs <= rhs && rhs < file_size {
            return Some(lhs..rhs);
        }
    }
    None
}

fn serve_nar_file(data: &ServerData, req: &Request, hash: &str, head_only: bool) -> TryResponse {
    use futures::future::TryFutureExt;

    log::debug!("Get nar file: {}", hash);
    let file_size = match data.nar_info_cache.get_file_size(hash) {
        Some(file_size) => file_size,
        None => return Ok(simple_response(StatusCode::NOT_FOUND, "Not found")),
    };

    let (tx, body) = Body::channel();
    let mut resp = Response::new(body);
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/x-nix-nar"),
    );
    resp.headers_mut().insert(
        header::ACCEPT_RANGES,
        header::HeaderValue::from_static("bytes"),
    );

    let range = match parse_content_range(req, file_size) {
        None => 0..file_size,
        Some(range) => {
            *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
            resp.headers_mut().insert(
                header::CONTENT_RANGE,
                header::HeaderValue::from_str(&format!(
                    "bytes {}-{}/{}",
                    range.start + 1,
                    range.end,
                    file_size,
                ))
                .unwrap(),
            );
            range
        }
    };

    resp.headers_mut().insert(
        header::CONTENT_LENGTH,
        header::HeaderValue::from(range.end - range.start),
    );

    let path = data.nar_file_dir.join(hash);
    if !head_only {
        hyper::rt::spawn(
            Box::pin(async move {
                send_file(path, tx, range).await;
                Ok(())
            })
            .compat(),
        );
    }
    Ok(resp)
}

async fn send_file(path: PathBuf, mut tx: hyper::body::Sender, range: Range<u64>) {
    use async_std::{fs::File, io::prelude::*, io::SeekFrom};
    use futures01::Async as Async01;
    use std::{
        future::Future,
        pin::Pin,
        task::{Context, Poll},
    };

    struct SenderReadyFuture<'a>(&'a mut hyper::body::Sender);

    impl Future for SenderReadyFuture<'_> {
        type Output = hyper::Result<()>;

        fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            match self.0.poll_ready() {
                Ok(Async01::Ready(())) => Poll::Ready(Ok(())),
                Ok(Async01::NotReady) => Poll::Pending,
                Err(err) => Poll::Ready(Err(err)),
            }
        }
    }

    let mut buf = vec![0u8; SEND_FILE_BUFFER_LEN];
    let mut file = match File::open(&path).await {
        Ok(file) => file,
        Err(err) => {
            log::error!("Failed to open file '{}': {}", path.display(), err);
            tx.abort();
            return;
        }
    };

    if range.start != 0 {
        if let Err(err) = file.seek(SeekFrom::Start(range.start)).await {
            log::debug!(
                "Failed to seek file '{}' to {}: {}",
                path.display(),
                range.start,
                err,
            );
            tx.abort();
            return;
        }
    }

    let mut rest_len = range.end - range.start;
    while rest_len != 0 {
        if let Err(err) = SenderReadyFuture(&mut tx).await {
            log::debug!(
                "Connection broken when sending file '{}': {}",
                path.display(),
                err,
            );
            tx.abort();
            return;
        }

        let read_len = rest_len.min(SEND_FILE_BUFFER_LEN as u64) as usize;
        match file.read(&mut buf[..read_len]).await {
            Ok(0) => {
                log::debug!("File truncated '{}'", path.display());
                tx.abort();
                return;
            }
            Ok(got_len) => {
                if let Err(_) = tx.send_data(Chunk::from(buf[..got_len].to_vec())) {
                    log::debug!("Failed to send chunk of file '{}'", path.display());
                    tx.abort();
                    return;
                }
                rest_len -= got_len as u64;
            }
            Err(err) => {
                log::error!("Failed to read file '{}' : {}", path.display(), err);
                tx.abort();
                return;
            }
        }
    }
}
