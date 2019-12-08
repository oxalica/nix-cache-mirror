use crate::database::{Database, GenerationExtraInfo, StorePath};
use failure::{ensure, format_err, Error, ResultExt as _};
use futures::{
    compat::{Future01CompatExt as _, Stream01CompatExt as _},
    prelude::*,
};
use lazy_static::lazy_static;
use log;
use reqwest::r#async::Client;
use xz2;

type Result<T> = std::result::Result<T, Error>;

lazy_static! {
    static ref CLIENT: Client = Client::new();
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
    if rev.len() == 40 && rev.chars().all(|c| c.is_ascii_alphanumeric()) {
        rev.make_ascii_lowercase();
        Ok(rev)
    } else {
        Err(format_err!("Invalid git revision"))
    }
}

pub async fn add_nix_channel_generation(
    db: &mut Database,
    channel_url: &str,
    cache_url: Option<&str>,
) -> Result<i64> {
    let revision_url = format!("{}/git-revision", channel_url);
    let store_path_url = format!("{}/store-paths.xz", channel_url);
    let cache_url_url = format!("{}/binary-cache-url", channel_url);

    log::info!("Fetching metadata");
    let git_revision1 = get_git_revision(&revision_url)
        .await
        .context("Cannot get git revision")?;

    let cache_url = match cache_url {
        Some(url) => url.to_owned(),
        None => get_all_to_string(&cache_url_url)
            .await
            .context("Cannot get binary cache url")?,
    };

    log::info!("Fetching store paths");
    let store_paths = get_store_paths(&store_path_url)
        .await
        .context("Cannot get store paths")?;

    log::info!("Checking git revision");
    let git_revision2 = get_git_revision(&revision_url).await?;
    log::info!("rev = {}", git_revision1);

    ensure!(
        git_revision1 == git_revision2,
        "Revision mismatch, before = {}, after = {}",
        git_revision1,
        git_revision2,
    );

    add_raw_generation(
        db,
        &cache_url,
        &GenerationExtraInfo {
            channel_url: Some(channel_url.to_owned()),
            total_paths: None,
            total_file_size: None,
        },
        store_paths,
    )
}

pub fn add_raw_generation<I: IntoIterator<Item = StorePath>>(
    db: &mut Database,
    cache_url: &str,
    extra_info: &GenerationExtraInfo,
    store_paths: I,
) -> Result<i64>
where
    I::IntoIter: ExactSizeIterator,
{
    let store_paths = store_paths.into_iter();
    log::info!("Saving {} root store paths", store_paths.size_hint().0);
    let gen_id = db.insert_generation(cache_url, extra_info, store_paths)?;
    log::info!("New generation {} added", gen_id);
    Ok(gen_id)
}

async fn get_store_paths(url: &str) -> Result<Vec<StorePath>> {
    use std::io::{BufRead, BufReader, Cursor};
    use xz2::read::XzDecoder;

    let resp = get_all_to_vec(&url).await?;
    BufReader::new(XzDecoder::new(Cursor::new(resp)))
        .lines()
        .map(|line| {
            let line = line?;
            Ok(line
                .parse::<StorePath>()
                .with_context(|err| format_err!("Invalid store path '{}': {}", line, err))?)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{block_on, database::model::*};
    use chrono::{Duration, Utc};

    #[test]
    fn test_parse_nix_path() {
        let p = |s: &str| s.parse::<StorePath>();

        assert!(p("").is_err());
        assert!(p("/nix/store💗️").is_err());
        assert!(p("/nix/store/").is_err());
        assert!(p("/nix/store/00000000000000000000000000000000").is_err());
        assert!(p("/nix/store/0000000000000000000000000000000💗️").is_err());
        assert!(p("/nix/store/00000000000000000000000000000000-").is_err());
        assert!(p("/nix/store/00000000000000000000000000000000-💗️").is_err());
        assert_eq!(
            p("/nix/store/5yr2767rqnvwvsfy445ny41lk67fcjjh-VSCode_1.40.1_linux-x64.tar.gz")
                .unwrap(),
            StorePath {
                hash: "5yr2767rqnvwvsfy445ny41lk67fcjjh".to_owned(),
                name: "VSCode_1.40.1_linux-x64.tar.gz".to_owned(),
            },
        );
    }

    #[test]
    #[ignore]
    fn test_add_channel() {
        block_on(async {
            const SAVE_TIME_LIMIT_SECS: i64 = 30;

            let mut db = Database::open_in_memory().unwrap();
            let channel_url = "https://nixos.org/channels/nixos-unstable";
            let gen_id = add_nix_channel_generation(&mut db, channel_url, None)
                .await
                .unwrap();
            let mut gens = db.select_generations().unwrap();
            let now = Utc::now();

            eprintln!("{:?}", gens);
            assert_eq!(gens.len(), 1);
            let mut gen = gens.pop().unwrap();

            assert!(now - gen.start_time < Duration::seconds(SAVE_TIME_LIMIT_SECS));
            gen.start_time = now;

            assert_eq!(
                gen,
                Generation {
                    id: gen_id,
                    start_time: now,
                    end_time: None,
                    cache_url: "https://cache.nixos.org".to_owned(),
                    extra_info: GenerationExtraInfo {
                        channel_url: Some(channel_url.to_owned()),
                        total_paths: None,
                        total_file_size: None,
                    },
                    status: GenerationStatus::Pending,
                }
            );
        });
    }
}
