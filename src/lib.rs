use futures;

pub mod database;
pub mod server;
pub mod update;

pub fn block_on(fut: impl std::future::Future<Output = ()> + Send + 'static) {
    use futures::TryFutureExt as _;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    let done = Arc::new(AtomicBool::new(false));
    let done_ = done.clone();
    tokio::runtime::Runtime::new()
        .expect("Cannot start runtime")
        .block_on(
            Box::pin(async move {
                fut.await;
                done_.store(true, Ordering::SeqCst);
                <Result<(), ()>>::Ok(())
            })
            .compat(),
        )
        .unwrap();

    if !done.load(Ordering::SeqCst) {
        panic!("Future panicked");
    }
}
