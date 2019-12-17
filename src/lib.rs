use futures::TryFutureExt as _;
use hyper;
use tokio;

pub mod database;
pub mod server;
pub mod update;
mod util;

pub fn block_on(fut: impl std::future::Future<Output = ()> + Send + 'static) {
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

pub(crate) fn spawn(fut: impl std::future::Future<Output = ()> + Send + 'static) {
    hyper::rt::spawn(
        Box::pin(async {
            fut.await;
            Ok(())
        })
        .compat(),
    );
}

#[cfg(test)]
pub(crate) mod tests {
    pub fn init_logger() {
        use std::sync::Once;
        static ONCE: Once = Once::new();
        ONCE.call_once(env_logger::init);
    }
}
