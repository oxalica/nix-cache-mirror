use std::{
    future::Future,
    pin::Pin,
    sync::Mutex as SyncMutex,
    task::{Context, Poll, Waker},
};

#[derive(Debug)]
pub struct Semaphore {
    inner: SyncMutex<(usize, Vec<Waker>)>,
}

impl Semaphore {
    pub fn new(permits: usize) -> Self {
        Self {
            inner: SyncMutex::new((permits.into(), Vec::with_capacity(permits * 2))),
        }
    }

    pub fn acquire(&self) -> AcquireFuture<'_> {
        AcquireFuture { sem: self }
    }
}

#[derive(Debug)]
pub struct AcquireFuture<'s> {
    sem: &'s Semaphore,
}

impl<'s> Future for AcquireFuture<'s> {
    type Output = Guard<'s>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let sem = self.sem;
        let mut g = sem.inner.lock().unwrap();
        if g.0 >= 1 {
            g.0 -= 1;
            Poll::Ready(Guard { sem })
        } else {
            g.1.push(ctx.waker().clone());
            Poll::Pending
        }
    }
}

#[derive(Debug)]
pub struct Guard<'s> {
    sem: &'s Semaphore,
}

impl Drop for Guard<'_> {
    fn drop(&mut self) {
        if let Ok(mut g) = self.sem.inner.lock() {
            g.0 += 1;
            if let Some(w) = g.1.pop() {
                w.wake();
            }
        }
    }
}
