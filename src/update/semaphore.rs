use futures01::Async as Async01;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio_sync::semaphore as sem;

#[derive(Debug)]
pub struct Semaphore(sem::Semaphore);

impl Semaphore {
    pub fn new(permits: usize) -> Self {
        Semaphore(sem::Semaphore::new(permits))
    }

    pub fn acquire(&self) -> AcquireFuture<'_> {
        AcquireFuture(Some(sem::Permit::new()), &self.0)
    }
}

pub struct AcquireFuture<'a>(Option<sem::Permit>, &'a sem::Semaphore);

impl<'a> Future for AcquireFuture<'a> {
    type Output = Result<PermitGuard<'a>, sem::AcquireError>;

    fn poll(mut self: Pin<&mut Self>, _ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let sem = self.1;
        match self.0.as_mut().unwrap().poll_acquire(sem) {
            Ok(Async01::Ready(())) => Poll::Ready(Ok(PermitGuard(self.0.take().unwrap(), sem))),
            Ok(Async01::NotReady) => Poll::Pending,
            Err(err) => Poll::Ready(Err(err)),
        }
    }
}

#[derive(Debug)]
pub struct PermitGuard<'a>(sem::Permit, &'a sem::Semaphore);

impl Drop for PermitGuard<'_> {
    fn drop(&mut self) {
        self.0.release(self.1);
    }
}
