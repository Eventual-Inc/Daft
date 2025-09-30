// Copyright (c) 2020 Yoshua Wuyts
//
// based on https://crates.io/crates/futures-time
// Licensed under either of Apache License, Version 2.0 or MIT license at your option.

use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use azure_core::sleep::{Sleep, sleep};
use futures::Future;

#[pin_project::pin_project]
#[derive(Debug)]
pub(crate) struct Timeout<F, D> {
    #[pin]
    future: F,
    #[pin]
    deadline: D,
    completed: bool,
}

impl<F, D> Timeout<F, D> {
    pub(crate) fn new(future: F, deadline: D) -> Self {
        Self {
            future,
            deadline,
            completed: false,
        }
    }
}

impl<F: Future, D: Future> Future for Timeout<F, D> {
    type Output = azure_core::Result<F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        assert!(!*this.completed, "future polled after completing");

        match this.future.poll(cx) {
            Poll::Ready(v) => {
                *this.completed = true;
                Poll::Ready(Ok(v))
            }
            Poll::Pending => match this.deadline.poll(cx) {
                Poll::Ready(_) => {
                    *this.completed = true;
                    Poll::Ready(Err(azure_core::error::Error::with_message(
                        azure_core::error::ErrorKind::Other,
                        || String::from("operation timed out"),
                    )))
                }
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

pub(crate) trait TimeoutExt: Future {
    fn timeout(self, duration: Duration) -> Timeout<Self, Sleep>
    where
        Self: Sized,
    {
        Timeout::new(self, sleep(duration))
    }
}

impl<T> TimeoutExt for T where T: Future {}
