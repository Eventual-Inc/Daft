use std::{future::Future, time::Duration};

use rand::Rng;

pub enum RetryError<T> {
    /// Error that should not be retried
    Permanent(T),
    /// Error that can be retried
    Transient(T),
}

impl<T> RetryError<T> {
    fn into_inner(self) -> T {
        match self {
            Self::Permanent(value) | Self::Transient(value) => value,
        }
    }
}

pub struct ExponentialBackoff {
    pub max_tries: usize,
    pub jitter_ms: u64,
    pub max_backoff_ms: u64,
    pub max_waittime_ms: Option<u64>,
}

impl Default for ExponentialBackoff {
    fn default() -> Self {
        Self {
            max_tries: 4,
            jitter_ms: 2_500,
            max_backoff_ms: 20_000,
            max_waittime_ms: None,
        }
    }
}

impl ExponentialBackoff {
    pub async fn retry<T, E, Fut>(&self, f: impl Fn() -> Fut) -> Result<T, E>
    where
        Fut: Future<Output = Result<T, RetryError<E>>>,
    {
        let mut attempts = 0;
        let start_time = std::time::Instant::now();

        loop {
            attempts += 1;

            let res = f().await;

            let success = res.is_ok();
            let permanent = matches!(res, Result::Err(RetryError::Permanent(..)));
            let exceeded_max_waittime = self
                .max_waittime_ms
                .is_some_and(|t| start_time.elapsed().as_millis() >= t as u128);
            let exceeded_max_tries = attempts == self.max_tries;

            if success || permanent || exceeded_max_waittime || exceeded_max_tries {
                return res.map_err(RetryError::into_inner);
            }

            let jitter = rand::thread_rng()
                .gen_range(0..((1 << attempts) * self.jitter_ms))
                .min(self.max_backoff_ms);

            tokio::time::sleep(Duration::from_millis(jitter)).await;
        }
    }
}
