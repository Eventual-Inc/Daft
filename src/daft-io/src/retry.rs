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
    pub async fn retry<T, E, Fut>(&self, mut f: impl FnMut() -> Fut) -> Result<T, E>
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, atomic::{AtomicU32, Ordering}};

    #[tokio::test]
    async fn test_exponential_backoff_transient_eventually_succeeds() {
        let backoff = ExponentialBackoff { max_tries: 5, jitter_ms: 1, max_backoff_ms: 5, max_waittime_ms: Some(1000) };
        let attempts = Arc::new(AtomicU32::new(0));
        let res = backoff
            .retry(|| {
                let attempts_clone = attempts.clone();
                async move {
                    let now = attempts_clone.fetch_add(1, Ordering::Relaxed) + 1;
                    if now < 3 {
                        Err(RetryError::Transient("oops"))
                    } else {
                        Ok("ok")
                    }
                }
            })
            .await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), "ok");
        assert!(attempts.load(Ordering::Relaxed) >= 3);
    }

    #[tokio::test]
    async fn test_exponential_backoff_permanent_does_not_retry() {
        let backoff = ExponentialBackoff { max_tries: 5, jitter_ms: 1, max_backoff_ms: 5, max_waittime_ms: Some(1000) };
        let attempts = Arc::new(AtomicU32::new(0));
        let res: Result<&str, &str> = backoff
            .retry(|| {
                let attempts_clone = attempts.clone();
                async move {
                    attempts_clone.fetch_add(1, Ordering::Relaxed);
                    Err(RetryError::Permanent("nope"))
                }
            })
            .await;
        assert!(res.is_err());
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }
}
