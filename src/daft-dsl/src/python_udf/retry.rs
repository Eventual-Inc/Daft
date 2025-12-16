#[cfg(feature = "python")]
use common_error::{DaftError, DaftResult};
#[cfg(feature = "python")]
use pyo3::{Python, import_exception, types::PyAnyMethods};
#[cfg(feature = "python")]
use rand::Rng;

#[cfg(feature = "python")]
import_exception!(daft.ai.utils, RetryAfterError);

/// Retry configuration constants
#[cfg(feature = "python")]
pub const INITIAL_DELAY_MS: u64 = 100;
#[cfg(feature = "python")]
pub const MAX_DELAY_MS: u64 = 60000;

/// Extract retry-after delay in milliseconds from a DaftError if it contains a RetryAfterError.
#[cfg(feature = "python")]
pub fn retry_after_ms_from_error(py: Option<Python>, err: &DaftError) -> Option<u64> {
    if let DaftError::PyO3Error(py_err) = err {
        let get_retry_after = |py: pyo3::Python| {
            let exception_value = py_err.value(py);

            // Only handle RetryAfterError instances
            if exception_value.is_instance_of::<RetryAfterError>() {
                exception_value
                    .getattr(pyo3::intern!(py, "retry_after"))
                    .ok()
                    .and_then(|val| val.extract::<f64>().ok())
                    .filter(|secs| secs.is_finite() && *secs >= 0.0)
                    .map(|secs| (secs * 1000.0).ceil() as u64)
            } else {
                None
            }
        };

        match py {
            Some(py) => get_retry_after(py),
            None => Python::attach(get_retry_after),
        }
    } else {
        None
    }
}

/// Extract the original exception from a RetryAfterError if present, otherwise return the error as-is.
/// When max retries are exceeded, we should raise the original exception rather than the RetryAfterError wrapper.
#[cfg(feature = "python")]
pub fn unwrap_retry_after_error(py: Option<Python>, err: DaftError) -> DaftError {
    if let DaftError::PyO3Error(py_err) = &err {
        let get_original_exception = |py: pyo3::Python| {
            let exception_value = py_err.value(py);

            // Check if this is a RetryAfterError with an original exception
            if exception_value.is_instance_of::<RetryAfterError>() {
                // Try to get the __cause__ attribute which contains the original exception
                if let Ok(cause) = exception_value.getattr(pyo3::intern!(py, "__cause__"))
                    && !cause.is_none()
                {
                    // If there's an original exception, create a new PyO3Error from it
                    // The cause is a Python exception object
                    return Some(DaftError::PyO3Error(pyo3::PyErr::from_value(cause)));
                }
            }
            None
        };
        let result = match py {
            Some(py) => get_original_exception(py),
            None => Python::attach(get_original_exception),
        };
        if let Some(unwrapped_err) = result {
            return unwrapped_err;
        }
    }
    err
}

#[cfg(feature = "python")]
pub async fn retry_with_backoff_async<F, Fut, T>(func: F, max_retries: usize) -> DaftResult<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = DaftResult<T>>,
{
    let mut result = func().await;
    let mut delay_ms = INITIAL_DELAY_MS;

    for _attempt in 0..max_retries {
        match result {
            Ok(_) => break,
            Err(err) => {
                if let Some(retry_after_ms) = retry_after_ms_from_error(None, &err) {
                    delay_ms = retry_after_ms.min(MAX_DELAY_MS).max(delay_ms);
                }
            }
        }

        // Add ±25% jitter to avoid thundering herd problems
        // This results in a delay between 75% and 125% of the base delay
        let jitter = rand::thread_rng().gen_range(0..=(delay_ms / 2));
        let jittered_delay_ms = (delay_ms * 3 / 4) + jitter;
        tokio::time::sleep(tokio::time::Duration::from_millis(jittered_delay_ms)).await;
        delay_ms = (delay_ms * 2).min(MAX_DELAY_MS);
        result = func().await;
    }

    // If retries are exhausted and we have a RetryAfterError, extract the original exception
    match result {
        Ok(val) => Ok(val),
        Err(err) => {
            if retry_after_ms_from_error(None, &err).is_some() {
                // This is a RetryAfterError, unwrap it to get the original exception
                Err(unwrap_retry_after_error(None, err))
            } else {
                Err(err)
            }
        }
    }
}

#[cfg(feature = "python")]
pub fn retry_with_backoff<F, T>(
    py: Option<Python>,
    mut func: F,
    max_retries: usize,
) -> DaftResult<T>
where
    F: FnMut() -> DaftResult<T>,
{
    use std::{thread, time::Duration};

    let mut result = func();
    let mut delay_ms = INITIAL_DELAY_MS;

    for _attempt in 0..max_retries {
        match result {
            Ok(_) => break,
            Err(err) => {
                if let Some(retry_after_ms) = retry_after_ms_from_error(py, &err) {
                    delay_ms = retry_after_ms.min(MAX_DELAY_MS).max(delay_ms);
                }
            }
        }

        // Add ±25% jitter to avoid thundering herd problems
        // This results in a delay between 75% and 125% of the base delay
        let jitter = rand::thread_rng().gen_range(0..=(delay_ms / 2));
        let jittered_delay_ms = (delay_ms * 3 / 4) + jitter;

        // Release GIL during sleep if Python instance is provided
        if let Some(py) = py {
            py.detach(|| thread::sleep(Duration::from_millis(jittered_delay_ms)));
        } else {
            thread::sleep(Duration::from_millis(jittered_delay_ms));
        }
        delay_ms = (delay_ms * 2).min(MAX_DELAY_MS);
        result = func();
    }

    // If retries are exhausted and we have a RetryAfterError, extract the original exception
    match result {
        Ok(val) => Ok(val),
        Err(err) => {
            if retry_after_ms_from_error(py, &err).is_some() {
                // This is a RetryAfterError, unwrap it to get the original exception
                Err(unwrap_retry_after_error(py, err))
            } else {
                Err(err)
            }
        }
    }
}
