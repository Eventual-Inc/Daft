#[cfg(feature = "python")]
use common_error::DaftError;
#[cfg(feature = "python")]
use pyo3::{Python, import_exception, types::PyAnyMethods};

#[cfg(feature = "python")]
import_exception!(daft.ai.utils, RetryAfterError);

/// Extract retry-after delay in milliseconds from a DaftError if it contains a RetryAfterError.
#[cfg(feature = "python")]
pub fn retry_after_ms_from_error(err: &DaftError) -> Option<u64> {
    if let DaftError::PyO3Error(py_err) = err {
        Python::attach(|py| {
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
        })
    } else {
        None
    }
}
