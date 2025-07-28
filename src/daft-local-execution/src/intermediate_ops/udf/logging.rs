use common_error::DaftResult;
use console::style;
use pyo3::{intern, pyclass, pymethods, types::PyAnyMethods, IntoPyObject, PyErr, Python};

/// Wrapper around logging that duck-types the Python `sys.stdout` and `sys.stderr` objects.
///
/// This allows us to log to the console while running a Python UDF on the same process.
/// With a separate process, we can just pipe the stdout & stderr to the main process.
#[pyclass]
struct LoggingStdout {
    buffer: String,
}

impl LoggingStdout {
    pub fn new() -> Self {
        Self {
            buffer: String::new(),
        }
    }
}

#[pymethods]
impl LoggingStdout {
    pub fn write(&mut self, message: &str) {
        self.buffer.push_str(message);

        // Check if the buffer contains a newline
        if let Some(newline_pos) = self.buffer.find('\n') {
            let prefix = style("[UDF on Thread]").green();
            // Split at the newline and write the complete lines
            let (complete_lines, remaining) = self.buffer.split_at(newline_pos + 1);
            log::error!(target: "PYTHON", "{} {}", prefix, complete_lines.trim_end_matches('\n'));

            // Keep the remaining content in the buffer
            self.buffer = remaining.to_string();
        }
    }

    pub fn flush(&mut self) {
        if !self.buffer.is_empty() {
            let prefix = style("[UDF on Thread]").green();
            log::error!(target: "PYTHON", "{} {}", prefix, self.buffer);
            self.buffer.clear();
        }
    }

    // Just need the property for hasattr
    #[allow(dead_code)]
    pub fn is_daft(&self) {}
}

/// Helper function to register a Python stdout & stderr writer for thread logging
pub fn with_py_thread_logger<T>(
    f: impl FnOnce() -> DaftResult<T>,
    in_ray_runner: bool,
) -> DaftResult<T> {
    // Ray already messes with Python's prints w/ progress bars, so we shouldn't do so on top of it.
    // https://docs.ray.io/en/latest/ray-observability/user-guides/configure-logging.html#distributed-progress-bars-with-tqdm
    if in_ray_runner {
        return f();
    }

    let output = Python::with_gil(|py| {
        let std_log = LoggingStdout::new();

        let sys = py.import(intern!(py, "sys"))?;
        let old_stdout = sys.getattr("stdout")?;
        let old_stderr = sys.getattr("stderr")?;

        let std_log_py = std_log.into_pyobject(py)?;
        sys.setattr("stdout", std_log_py.clone())?;
        sys.setattr("stderr", std_log_py)?;

        let output = f()?;

        if !old_stdout.hasattr("is_daft")? {
            sys.setattr("stdout", old_stdout)?;
            sys.setattr("stderr", old_stderr)?;
        }

        Ok::<T, PyErr>(output)
    })?;

    Ok(output)
}
