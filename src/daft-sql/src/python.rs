use pyo3::{pyfunction, PyResult};

#[pyfunction]
pub fn sql(_sql: &str) -> PyResult<String> {
    Ok("Hello from Rust".to_string())
}
