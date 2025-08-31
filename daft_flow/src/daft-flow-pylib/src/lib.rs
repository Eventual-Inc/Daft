use pyo3::prelude::*;

#[pyfunction]
fn hello_world() -> &'static str {
    "Hello, world!"
}

pub fn register_modules(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(hello_world, m)?)?;
    Ok(())
}
