use pyo3::prelude::*;

#[pyfunction]
fn example_url_download(url: &str) -> Option<Vec<u8>> {
    daft_functions_adapter::example_uri_download(url)
}

pub fn register_modules(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(example_url_download, m)?)?;
    Ok(())
}
