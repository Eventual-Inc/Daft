use pyo3::prelude::*;

pub mod pylib {
    use daft_io::python::PyIOConfig;
    use daft_table::python::PyTable;
    use pyo3::{pyfunction, PyResult, Python};

    #[pyfunction]
    pub fn read_parquet(
        py: Python,
        uri: &str,
        size: Option<usize>,
        io_config: Option<PyIOConfig>,
    ) -> PyResult<PyTable> {
        py.allow_threads(|| {
            Ok(
                crate::read::read_parquet(uri, size, io_config.unwrap_or_default().config.into())?
                    .into(),
            )
        })
    }
}
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_wrapped(wrap_pyfunction!(pylib::read_parquet))?;
    Ok(())
}
