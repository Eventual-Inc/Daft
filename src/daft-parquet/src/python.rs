use pyo3::prelude::*;

pub mod pylib {
    use daft_io::{get_io_client, python::PyIOConfig};
    use daft_table::python::PyTable;
    use pyo3::{pyfunction, PyResult, Python};

    #[pyfunction]
    pub fn read_parquet(
        py: Python,
        uri: &str,
        columns: Option<Vec<&str>>,
        row_groups: Option<Vec<i64>>,
        size: Option<usize>,
        io_config: Option<PyIOConfig>,
    ) -> PyResult<PyTable> {
        py.allow_threads(|| {
            let io_client = get_io_client(io_config.unwrap_or_default().config.into())?;

            Ok(crate::read::read_parquet(
                uri,
                columns.as_deref(),
                row_groups.as_deref(),
                size,
                io_client,
            )?
            .into())
        })
    }
}
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_wrapped(wrap_pyfunction!(pylib::read_parquet))?;
    Ok(())
}
