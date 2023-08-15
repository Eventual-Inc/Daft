#[cfg(feature = "python")]
pub mod pylib {

    use pyo3::prelude::*;

    #[pyfunction]
    pub fn version() -> &'static str {
        daft_core::VERSION
    }

    #[pyfunction]
    pub fn build_type() -> &'static str {
        daft_core::DAFT_BUILD_TYPE
    }

    #[pymodule]
    fn daft(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
        pyo3_log::init();

        daft_core::register_modules(_py, m)?;
        daft_core::python::register_modules(_py, m)?;
        daft_dsl::register_modules(_py, m)?;
        daft_table::register_modules(_py, m)?;
        daft_io::register_modules(_py, m)?;
        daft_parquet::register_modules(_py, m)?;
        daft_plan::register_modules(_py, m)?;

        m.add_wrapped(wrap_pyfunction!(version))?;
        m.add_wrapped(wrap_pyfunction!(build_type))?;
        Ok(())
    }
}
