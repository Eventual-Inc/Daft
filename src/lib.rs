const VERSION: &str = env!("CARGO_PKG_VERSION");
const BUILD_TYPE_DEV: &str = "dev";
const DAFT_BUILD_TYPE: &str = {
    let env_build_type: Option<&str> = option_env!("RUST_DAFT_PKG_BUILD_TYPE");
    match env_build_type {
        Some(val) => val,
        None => BUILD_TYPE_DEV,
    }
};

#[cfg(feature = "python")]
pub mod pylib {

    use pyo3::prelude::*;

    // #[pyfunction]
    // fn version() -> &'static str {
    //     VERSION
    // }

    // #[pyfunction]
    // fn build_type() -> &'static str {
    //     DAFT_BUILD_TYPE
    // }

    #[pymodule]
    fn daft(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
        pyo3_log::init();

        core::python::register_modules(_py, m)?;
        // m.add_wrapped(wrap_pyfunction!(version))?;
        // m.add_wrapped(wrap_pyfunction!(build_type))?;
        Ok(())
    }
}
