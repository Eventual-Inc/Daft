use pyo3::prelude::*;
pub mod series;

pub use series::PySeries;

pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<series::PySeries>()?;

    Ok(())
}
