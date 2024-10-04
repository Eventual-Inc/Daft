pub mod decode;
pub mod encode;
pub mod geo_ops;

mod utils;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction_bound!(decode::py_decode, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(encode::py_encode, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(geo_ops::py_geo_op, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(geo_ops::py_geo_op_binary, parent)?)?;
    parent.add_class::<utils::GeoOperation>()?;
    Ok(())
}
