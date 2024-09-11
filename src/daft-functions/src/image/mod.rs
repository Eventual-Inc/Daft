pub mod crop;
pub mod decode;
pub mod encode;
pub mod resize;
pub mod to_mode;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction_bound!(crop::py_crop, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(decode::py_decode, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(encode::py_encode, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(resize::py_resize, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(to_mode::py_image_to_mode, parent)?)?;

    Ok(())
}
