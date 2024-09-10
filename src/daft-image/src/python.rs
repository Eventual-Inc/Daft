use daft_core::{
    prelude::{ImageFormat, ImageMode},
    python::PySeries,
};
use pyo3::{exceptions::PyValueError, prelude::*};

#[pyfunction]
pub fn decode(
    s: &PySeries,
    raise_error_on_failure: bool,
    mode: Option<ImageMode>,
) -> PyResult<PySeries> {
    let s = crate::series::decode(&s.series, raise_error_on_failure, mode)?;
    Ok(s.into())
}

#[pyfunction]
pub fn encode(s: &PySeries, image_format: ImageFormat) -> PyResult<PySeries> {
    let s = crate::series::encode(&s.series, image_format)?;
    Ok(s.into())
}

#[pyfunction]
pub fn resize(s: &PySeries, w: i64, h: i64) -> PyResult<PySeries> {
    if w < 0 {
        return Err(PyValueError::new_err(format!(
            "width can not be negative: {w}"
        )));
    }
    if h < 0 {
        return Err(PyValueError::new_err(format!(
            "height can not be negative: {h}"
        )));
    }
    let s = crate::series::resize(&s.series, w as u32, h as u32)?;
    Ok(s.into())
}

#[pyfunction]
pub fn to_mode(s: &PySeries, mode: &ImageMode) -> PyResult<PySeries> {
    let s = crate::series::to_mode(&s.series, *mode)?;
    Ok(s.into())
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    let module = PyModule::new_bound(parent.py(), "image")?;
    module.add_wrapped(wrap_pyfunction!(decode))?;
    module.add_wrapped(wrap_pyfunction!(encode))?;
    module.add_wrapped(wrap_pyfunction!(resize))?;
    module.add_wrapped(wrap_pyfunction!(to_mode))?;
    parent.add_submodule(&module)?;
    Ok(())
}
