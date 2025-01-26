use daft_core::prelude::{ImageFormat, ImageMode};
use daft_dsl::python::PyExpr;
use pyo3::{exceptions::PyValueError, pyfunction, PyResult};

use crate::image::{decode::ImageDecode, encode::ImageEncode};

simple_python_wrapper!(image_crop, crate::image::crop::crop, [expr: PyExpr, bbox: PyExpr]);
simple_python_wrapper!(image_to_mode, crate::image::to_mode::image_to_mode, [expr: PyExpr, mode: ImageMode]);

#[pyfunction(signature = (expr, raise_on_error=None, mode=None))]
pub fn image_decode(
    expr: PyExpr,
    raise_on_error: Option<bool>,
    mode: Option<ImageMode>,
) -> PyResult<PyExpr> {
    let image_decode = ImageDecode {
        mode,
        raise_on_error: raise_on_error.unwrap_or(true),
    };
    Ok(crate::image::decode::decode(expr.into(), Some(image_decode)).into())
}

#[pyfunction]
pub fn image_encode(expr: PyExpr, image_format: ImageFormat) -> PyResult<PyExpr> {
    let image_encode = ImageEncode { image_format };
    Ok(crate::image::encode::encode(expr.into(), image_encode).into())
}

#[pyfunction]
pub fn image_resize(expr: PyExpr, w: i64, h: i64) -> PyResult<PyExpr> {
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
    Ok(crate::image::resize::resize(expr.into(), w as u32, h as u32).into())
}
