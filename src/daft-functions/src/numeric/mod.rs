pub mod abs;
pub mod cbrt;
pub mod ceil;
pub mod exp;
pub mod floor;
pub mod log;

use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{Field, Schema},
    series::Series,
};
use daft_dsl::{functions::ScalarUDF, ExprRef};
#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction_bound!(abs::py_abs, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(cbrt::py_cbrt, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(ceil::py_ceil, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(exp::py_exp, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(floor::py_floor, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(log::py_log2, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(log::py_log10, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(log::py_log, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(log::py_ln, parent)?)?;

    Ok(())
}

pub(self) fn to_field_single_numeric(
    f: &dyn ScalarUDF,
    inputs: &[ExprRef],
    schema: &Schema,
) -> DaftResult<Field> {
    if inputs.len() != 1 {
        return Err(DaftError::SchemaMismatch(format!(
            "Expected 1 input arg, got {}",
            inputs.len()
        )));
    }
    let field = inputs.first().unwrap().to_field(schema)?;
    if !field.dtype.is_numeric() {
        return Err(DaftError::TypeError(format!(
            "Expected input for {} to be numeric, got {}",
            f.name(),
            field.dtype
        )));
    }
    Ok(field)
}
pub(self) fn evaluate_single_numeric<F: Fn(&Series) -> DaftResult<Series>>(
    inputs: &[Series],
    func: F,
) -> DaftResult<Series> {
    if inputs.len() != 1 {
        return Err(DaftError::ValueError(format!(
            "Expected 1 input arg, got {}",
            inputs.len()
        )));
    }
    func(inputs.first().unwrap())
}
