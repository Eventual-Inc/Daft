pub mod abs;
pub mod cbrt;
pub mod ceil;
pub mod exp;
pub mod floor;
pub mod log;
pub mod round;
pub mod sign;
pub mod sqrt;
pub mod trigonometry;

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
    parent.add_function(wrap_pyfunction_bound!(round::py_round, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(sign::py_sign, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(sqrt::py_sqrt, parent)?)?;

    parent.add_function(wrap_pyfunction_bound!(trigonometry::py_sin, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(trigonometry::py_cos, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(trigonometry::py_tan, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(trigonometry::py_cot, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(trigonometry::py_arcsin, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(trigonometry::py_arccos, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(trigonometry::py_arctan, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(trigonometry::py_radians, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(trigonometry::py_degrees, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(trigonometry::py_arctanh, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(trigonometry::py_arccosh, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(trigonometry::py_arcsinh, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(trigonometry::py_arctan2, parent)?)?;

    Ok(())
}

fn to_field_single_numeric(
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
            "Expected input to {} to be numeric, got {}",
            f.name(),
            field.dtype
        )));
    }
    Ok(field)
}

fn to_field_single_floating(
    f: &dyn ScalarUDF,
    inputs: &[ExprRef],
    schema: &Schema,
) -> DaftResult<Field> {
    match inputs {
        [first] => {
            let field = first.to_field(schema)?;
            let dtype = field.dtype.to_floating_representation()?;
            Ok(Field::new(field.name, dtype))
        }
        _ => Err(DaftError::SchemaMismatch(format!(
            "Expected 1 input arg for {}, got {}",
            f.name(),
            inputs.len()
        ))),
    }
}
fn evaluate_single_numeric<F: Fn(&Series) -> DaftResult<Series>>(
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
