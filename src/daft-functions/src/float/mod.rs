mod fill_nan;
mod is_inf;
mod is_nan;
mod not_nan;

use daft_dsl::{functions::ScalarFunction, ExprRef};
use fill_nan::FillNan;
use is_inf::IsInf;
use is_nan::IsNan;
use not_nan::NotNan;
#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction_bound!(
        crate::python::float::fill_nan,
        parent
    )?)?;
    parent.add_function(wrap_pyfunction_bound!(
        crate::python::float::is_inf,
        parent
    )?)?;
    parent.add_function(wrap_pyfunction_bound!(
        crate::python::float::is_nan,
        parent
    )?)?;
    parent.add_function(wrap_pyfunction_bound!(
        crate::python::float::not_nan,
        parent
    )?)?;

    Ok(())
}

#[must_use]
pub fn not_nan(input: ExprRef) -> ExprRef {
    ScalarFunction::new(NotNan {}, vec![input]).into()
}

#[must_use]
pub fn is_nan(input: ExprRef) -> ExprRef {
    ScalarFunction::new(IsNan {}, vec![input]).into()
}

#[must_use]
pub fn fill_nan(input: ExprRef, fill_value: ExprRef) -> ExprRef {
    ScalarFunction::new(FillNan {}, vec![input, fill_value]).into()
}

#[must_use]
pub fn is_inf(input: ExprRef) -> ExprRef {
    ScalarFunction::new(IsInf {}, vec![input]).into()
}
