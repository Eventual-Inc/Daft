use common_error::DaftResult;
use daft_core::{
    prelude::{Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Round {
    decimal: i32,
}

#[typetag::serde]
impl ScalarUDF for Round {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "round"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        to_field_single_numeric(self, inputs, schema)
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        evaluate_single_numeric(inputs, |s| s.round(self.decimal))
    }
}

#[must_use]
pub fn round(input: ExprRef, decimal: i32) -> ExprRef {
    ScalarFunction::new(Round { decimal }, vec![input]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};

use super::{evaluate_single_numeric, to_field_single_numeric};
#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "round")]
pub fn py_round(expr: PyExpr, decimal: i32) -> PyResult<PyExpr> {
    use pyo3::exceptions::PyValueError;

    if decimal < 0 {
        return Err(PyValueError::new_err(format!(
            "decimal can not be negative: {decimal}"
        )));
    }
    Ok(round(expr.into(), decimal).into())
}
