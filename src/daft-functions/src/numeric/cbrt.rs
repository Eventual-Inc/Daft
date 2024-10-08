use common_error::DaftResult;
use daft_core::prelude::*;
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Cbrt;
use super::{evaluate_single_numeric, to_field_single_floating};

#[typetag::serde]
impl ScalarUDF for Cbrt {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "cbrt"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        to_field_single_floating(self, inputs, schema)
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        evaluate_single_numeric(inputs, Series::cbrt)
    }
}

#[must_use]
pub fn cbrt(input: ExprRef) -> ExprRef {
    ScalarFunction::new(Cbrt {}, vec![input]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};
#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "cbrt")]
pub fn py_cbrt(expr: PyExpr) -> PyResult<PyExpr> {
    Ok(cbrt(expr.into()).into())
}
