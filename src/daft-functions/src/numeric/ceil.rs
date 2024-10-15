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
pub struct Ceil {}

#[typetag::serde]
impl ScalarUDF for Ceil {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &'static str {
        "ceil"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        to_field_single_numeric(self, inputs, schema)
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        evaluate_single_numeric(inputs, Series::ceil)
    }
}

#[must_use]
pub fn ceil(input: ExprRef) -> ExprRef {
    ScalarFunction::new(Ceil {}, vec![input]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};

use super::{evaluate_single_numeric, to_field_single_numeric};
#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "ceil")]
pub fn py_ceil(expr: PyExpr) -> PyResult<PyExpr> {
    Ok(ceil(expr.into()).into())
}
