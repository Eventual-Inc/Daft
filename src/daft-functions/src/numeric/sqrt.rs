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

use super::{evaluate_single_numeric, to_field_single_floating};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Sqrt {}

#[typetag::serde]
impl ScalarUDF for Sqrt {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "sqrt"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        to_field_single_floating(self, inputs, schema)
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        evaluate_single_numeric(inputs, Series::sqrt)
    }
}

#[must_use]
pub fn sqrt(input: ExprRef) -> ExprRef {
    ScalarFunction::new(Sqrt {}, vec![input]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "sqrt")]
pub fn py_sqrt(expr: PyExpr) -> PyResult<PyExpr> {
    Ok(sqrt(expr.into()).into())
}
