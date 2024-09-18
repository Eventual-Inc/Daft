use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct CbrtFunction;
use super::evaluate_single_numeric;

#[typetag::serde]
impl ScalarUDF for CbrtFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "cbrt"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let field = input.to_field(schema)?;
                let dtype = field.dtype.to_floating_representation()?;
                Ok(Field::new(field.name, dtype))
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        evaluate_single_numeric(inputs, Series::cbrt)
    }
}

pub fn cbrt(input: ExprRef) -> ExprRef {
    ScalarFunction::new(CbrtFunction {}, vec![input]).into()
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
