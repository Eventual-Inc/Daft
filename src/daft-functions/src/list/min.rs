use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListMin {}

#[typetag::serde]
impl ScalarUDF for ListMin {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "list_min"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let field = input.to_field(schema)?.to_exploded_field()?;

                if field.dtype.is_numeric() {
                    Ok(field)
                } else {
                    Err(DaftError::TypeError(format!(
                        "Expected input to be numeric, got {}",
                        field.dtype
                    )))
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [input] => Ok(input.list_min()?),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn list_min(expr: ExprRef) -> ExprRef {
    ScalarFunction::new(ListMin {}, vec![expr]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "list_min")]
pub fn py_list_min(expr: PyExpr) -> PyResult<PyExpr> {
    Ok(list_min(expr.into()).into())
}
