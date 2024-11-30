use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::try_mean_aggregation_supertype,
    prelude::{Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListMean {}

#[typetag::serde]
impl ScalarUDF for ListMean {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "list_mean"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let inner_field = input.to_field(schema)?.to_exploded_field()?;
                Ok(Field::new(
                    inner_field.name.as_str(),
                    try_mean_aggregation_supertype(&inner_field.dtype)?,
                ))
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [input] => Ok(input.list_mean()?),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn list_mean(expr: ExprRef) -> ExprRef {
    ScalarFunction::new(ListMean {}, vec![expr]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "list_mean")]
pub fn py_list_mean(expr: PyExpr) -> PyResult<PyExpr> {
    Ok(list_mean(expr.into()).into())
}
