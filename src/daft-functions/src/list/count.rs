use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{CountMode, DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListCount {
    pub mode: CountMode,
}

#[typetag::serde]
impl ScalarUDF for ListCount {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "count"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let input_field = input.to_field(schema)?;

                match input_field.dtype {
                    DataType::List(_) | DataType::FixedSizeList(_, _) => {
                        Ok(Field::new(input.name(), DataType::UInt64))
                    }
                    _ => Err(DaftError::TypeError(format!(
                        "Expected input to be a list type, received: {}",
                        input_field.dtype
                    ))),
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
            [input] => Ok(input.list_count(self.mode)?.into_series()),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn list_count(expr: ExprRef, mode: CountMode) -> ExprRef {
    ScalarFunction::new(ListCount { mode }, vec![expr]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "list_count")]
pub fn py_list_count(expr: PyExpr, mode: CountMode) -> PyResult<PyExpr> {
    Ok(list_count(expr.into(), mode).into())
}
