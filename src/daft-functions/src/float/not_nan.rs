use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use common_error::{DaftError, DaftResult};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(super) struct NotNanFunction {}

#[typetag::serde]
impl ScalarUDF for NotNanFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &'static str {
        "not_nan"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data] => match data.to_field(schema) {
                Ok(data_field) => match &data_field.dtype {
                    // DataType::Float16 |
                    DataType::Float32 | DataType::Float64 => {
                        Ok(Field::new(data_field.name, DataType::Boolean))
                    }
                    _ => Err(DaftError::TypeError(format!(
                        "Expects input to is_nan to be float, but received {data_field}",
                    ))),
                },
                Err(e) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [data] => data.not_nan(),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

pub fn not_nan(data: ExprRef) -> ExprRef {
    ScalarFunction::new(NotNanFunction {}, vec![data]).into()
}

#[cfg(feature = "python")]
use {daft_dsl::python::PyExpr, pyo3::prelude::*};

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "not_nan")]
pub fn py_not_nan(data: PyExpr) -> PyExpr {
    let expr: ExprRef = ScalarFunction::new(NotNanFunction {}, vec![data.into()]).into();

    expr.into()
}
