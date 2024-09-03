use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};

use common_error::{DaftError, DaftResult};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(super) struct IsNanFunction {}

#[typetag::serde]
impl ScalarUDF for IsNanFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &'static str {
        "is_nan"
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
            [data] => data.is_nan(),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

pub fn is_nan(data: ExprRef) -> ExprRef {
    ScalarFunction::new(IsNanFunction {}, vec![data]).into()
}

#[cfg(feature = "python")]
use {daft_dsl::python::PyExpr, pyo3::prelude::*};

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "is_nan")]
pub fn py_is_nan(expr: PyExpr) -> PyExpr {
    is_nan(expr.into()).into()
}
