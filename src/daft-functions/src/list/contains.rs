use common_error::{DaftError, DaftResult};
use daft_core::prelude::{DataType, Field, Schema, Series};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};
#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListContains;

#[typetag::serde]
impl ScalarUDF for ListContains {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "list_contains"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data, value] => {
                let data_field = data.to_field(schema)?;
                let value_field = value.to_field(schema)?;
                match &data_field.dtype {
                    DataType::List(inner_type) => {
                        if inner_type.as_ref() == &value_field.dtype {
                            Ok(data_field)
                        } else {
                            Err(DaftError::TypeError(format!(
                                "Can't check for the existence of a value of type {} in a list of type {}",
                                value_field.dtype,
                                inner_type.as_ref(),
                            )))
                        }
                    }
                    _ => Err(DaftError::TypeError(format!(
                        "Expected list, got {}",
                        data_field.dtype
                    ))),
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [data, value] => data.list_contains(value),
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

pub fn list_contains(expr: ExprRef, value: ExprRef) -> ExprRef {
    let args = vec![expr, value];
    let scalar_function = ScalarFunction::new(ListContains, args);
    scalar_function.into()
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "list_contains")]
pub fn py_list_contains(expr: PyExpr, value: PyExpr) -> PyResult<PyExpr> {
    Ok(list_contains(expr.into(), value.into()).into())
}
