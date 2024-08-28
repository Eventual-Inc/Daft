use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::Field, schema::Schema, DataType, Series};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    python::PyExpr,
    ExprRef,
};
use pyo3::{pyfunction, PyResult};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct ListContainsFunction;

#[typetag::serde]
impl ScalarUDF for ListContainsFunction {
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

#[pyfunction]
pub fn list_contains(expr: PyExpr, value: PyExpr) -> PyResult<PyExpr> {
    let args = vec![expr.into(), value.into()];
    let scalar_function = ScalarFunction::new(ListContainsFunction, args);
    let expr = ExprRef::from(scalar_function);
    Ok(expr.into())
}
