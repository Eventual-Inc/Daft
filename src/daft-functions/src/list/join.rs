use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListJoin {}

#[typetag::serde]
impl ScalarUDF for ListJoin {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "list_join"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input, delimiter] => {
                let input_field = input.to_field(schema)?;
                let delimiter_field = delimiter.to_field(schema)?;
                if delimiter_field.dtype != DataType::Utf8 {
                    return Err(DaftError::TypeError(format!(
                        "Expected join delimiter to be of type {}, received: {}",
                        DataType::Utf8,
                        delimiter_field.dtype
                    )));
                }

                match input_field.dtype {
                    DataType::List(_) | DataType::FixedSizeList(_, _) => {
                        let exploded_field = input_field.to_exploded_field()?;
                        if exploded_field.dtype != DataType::Utf8 {
                            return Err(DaftError::TypeError(format!("Expected column \"{}\" to be a list type with a Utf8 child, received list type with child dtype {}", exploded_field.name, exploded_field.dtype)));
                        }
                        Ok(exploded_field)
                    }
                    _ => Err(DaftError::TypeError(format!(
                        "Expected input to be a list type, received: {}",
                        input_field.dtype
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
            [input, delimiter] => {
                let delimiter = delimiter.utf8().unwrap();
                Ok(input.join(delimiter)?.into_series())
            }
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn list_join(expr: ExprRef, delim: ExprRef) -> ExprRef {
    ScalarFunction::new(ListJoin {}, vec![expr, delim]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "list_join")]
pub fn py_list_join(expr: PyExpr, delim: PyExpr) -> PyResult<PyExpr> {
    Ok(list_join(expr.into(), delim.into()).into())
}
