use crate::Expr;
use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::{IntoSeries, Series},
};

use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;

pub(super) struct JoinEvaluator {}

impl FunctionEvaluator for JoinEvaluator {
    fn fn_name(&self) -> &'static str {
        "join"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema, _: &Expr) -> DaftResult<Field> {
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
                        let child_type = input_field.dtype.get_exploded_dtype().unwrap();
                        if child_type != &DataType::Utf8 {
                            return Err(DaftError::TypeError(format!("Expected input to be a list type with a Utf8 child, received child: {}", child_type)));
                        }
                        Ok(Field::new(input.name()?, DataType::Utf8))
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

    fn evaluate(&self, inputs: &[Series], _: &Expr) -> DaftResult<Series> {
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
