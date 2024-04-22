use crate::ExprRef;
use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::{IntoSeries, Series},
};

use crate::functions::FunctionExpr;
use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;

pub(super) struct JoinEvaluator {}

impl FunctionEvaluator for JoinEvaluator {
    fn fn_name(&self) -> &'static str {
        "join"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
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

    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
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
