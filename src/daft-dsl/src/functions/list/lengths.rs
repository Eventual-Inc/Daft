use crate::Expr;
use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::{IntoSeries, Series},
};

use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;

pub(super) struct LengthsEvaluator {}

impl FunctionEvaluator for LengthsEvaluator {
    fn fn_name(&self) -> &'static str {
        "lengths"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema, _: &Expr) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let input_field = input.to_field(schema)?;

                match input_field.dtype {
                    DataType::List(_) | DataType::FixedSizeList(_, _) => {
                        Ok(Field::new(input.name()?, DataType::UInt64))
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

    fn evaluate(&self, inputs: &[Series], _: &Expr) -> DaftResult<Series> {
        match inputs {
            [input] => Ok(input.list_lengths()?.into_series()),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
