use crate::Expr;
use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::{IntoSeries, Series},
};

use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;

pub(super) struct ContainsEvaluator {}

impl FunctionEvaluator for ContainsEvaluator {
    fn fn_name(&self) -> &'static str {
        "contains"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema, _: &Expr) -> DaftResult<Field> {
        match inputs {
            [input, element] => {
                let input_field = input.to_field(schema)?;
                let element_field = element.to_field(schema)?;

                match input_field.dtype {
                    DataType::List(child) | DataType::FixedSizeList(child, _) => {
                        if child.dtype != element_field.dtype {
                            Err(DaftError::TypeError(format!("Expected element type to match child type of list, but received: {} vs {}", element_field.dtype, child.dtype)))
                        } else {
                            Ok(Field::new(input.name()?, DataType::Boolean))
                        }
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
            [input, element] => Ok(input.contains(element)?.into_series()),
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
