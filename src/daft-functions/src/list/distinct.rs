use std::any::Any;

use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListDistinct {}

#[typetag::serde]
impl ScalarUDF for ListDistinct {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "list_distinct"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let field = input.to_field(schema)?;
                match field.dtype {
                    DataType::List(inner_type) => {
                        Ok(Field::new(field.name, DataType::List(inner_type)))
                    }
                    DataType::FixedSizeList(inner_type, _) => {
                        Ok(Field::new(field.name, DataType::List(inner_type)))
                    }
                    _ => Err(DaftError::TypeError(format!(
                        "Expected list input, got {}",
                        field.dtype
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
            [input] => input.list_distinct(),
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

/// Returns a list of unique elements in each list, preserving order of first occurrence and ignoring nulls.
pub fn list_distinct(expr: ExprRef) -> ExprRef {
    ScalarFunction::new(ListDistinct {}, vec![expr]).into()
}
