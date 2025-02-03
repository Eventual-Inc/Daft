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
pub struct ListUnique {
    ignore_nulls: bool,
}

#[typetag::serde]
impl ScalarUDF for ListUnique {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "list_unique"
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
            [input] => input.list_unique(self.ignore_nulls),
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

/// Returns a list of unique elements in each list, preserving order of first occurrence.
///
/// When ignore_nulls is true (default), nulls are excluded from the result.
/// When ignore_nulls is false, nulls are included in the result.
pub fn list_unique(expr: ExprRef, ignore_nulls: bool) -> ExprRef {
    ScalarFunction::new(ListUnique { ignore_nulls }, vec![expr]).into()
}
