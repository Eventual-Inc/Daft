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
pub struct ListUniqueCount;

#[typetag::serde]
impl ScalarUDF for ListUniqueCount {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "list_unique_count"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let field = input.to_field(schema)?;
                Ok(Field::new(field.name, DataType::UInt64))
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [input] => input.list_unique_count(),
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn list_unique_count(expr: ExprRef) -> ExprRef {
    ScalarFunction::new(ListUniqueCount, vec![expr]).into()
}
