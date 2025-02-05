use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListBoolAnd;

#[typetag::serde]
impl ScalarUDF for ListBoolAnd {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "list_bool_and"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let inner_field = input.to_field(schema)?.to_exploded_field()?;
                Ok(Field::new(
                    inner_field.name.as_str(),
                    daft_core::datatypes::DataType::Boolean,
                ))
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [input] => Ok(input.list_bool_and()?),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn list_bool_and(expr: ExprRef) -> ExprRef {
    ScalarFunction::new(ListBoolAnd, vec![expr]).into()
}
