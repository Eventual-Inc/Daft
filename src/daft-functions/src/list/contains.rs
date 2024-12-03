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

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub struct ListContains;

#[typetag::serde]
impl ScalarUDF for ListContains {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "list_contains"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input, contains] => {
                let input = input.to_field(schema)?;
                let contains = contains.to_field(schema)?;
                match input.dtype {
                    DataType::List(internal_dt) | DataType::FixedSizeList(internal_dt, _) => {
                        if internal_dt.as_ref() != &contains.dtype {
                            Ok(Field::new(input.name, DataType::Boolean))
                        } else {
                            Err(DaftError::TypeError(format!(
                                "Expected input and contains to have the same type, received: {} and {}",
                                internal_dt, contains.dtype
                            )))
                        }
                    }
                    dt => Err(DaftError::TypeError(format!(
                        "Expected input to be a list type, received: {}",
                        dt,
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
            [input, contains] => input.list_contains(contains),
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn list_contains(expr: ExprRef, contains: ExprRef) -> ExprRef {
    ScalarFunction::new(ListContains, vec![expr, contains]).into()
}
