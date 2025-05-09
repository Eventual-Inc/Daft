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
pub struct ListSlice {}

#[typetag::serde]
impl ScalarUDF for ListSlice {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inner = inputs.into_inner();
        self.evaluate_from_series(&inner)
    }

    fn name(&self) -> &'static str {
        "list_slice"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input, start, end] => {
                let input_field = input.to_field(schema)?;
                let start_field = start.to_field(schema)?;
                let end_field = end.to_field(schema)?;

                if !start_field.dtype.is_integer() {
                    return Err(DaftError::TypeError(format!(
                        "Expected start index to be integer, received: {}",
                        start_field.dtype
                    )));
                }

                if !end_field.dtype.is_integer() && !end_field.dtype.is_null() {
                    return Err(DaftError::TypeError(format!(
                        "Expected end index to be integer or unprovided, received: {}",
                        end_field.dtype
                    )));
                }
                Ok(input_field.to_exploded_field()?.to_list_field()?)
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [input, start, end] => Ok(input.list_slice(start, end)?),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn list_slice(expr: ExprRef, start: ExprRef, end: ExprRef) -> ExprRef {
    ScalarFunction::new(ListSlice {}, vec![expr, start, end]).into()
}
