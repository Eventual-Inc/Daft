use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::functions::{FunctionArgs, ScalarUDF};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Uuid;

#[typetag::serde]
impl ScalarUDF for Uuid {
    fn name(&self) -> &'static str {
        "uuid"
    }

    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let iter = (0..input.len()).map(|_| Some(::uuid::Uuid::new_v4().to_string()));
        Ok(Utf8Array::from_iter(input.name(), iter).into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<daft_dsl::ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        if inputs.is_empty() {
            return Ok(Field::new("", DataType::Utf8));
        }
        if inputs.len() != 1 {
            return Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            )));
        }
        let input = inputs.required((0, "input"))?;
        let input_field = input.to_field(schema)?;
        Ok(Field::new(input_field.name, DataType::Utf8))
    }

    fn docstring(&self) -> &'static str {
        "Generates a column of UUID strings."
    }
}
