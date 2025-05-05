use common_error::{ensure, DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Utf8Capitalize;

#[typetag::serde]
impl ScalarUDF for Utf8Capitalize {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        input.utf8_capitalize()
    }

    fn name(&self) -> &'static str {
        "capitalize"
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.len() == 1, SchemaMismatch: "Expected 1 input, but received {}", inputs.len());
        let input = inputs.required((0, "input"))?.to_field(schema)?;
        ensure!(
            input.dtype == DataType::Utf8,
            TypeError: "Expects input to capitalize to be utf8, but recieved {}", input.dtype
        );

        Ok(Field::new(input.name, DataType::Utf8))
    }

    fn docstring(&self) -> &'static str {
        "Capitalize a UTF-8 string."
    }
}

#[must_use]
pub fn utf8_capitalize(input: ExprRef) -> ExprRef {
    ScalarFunction::new(Utf8Capitalize, vec![input]).into()
}
