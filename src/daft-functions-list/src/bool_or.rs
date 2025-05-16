use common_error::{ensure, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::series::SeriesListExtension;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListBoolOr;

#[typetag::serde]
impl ScalarUDF for ListBoolOr {
    fn name(&self) -> &'static str {
        "list_bool_or"
    }

    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        input.list_bool_or()
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.len() == 1, SchemaMismatch: "Expected 1 input, but received {}", inputs.len());
        let input = inputs.required((0, "input"))?.to_field(schema)?;
        let inner_field = input.to_exploded_field()?;

        Ok(Field::new(inner_field.name.as_str(), DataType::Boolean))
    }
}

#[must_use]
pub fn list_bool_or(expr: ExprRef) -> ExprRef {
    ScalarFunction::new(ListBoolOr, vec![expr]).into()
}
