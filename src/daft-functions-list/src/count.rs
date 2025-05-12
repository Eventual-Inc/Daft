use common_error::{ensure, DaftError, DaftResult};
use daft_core::{
    prelude::{CountMode, DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    lit, ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::series::SeriesListExtension;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListCount;

#[typetag::serde]
impl ScalarUDF for ListCount {
    fn name(&self) -> &'static str {
        "list_count"
    }
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let count_mode: CountMode = inputs.required_scalar((1, "mode"))?;

        Ok(input.list_count(count_mode)?.into_series())
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.len() == 2, SchemaMismatch: "Expected 2 input args, got {}", inputs.len());

        let input_field = inputs.required((0, "input"))?.to_field(schema)?;
        let _: CountMode = inputs.required_scalar((1, "mode"))?;

        Ok(Field::new(input_field.name, DataType::UInt64))
    }
}

#[must_use]
pub fn list_count(expr: ExprRef, mode: CountMode) -> ExprRef {
    ScalarFunction::new(ListCount, vec![expr, lit(mode)]).into()
}
