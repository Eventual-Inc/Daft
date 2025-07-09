use common_error::{ensure, DaftResult};
use daft_core::{
    prelude::{Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::series::SeriesListExtension;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListSlice;

#[typetag::serde]
impl ScalarUDF for ListSlice {
    fn name(&self) -> &'static str {
        "list_slice"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let start = inputs.required((1, "start"))?;
        let end = inputs.required((2, "end"))?;
        input.list_slice(start, end)
    }
    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(
            inputs.len() == 3,
            SchemaMismatch: "Expected 3 input args, got {}",
            inputs.len()
        );

        let input = inputs.required((0, "input"))?.to_field(schema)?;
        ensure!(
            input.dtype.is_list() || input.dtype.is_fixed_size_list(),
            "Input must be a list"
        );

        let start = inputs.required((1, "start"))?.to_field(schema)?;
        ensure!(
            start.dtype.is_integer(),
            TypeError: "Start index must be an integer, received: {}",
            start.dtype
        );

        if let Some(end) = inputs
            .optional((2, "end"))?
            .map(|expr| expr.to_field(schema))
            .transpose()?
        {
            ensure!(
                end.dtype.is_integer() || end.dtype.is_null(),
                TypeError: "End index must be an integer, received: {}",
                end.dtype
            );
        }

        input.to_exploded_field()?.to_list_field()
    }
}

#[must_use]
pub fn list_slice(expr: ExprRef, start: ExprRef, end: ExprRef) -> ExprRef {
    ScalarFunction::new(ListSlice {}, vec![expr, start, end]).into()
}
