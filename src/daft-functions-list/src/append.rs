use common_error::{DaftResult, ensure};
use daft_core::{
    prelude::{Field, Schema},
    series::Series,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

use crate::series::SeriesListExtension;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListAppend;

#[typetag::serde]
impl ScalarUDF for ListAppend {
    fn name(&self) -> &'static str {
        "list_append"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let other = inputs.required((1, "other"))?;

        // Normalize other input to same # of rows as input
        let other = if other.len() == 1 {
            &other.broadcast(input.len())?
        } else {
            other
        };

        input.list_append(other)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(
            inputs.len() == 2,
            SchemaMismatch: "Expected 2 input args, got {}",
            inputs.len()
        );

        let input = inputs.required((0, "input"))?.to_field(schema)?;
        let other = inputs.required((1, "other"))?.to_field(schema)?;

        ensure!(
            input.dtype.is_list() || input.dtype.is_fixed_size_list(),
            "Input must be a list"
        );

        // The other input should have the same type as the list elements
        let input_exploded = input.to_exploded_field()?;

        ensure!(
            input_exploded.dtype == other.dtype,
            TypeError: "Cannot append value of type {} to list of type {}",
            other.dtype,
            input_exploded.dtype
        );

        Ok(input_exploded.to_list_field())
    }
}

#[must_use]
pub fn list_append(expr: ExprRef, other: ExprRef) -> ExprRef {
    ScalarFn::builtin(ListAppend {}, vec![expr, other]).into()
}
