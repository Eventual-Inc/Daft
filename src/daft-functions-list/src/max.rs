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
pub struct ListMax;

#[typetag::serde]
impl ScalarUDF for ListMax {
    fn name(&self) -> &'static str {
        "list_max"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        ensure!(inputs.len() == 1, ValueError: "Expected 1 input arg, got {}", inputs.len());
        let input = inputs.required((0, "input"))?;
        input.list_max()
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.len() == 1, SchemaMismatch: "Expected 1 input arg, got {}", inputs.len());
        let input = inputs.required((0, "input"))?;
        let field = input.to_field(schema)?.to_exploded_field()?;
        ensure!(field.dtype.is_numeric(), TypeError: "Expected input to be numeric, got {}", field.dtype);
        Ok(field)
    }
}

#[must_use]
pub fn list_max(expr: ExprRef) -> ExprRef {
    ScalarFunction::new(ListMax {}, vec![expr]).into()
}
