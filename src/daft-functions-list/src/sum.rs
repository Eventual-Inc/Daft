use common_error::{DaftResult, ensure};
use daft_core::{
    datatypes::try_sum_supertype,
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
pub struct ListSum;

#[typetag::serde]
impl ScalarUDF for ListSum {
    fn name(&self) -> &'static str {
        "list_sum"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        ensure!(inputs.len() == 1, ValueError: "Expected 1 input arg, got {}", inputs.len());
        let input = inputs.required((0, "input"))?;
        input.list_sum()
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.len() == 1, SchemaMismatch: "Expected 1 input arg, got {}", inputs.len());
        let input = inputs.required((0, "input"))?;
        let mut field = input.to_field(schema)?.to_exploded_field()?;
        ensure!(field.dtype.is_numeric(), TypeError: "Expected input to be numeric, got {}", field.dtype);
        field.dtype = try_sum_supertype(&field.dtype)?;
        Ok(field)
    }
}

#[must_use]
pub fn list_sum(expr: ExprRef) -> ExprRef {
    ScalarFn::builtin(ListSum {}, vec![expr]).into()
}
