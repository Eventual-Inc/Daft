use common_error::{DaftResult, ensure};
use daft_core::{
    datatypes::try_mean_aggregation_supertype,
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
pub struct ListMean;

#[typetag::serde]
impl ScalarUDF for ListMean {
    fn name(&self) -> &'static str {
        "list_mean"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        ensure!(inputs.len() == 1, ValueError: "Expected 1 input arg, got {}", inputs.len());
        let input = inputs.required((0, "input"))?;
        input.list_mean()
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.len() == 1, SchemaMismatch: "Expected 1 input arg, got {}", inputs.len());
        let input = inputs.required((0, "input"))?;
        let inner_field = input.to_field(schema)?.to_exploded_field()?;
        Ok(Field::new(
            inner_field.name.as_str(),
            try_mean_aggregation_supertype(&inner_field.dtype)?,
        ))
    }
}

#[must_use]
pub fn list_mean(expr: ExprRef) -> ExprRef {
    ScalarFn::builtin(ListMean {}, vec![expr]).into()
}
