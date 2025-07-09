use common_error::DaftResult;
use daft_core::{
    prelude::{CountMode, DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::series::SeriesListExtension;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListCount;

#[derive(FunctionArgs)]
struct ListCountArgs<T> {
    input: T,
    #[arg(optional)]
    mode: Option<CountMode>,
}

#[typetag::serde]
impl ScalarUDF for ListCount {
    fn name(&self) -> &'static str {
        "list_count"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let ListCountArgs { input, mode } = inputs.try_into()?;
        let mode = mode.unwrap_or(CountMode::Valid);

        Ok(input.list_count(mode)?.into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let ListCountArgs { input, .. } = inputs.try_into()?;

        let input_field = input.to_field(schema)?;
        Ok(Field::new(input_field.name, DataType::UInt64))
    }
}
