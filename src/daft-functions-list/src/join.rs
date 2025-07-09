use common_error::{ensure, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::series::SeriesListExtension;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListJoin;

#[typetag::serde]
impl ScalarUDF for ListJoin {
    fn name(&self) -> &'static str {
        "list_join"
    }
    fn aliases(&self) -> &'static [&'static str] {
        &["array_to_string"]
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let delimiter = inputs.required((1, "delimiter"))?;
        ensure!(
            delimiter.data_type().is_string(),
            "Expected join delimiter to be of type string"
        );

        Ok(input.join(delimiter.utf8()?)?.into_series())
    }
    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(
            inputs.len() == 2,
            SchemaMismatch: "Expected 2 arguments, received: {}",
            inputs.len()
        );
        let input_field = inputs.required((0, "input"))?.to_field(schema)?;
        let delimiter = inputs.required((1, "delimiter"))?.to_field(schema)?;

        ensure!(
            input_field.dtype.is_list() || input_field.dtype.is_fixed_size_list(),
            TypeError: "Expected input to be of type List, received: {}",
            input_field.dtype
        );

        ensure!(
            delimiter.dtype.is_string(),
            TypeError: "Expected join delimiter to be of type {}, received: {}",
            DataType::Utf8,
            delimiter.dtype
        );

        let exploded_field = input_field.to_exploded_field()?;
        ensure!(

            exploded_field.dtype.is_string(),
            TypeError: "Expected exploded input to be of type Utf8, received: {}",
            exploded_field.dtype
        );

        Ok(exploded_field)
    }
}

#[must_use]
pub fn list_join(expr: ExprRef, delim: ExprRef) -> ExprRef {
    ScalarFunction::new(ListJoin {}, vec![expr, delim]).into()
}
