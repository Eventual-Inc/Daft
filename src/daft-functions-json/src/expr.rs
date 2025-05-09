use common_error::{ensure, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    functions::{FunctionArgs, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::json_query_series;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct JsonQuery;

#[typetag::serde]
impl ScalarUDF for JsonQuery {
    #[allow(
        deprecated,
        reason = "temporary while transitioning to new scalarudf impl"
    )]
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        ensure!(inputs.len() == 2, ComputeError: "json_query requires two arguments: input and query");
        let input = inputs.required((0, "input"))?;
        let query = inputs.required((1, "query"))?;
        ensure!(query.len() == 1, ComputeError: "expected scalar value for query");
        let query = query.utf8()?.get(0).unwrap();

        json_query_series(input, query)
    }

    fn name(&self) -> &'static str {
        "json_query"
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.len() == 2, SchemaMismatch: "json_query requires two arguments: input and query");
        let input = inputs.required((0, "input"))?.to_field(schema)?;
        ensure!(input.dtype == DataType::Utf8, TypeError: "Input must be a string type");
        let query = inputs.required((1, "query"))?.to_field(schema)?;
        ensure!(query.dtype == DataType::Utf8, TypeError: "Query must be a string type");

        Ok(Field::new(input.name, DataType::Utf8))
    }

    fn docstring(&self) -> &'static str {
        "Extracts a JSON object from a JSON string using a JSONPath expression."
    }
}
