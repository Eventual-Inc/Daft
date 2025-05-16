use common_error::{ensure, DaftError, DaftResult};
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
pub struct ListGet;

#[typetag::serde]
impl ScalarUDF for ListGet {
    fn name(&self) -> &'static str {
        "list_get"
    }

    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let idx = inputs.required((1, "index"))?;
        let _default = inputs.required((2, "default"))?;
        input.list_get(idx, _default)
    }

    fn function_args_to_field(
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
        let idx = inputs.required(1)?.to_field(schema)?;
        let _default = inputs.required(2)?.to_field(schema)?;

        ensure!(
            input.dtype.is_list() || input.dtype.is_fixed_size_list(),
            "Input must be a list"
        );

        ensure!(
            idx.dtype.is_integer(),
            TypeError: "Index must be an integer, received: {}",
            idx.dtype
        );

        // TODO(Kevin): Check if default dtype can be cast into input dtype.
        let exploded_field = input.to_exploded_field()?;
        Ok(exploded_field)
    }

    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [input, idx, default] => Ok(input.list_get(idx, default)?),
            _ => Err(DaftError::ValueError(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn list_get(expr: ExprRef, idx: ExprRef, default_value: ExprRef) -> ExprRef {
    ScalarFunction::new(ListGet {}, vec![expr, idx, default_value]).into()
}
