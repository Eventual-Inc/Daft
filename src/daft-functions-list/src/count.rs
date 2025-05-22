use common_error::{ensure, DaftResult};
use daft_core::{
    prelude::{CountMode, DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarUDF},
    ExprRef, LiteralValue,
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
        let count_mode = inputs.optional((1, "mode"))?;
        let count_mode = match count_mode {
            Some(mode) => {
                if mode.data_type().is_null() {
                    CountMode::Valid
                } else {
                    ensure!(mode.len()==1, ValueError: "expected string literal");
                    let mode = mode.utf8()?.get(0).unwrap();
                    mode.parse()?
                }
            }
            None => CountMode::Valid,
        };

        Ok(input.list_count(count_mode)?.into_series())
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(!inputs.is_empty() && inputs.len() <=2, SchemaMismatch: "Expected 1 or 2 input args, got {}", inputs.len());

        let input_field = inputs.required((0, "input"))?.to_field(schema)?;
        if let Some(mode) = inputs.optional((1, "mode"))? {
            let is_str_or_null = mode
                .as_literal()
                .map(|lit| matches!(lit, LiteralValue::Utf8(_) | LiteralValue::Null))
                .is_some_and(|b| b);

            ensure!(is_str_or_null, TypeError: "expected string literal");
        }

        Ok(Field::new(input_field.name, DataType::UInt64))
    }
}
