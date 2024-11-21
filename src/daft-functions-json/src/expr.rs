// use crate::functions::FunctionExpr;
use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{functions::ScalarUDF, ExprRef};
use serde::{Deserialize, Serialize};

use crate::json_query_series;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct JsonQuery {
    pub query: String,
}

#[typetag::serde]
impl ScalarUDF for JsonQuery {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &'static str {
        "json_query"
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [input] => json_query_series(input, &self.query),

            _ => Err(DaftError::TypeError(
                "Json query expects a single argument".to_string(),
            )),
        }
    }
    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let input_field = input.to_field(schema)?;
                match input_field.dtype {
                    DataType::Utf8 => Ok(Field::new(input_field.name, DataType::Utf8)),
                    _ => Err(DaftError::TypeError(format!(
                        "Expected input to be a string type, received: {}",
                        input_field.dtype
                    ))),
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
