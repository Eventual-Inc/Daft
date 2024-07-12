use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::Field, schema::Schema, DataType, Series};
use daft_dsl::{functions::ScalarUDF, ExprRef};
use serde::Serialize;

use super::series::{tokenize_decode, tokenize_encode};

#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub(super) struct TokenizeEncodeFunction {
    pub(super) tokens_path: String,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub(super) struct TokenizeDecodeFunction {
    pub(super) tokens_path: String,
}

#[typetag::serde]
impl ScalarUDF for TokenizeEncodeFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "tokenize_encode"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data] => match data.to_field(schema) {
                Ok(data_field) => match &data_field.dtype {
                    DataType::Utf8 => Ok(Field::new(
                        data_field.name,
                        DataType::List(Box::new(DataType::UInt32)),
                    )),
                    _ => Err(DaftError::TypeError(format!(
                        "Expects input to tokenize_encode to be utf8, but received {data_field}",
                    ))),
                },
                Err(e) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [data] => tokenize_encode(data, &self.tokens_path),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

#[typetag::serde]
impl ScalarUDF for TokenizeDecodeFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "tokenize_decode"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data] => match data.to_field(schema) {
                Ok(data_field) => match &data_field.dtype {
                    DataType::List(inner) if inner.is_integer() => {
                        Ok(Field::new(data_field.name, DataType::Utf8))
                    }
                    _ => Err(DaftError::TypeError(format!(
                        "Expected input to tokenize_decode to be list[integer], but received {data_field}",
                    ))),
                },
                Err(e) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [data] => tokenize_decode(data, &self.tokens_path),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
