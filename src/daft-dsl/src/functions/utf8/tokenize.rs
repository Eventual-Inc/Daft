use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};
use daft_tokenize::series::{tokenize_decode, tokenize_encode};

use crate::functions::FunctionExpr;
use crate::ExprRef;
use common_error::{DaftError, DaftResult};

use super::{super::FunctionEvaluator, Utf8Expr};

pub(super) struct TokenizeEncodeEvaluator {}

impl FunctionEvaluator for TokenizeEncodeEvaluator {
    fn fn_name(&self) -> &'static str {
        "tokenize_encode"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
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
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], expr: &FunctionExpr) -> DaftResult<Series> {
        match inputs {
            [data] => {
                let tokens_path = match expr {
                    FunctionExpr::Utf8(Utf8Expr::TokenizeEncode(tokens_path)) => tokens_path,
                    _ => panic!("Expected TokenizeEncode Expr, got {expr}"),
                };
                tokenize_encode(data, tokens_path)
            }
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

pub(super) struct TokenizeDecodeEvaluator {}

impl FunctionEvaluator for TokenizeDecodeEvaluator {
    fn fn_name(&self) -> &'static str {
        "tokenize_decode"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
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
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], expr: &FunctionExpr) -> DaftResult<Series> {
        match inputs {
            [data] => {
                let tokens_path = match expr {
                    FunctionExpr::Utf8(Utf8Expr::TokenizeDecode(tokens_path)) => tokens_path,
                    _ => panic!("Expected TokenizeDecode Expr, got {expr}"),
                };
                tokenize_decode(data, tokens_path)
            }
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
