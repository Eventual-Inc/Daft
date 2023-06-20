use crate::datatypes::{DataType, Field};

use crate::dsl::functions::FunctionExpr;
use crate::error::DaftError;
use crate::schema::Schema;
use crate::series::Series;
use crate::{
    dsl::{functions::FunctionEvaluator, Expr},
    error::DaftResult,
};

use super::UriExpr;

pub(super) struct DownloadEvaluator {}

impl FunctionEvaluator for DownloadEvaluator {
    fn fn_name(&self) -> &'static str {
        "download"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let field = input.to_field(schema)?;

                match &field.dtype {
                    DataType::Utf8 => Ok(Field::new(field.name, DataType::Binary)),
                    _ => Err(DaftError::TypeError(format!(
                        "Download can only download uris from Utf8Array, got {}",
                        field
                    ))),
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], expr: &Expr) -> DaftResult<Series> {
        let (max_connections, raise_error_on_failure, multi_thread) = match expr {
            Expr::Function {
                func:
                    FunctionExpr::Uri(UriExpr::Download {
                        max_connections,
                        raise_error_on_failure,
                        multi_thread,
                    }),
                inputs: _,
            } => (max_connections, raise_error_on_failure, multi_thread),
            _ => panic!("Expected Url Download Expr, got {expr}"),
        };

        match inputs {
            [input] => input.url_download(*max_connections, *raise_error_on_failure, *multi_thread),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
