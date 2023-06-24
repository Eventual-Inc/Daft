use daft_core::datatypes::{DataType, Field};
use daft_io::url_download;

use crate::functions::FunctionExpr;
use crate::{functions::FunctionEvaluator, Expr};
use common_error::DaftError;
use common_error::DaftResult;
use daft_core::schema::Schema;
use daft_core::series::Series;

use super::UriExpr;

pub(super) struct DownloadEvaluator {}

impl FunctionEvaluator for DownloadEvaluator {
    fn fn_name(&self) -> &'static str {
        "download"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema, _expr: &Expr) -> DaftResult<Field> {
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
        let (max_connections, raise_error_on_failure, multi_thread, config) = match expr {
            Expr::Function {
                func:
                    FunctionExpr::Uri(UriExpr::Download {
                        max_connections,
                        raise_error_on_failure,
                        multi_thread,
                        config,
                    }),
                inputs: _,
            } => (
                max_connections,
                raise_error_on_failure,
                multi_thread,
                config,
            ),
            _ => panic!("Expected Url Download Expr, got {expr}"),
        };

        match inputs {
            [input] => url_download(
                input,
                *max_connections,
                *raise_error_on_failure,
                *multi_thread,
                config.clone(),
            ),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
