use daft_core::{datatypes::Field, schema::Schema, series::Series, DataType};
use daft_io::url_upload;

use crate::ExprRef;

use crate::functions::FunctionExpr;
use common_error::{DaftError, DaftResult};

use super::{super::FunctionEvaluator, UriExpr};

pub(super) struct UploadEvaluator {}

impl FunctionEvaluator for UploadEvaluator {
    fn fn_name(&self) -> &'static str {
        "upload"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [data] => {
                let data_field = data.to_field(schema)?;
                match data_field.dtype {
                    DataType::Binary | DataType::FixedSizeBinary(..) | DataType::Utf8 => Ok(Field::new(data_field.name, DataType::Utf8)),
                    _ => Err(DaftError::TypeError(format!("Expects input to url_upload to be Binary, FixedSizeBinary or String, but received {}", data_field))),
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], expr: &FunctionExpr) -> DaftResult<Series> {
        let (location, io_config, max_connections, multi_thread) = match expr {
            FunctionExpr::Uri(UriExpr::Upload {
                location,
                config,
                max_connections,
                multi_thread,
            }) => Ok((location, config, max_connections, multi_thread)),
            _ => Err(DaftError::ValueError(format!(
                "Expected an Upload expression but received {expr}"
            ))),
        }?;

        match inputs {
            [data] => url_upload(
                data,
                location,
                *max_connections,
                *multi_thread,
                io_config.clone(),
                None,
            ),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
