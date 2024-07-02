use daft_core::{datatypes::Field, schema::Schema, series::Series, DataType};

use crate::ExprRef;

use crate::functions::FunctionExpr;
use common_error::{DaftError, DaftResult};

use super::{super::FunctionEvaluator, BinaryExpr};

pub(super) struct UploadToFolderEvaluator {}

impl FunctionEvaluator for UploadToFolderEvaluator {
    fn fn_name(&self) -> &'static str {
        "upload_to_folder"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [data] => {
                let data_field = data.to_field(schema)?;
                match data_field.dtype {
                    DataType::Binary | DataType::FixedSizeBinary(..) | DataType::Utf8 => Ok(Field::new(data_field.name, DataType::Utf8)),
                    _ => Err(DaftError::TypeError(format!("Expects input to upload_to_folder to be Binary, FixedSizeBinary or String, but received {}", data_field))),
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], expr: &FunctionExpr) -> DaftResult<Series> {
        let (_folder_location, _io_config) = match expr {
            FunctionExpr::Binary(BinaryExpr::UploadToFolder {
                folder_location,
                io_config,
            }) => Ok((folder_location, io_config)),
            _ => Err(DaftError::ValueError(format!(
                "Expected an UploadToFolder expression but received {expr}"
            ))),
        }?;

        match inputs {
            [_data] => todo!("Implement uploading of data"),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
