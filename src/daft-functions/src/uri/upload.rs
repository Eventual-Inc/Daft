use std::sync::Arc;

use daft_core::datatypes::Field;
use daft_core::DataType;
use daft_dsl::functions::ScalarUDF;
use daft_dsl::ExprRef;
use daft_io::{url_upload, IOConfig};
use serde::Serialize;

use common_error::{DaftError, DaftResult};
use daft_core::schema::Schema;
use daft_core::series::Series;

#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub(super) struct UploadFunction {
    pub(super) location: String,
    pub(super) max_connections: usize,
    pub(super) multi_thread: bool,
    pub(super) config: Arc<IOConfig>,
}

#[typetag::serde]
impl ScalarUDF for UploadFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "upload"
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        let UploadFunction {
            location,
            config,
            max_connections,
            multi_thread,
        } = self;

        match inputs {
            [data] => url_upload(
                data,
                location,
                *max_connections,
                *multi_thread,
                config.clone(),
                None,
            ),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
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
}
