use std::sync::Arc;

use daft_core::datatypes::Field;
use daft_core::DataType;
use daft_dsl::functions::ScalarUDF;
use daft_dsl::ExprRef;
use daft_io::{url_download, IOConfig};
use serde::Serialize;

use common_error::{DaftError, DaftResult};
use daft_core::schema::Schema;
use daft_core::series::Series;

#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub(super) struct DownloadFunction {
    pub(super) max_connections: usize,
    pub(super) raise_error_on_failure: bool,
    pub(super) multi_thread: bool,
    pub(super) config: Arc<IOConfig>,
}

#[typetag::serde]
impl ScalarUDF for DownloadFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "download"
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        let DownloadFunction {
            max_connections,
            raise_error_on_failure,
            multi_thread,
            config,
        } = self;

        match inputs {
            [input] => url_download(
                input,
                *max_connections,
                *raise_error_on_failure,
                *multi_thread,
                config.clone(),
                None,
            ),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
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
}
