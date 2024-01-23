use std::sync::Arc;

use common_error::DaftResult;
use daft_core::{
    datatypes::Field,
    schema::{Schema, SchemaRef},
};

use crate::{
    sink_info::{OutputFileInfo, SinkInfo},
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Sink {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub schema: SchemaRef,
    /// Information about the sink data location.
    pub sink_info: Arc<SinkInfo>,
}

impl Sink {
    pub(crate) fn try_new(input: Arc<LogicalPlan>, sink_info: Arc<SinkInfo>) -> DaftResult<Self> {
        let mut fields = vec![Field::new("path", daft_core::DataType::Utf8)];
        let schema = input.schema();

        match sink_info.as_ref() {
            SinkInfo::OutputFileInfo(output_file_info) => {
                if let Some(ref pcols) = output_file_info.partition_cols {
                    for pc in pcols {
                        fields.push(pc.to_field(&schema)?);
                    }
                }
            }
        }
        let schema = Schema::new(fields)?.into();
        Ok(Self {
            input,
            schema,
            sink_info,
        })
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];

        match self.sink_info.as_ref() {
            SinkInfo::OutputFileInfo(OutputFileInfo {
                root_dir,
                file_format,
                partition_cols,
                compression,
                io_config,
            }) => {
                res.push(format!("Sink: {:?}", file_format));
                if let Some(partition_cols) = partition_cols {
                    res.push(format!(
                        "Partition cols = {}",
                        partition_cols
                            .iter()
                            .map(|e| e.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
                if let Some(compression) = compression {
                    res.push(format!("Compression = {}", compression));
                }
                res.push(format!("Root dir = {}", root_dir));
                match io_config {
                    None => res.push("IOConfig = None".to_string()),
                    Some(io_config) => res.push(format!("IOConfig = {}", io_config)),
                }
            }
        }
        res.push(format!("Output schema = {}", self.schema.short_string()));
        res
    }
}
