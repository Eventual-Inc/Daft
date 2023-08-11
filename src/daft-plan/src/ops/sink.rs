use std::sync::Arc;

use daft_core::schema::SchemaRef;

use crate::{
    sink_info::{OutputFileInfo, SinkInfo},
    LogicalPlan,
};

#[derive(Clone, Debug)]
pub struct Sink {
    pub schema: SchemaRef,
    /// Information about the sink data location.
    pub sink_info: Arc<SinkInfo>,

    // Upstream node.
    pub input: Arc<LogicalPlan>,
}

impl Sink {
    pub(crate) fn new(
        schema: SchemaRef,
        sink_info: Arc<SinkInfo>,
        input: Arc<LogicalPlan>,
    ) -> Self {
        Self {
            schema,
            sink_info,
            input,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];

        match self.sink_info.as_ref() {
            SinkInfo::OutputFileInfo(OutputFileInfo {
                root_dir,
                file_format,
                partition_cols,
                compression,
            }) => {
                res.push(format!("Sink: {:?}", file_format));
                if let Some(partition_cols) = partition_cols {
                    res.push(format!("  Partition cols: {:?}", partition_cols));
                }
                if let Some(compression) = compression {
                    res.push(format!("  Compression: {:?}", compression));
                }
                res.push(format!("  Root dir: {:?}", root_dir));
            }
        }
        res.push(format!("  Output schema: {}", self.schema.short_string()));
        res
    }
}
