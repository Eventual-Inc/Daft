use daft_logical_plan::sink_info::DataSinkInfo;
use daft_schema::schema::SchemaRef;
use serde::{Deserialize, Serialize};

use crate::PhysicalPlanRef;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DataSink {
    pub schema: SchemaRef,
    pub data_sink_info: DataSinkInfo,
    // Upstream node.
    pub input: PhysicalPlanRef,
}

impl DataSink {
    pub(crate) fn new(
        schema: SchemaRef,
        data_sink_info: DataSinkInfo,
        input: PhysicalPlanRef,
    ) -> Self {
        Self {
            schema,
            data_sink_info,
            input,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("DataSink:".to_string());
        res.extend(self.data_sink_info.multiline_display());
        res
    }
}

crate::impl_default_tree_display!(DataSink);
