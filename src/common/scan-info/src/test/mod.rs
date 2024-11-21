use std::{any::Any, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_display::DisplayAs;
use common_error::DaftResult;
use common_file_formats::FileFormatConfig;
use daft_schema::schema::SchemaRef;
use serde::{Deserialize, Serialize};

use crate::{PartitionField, Pushdowns, ScanOperator, ScanTaskLike, ScanTaskLikeRef};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct DummyScanTask {
    pub schema: SchemaRef,
    pub pushdowns: Pushdowns,
}

#[derive(Debug)]
pub struct DummyScanOperator {
    pub schema: SchemaRef,
    pub num_scan_tasks: u32,
}

#[typetag::serde]
impl ScanTaskLike for DummyScanTask {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }

    fn dyn_eq(&self, other: &dyn ScanTaskLike) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .map_or(false, |a| a == self)
    }

    fn materialized_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn num_rows(&self) -> Option<usize> {
        None
    }

    fn approx_num_rows(&self, _: Option<&DaftExecutionConfig>) -> Option<f64> {
        None
    }

    fn upper_bound_rows(&self) -> Option<usize> {
        None
    }

    fn size_bytes_on_disk(&self) -> Option<usize> {
        None
    }

    fn estimate_in_memory_size_bytes(&self, _: Option<&DaftExecutionConfig>) -> Option<usize> {
        None
    }

    fn file_format_config(&self) -> Arc<FileFormatConfig> {
        FileFormatConfig::Json(Default::default()).into()
    }

    fn pushdowns(&self) -> &Pushdowns {
        &self.pushdowns
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl DisplayAs for DummyScanTask {
    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        format!(
            "DummyScanTask:
Schema: {schema}
Pushdowns: {pushdowns}
",
            schema = self.schema,
            pushdowns = self.pushdowns.display_as(level)
        )
    }
}

impl ScanOperator for DummyScanOperator {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn partitioning_keys(&self) -> &[PartitionField] {
        &[]
    }

    fn file_path_column(&self) -> Option<&str> {
        None
    }

    fn generated_fields(&self) -> Option<SchemaRef> {
        None
    }

    fn can_absorb_filter(&self) -> bool {
        false
    }

    fn can_absorb_select(&self) -> bool {
        false
    }

    fn can_absorb_limit(&self) -> bool {
        false
    }

    fn multiline_display(&self) -> Vec<String> {
        vec!["DummyScanOperator".to_string()]
    }

    fn to_scan_tasks(
        &self,
        pushdowns: Pushdowns,
        _: Option<&DaftExecutionConfig>,
    ) -> DaftResult<Vec<ScanTaskLikeRef>> {
        let scan_task = Arc::new(DummyScanTask {
            schema: self.schema.clone(),
            pushdowns,
        });

        Ok((0..self.num_scan_tasks)
            .map(|_| scan_task.clone() as Arc<dyn ScanTaskLike>)
            .collect())
    }
}
