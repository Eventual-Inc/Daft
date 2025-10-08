use std::{
    any::Any,
    hash::{Hash, Hasher},
    sync::Arc,
};

use common_daft_config::DaftExecutionConfig;
use common_display::DisplayAs;
use common_error::DaftResult;
use common_file_formats::FileFormatConfig;
use daft_dsl::ExprRef;
use daft_schema::schema::SchemaRef;
use serde::{Deserialize, Serialize};

use crate::{
    PartitionField, Pushdowns, ScanOperator, ScanTaskLike, ScanTaskLikeRef, SupportsPushdownFilters,
};

#[derive(Debug, Serialize, Deserialize, PartialEq, Hash)]
struct DummyScanTask {
    pub file_paths: Vec<String>,
    pub schema: SchemaRef,
    pub pushdowns: Pushdowns,
    pub num_rows: Option<usize>,
}

#[derive(Debug)]
pub struct DummyScanOperator {
    pub schema: SchemaRef,
    pub num_scan_tasks: u32,
    pub num_rows_per_task: Option<usize>,
    pub supports_count_pushdown_flag: bool,
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
        other.as_any().downcast_ref::<Self>() == Some(self)
    }

    fn dyn_hash(&self, mut state: &mut dyn Hasher) {
        self.hash(&mut state);
    }

    fn materialized_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn num_rows(&self) -> Option<usize> {
        self.num_rows
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

    fn get_file_paths(&self) -> Vec<String> {
        self.file_paths.clone()
    }
}

impl DisplayAs for DummyScanTask {
    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        format!(
            "DummyScanTask:
File Paths: [{file_paths}]
Schema: {schema}
Pushdowns: {pushdowns}
",
            file_paths = self.file_paths.join(", "),
            schema = self.schema,
            pushdowns = self.pushdowns.display_as(level)
        )
    }
}

impl ScanOperator for DummyScanOperator {
    fn name(&self) -> &'static str {
        "dummy"
    }
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

    fn can_absorb_shard(&self) -> bool {
        false
    }

    fn multiline_display(&self) -> Vec<String> {
        vec!["DummyScanOperator".to_string()]
    }

    fn supports_count_pushdown(&self) -> bool {
        self.supports_count_pushdown_flag
    }

    fn to_scan_tasks(&self, pushdowns: Pushdowns) -> DaftResult<Vec<ScanTaskLikeRef>> {
        Ok((0..self.num_scan_tasks)
            .map(|i| {
                let scan_task = Arc::new(DummyScanTask {
                    file_paths: vec![format!("dummy_file_{}.txt", i)],
                    schema: self.schema.clone(),
                    pushdowns: pushdowns.clone(),
                    num_rows: self.num_rows_per_task,
                });
                scan_task as Arc<dyn ScanTaskLike>
            })
            .collect())
    }

    fn as_pushdown_filter(&self) -> Option<&dyn SupportsPushdownFilters> {
        Some(self)
    }
}

impl SupportsPushdownFilters for DummyScanOperator {
    fn push_filters(&self, filters: &[ExprRef]) -> (Vec<ExprRef>, Vec<ExprRef>) {
        // Split predicates: those containing date functions are not pushable
        let (pushable, unpushable): (Vec<_>, Vec<_>) = filters
            .iter()
            .cloned()
            .partition(|expr| !contains_in_expression(expr));

        (pushable, unpushable)
    }
}

// Helper function to check for date function in expressions
fn contains_in_expression(expr: &ExprRef) -> bool {
    match expr.as_ref() {
        daft_dsl::Expr::IsIn { .. } => true,
        _ => expr.children().iter().any(contains_in_expression),
    }
}
