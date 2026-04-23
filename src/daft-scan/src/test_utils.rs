use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_schema::schema::SchemaRef;
use daft_stats::TableMetadata;

use crate::{
    FileFormatConfig, PartitionField, Pushdowns, ScanOperator, ScanSource, ScanSourceKind,
    ScanTask, ScanTaskRef, SourceConfig, Statistics, SupportsPushdownFilters,
    storage_config::StorageConfig,
};

#[derive(Debug, Default)]
pub struct DummyScanOperator {
    pub schema: SchemaRef,
    pub num_scan_tasks: u32,
    pub num_rows_per_task: Option<usize>,
    pub supports_count_pushdown_flag: bool,
    pub stats: Option<Statistics>,
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

    fn to_scan_tasks(&self, pushdowns: Pushdowns) -> DaftResult<Vec<ScanTaskRef>> {
        Ok((0..self.num_scan_tasks)
            .map(|i| {
                let metadata = self.num_rows_per_task.map(|n| TableMetadata {
                    length: n,
                    size_bytes: None,
                });
                Arc::new(ScanTask::new(
                    vec![ScanSource {
                        size_bytes: None,
                        metadata,
                        statistics: None,
                        partition_spec: None,
                        kind: ScanSourceKind::File {
                            path: format!("dummy_file_{}.txt", i),
                            chunk_spec: None,
                            iceberg_delete_files: None,
                            parquet_metadata: None,
                        },
                    }],
                    Arc::new(SourceConfig::File(FileFormatConfig::Parquet(
                        Default::default(),
                    ))),
                    self.schema.clone(),
                    Arc::new(StorageConfig::default()),
                    pushdowns.clone(),
                    None,
                ))
            })
            .collect())
    }

    fn as_pushdown_filter(&self) -> Option<&dyn SupportsPushdownFilters> {
        Some(self)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.stats.clone()
    }
}

impl SupportsPushdownFilters for DummyScanOperator {
    fn push_filters(&self, filters: &[ExprRef]) -> (Vec<ExprRef>, Vec<ExprRef>) {
        // Split predicates: those containing IsIn expressions are not pushable
        let (pushable, unpushable): (Vec<_>, Vec<_>) = filters
            .iter()
            .cloned()
            .partition(|expr| !contains_in_expression(expr));

        (pushable, unpushable)
    }
}

// Helper function to check for IsIn expression
fn contains_in_expression(expr: &ExprRef) -> bool {
    match expr.as_ref() {
        daft_dsl::Expr::IsIn { .. } => true,
        _ => expr.children().iter().any(contains_in_expression),
    }
}
