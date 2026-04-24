use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_schema::schema::SchemaRef;
use daft_stats::TableMetadata;

use crate::{
    FileFormatConfig, PartitionField, PushdownCapability, PushdownVerdict, Pushdowns, ScanOperator,
    ScanSource, ScanSourceKind, ScanTask, ScanTaskRef, SourceConfig, Statistics,
    SupportsPushdownFilters, storage_config::StorageConfig,
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
                let metadata = self.num_rows_per_task.map(|n| TableMetadata { length: n });
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

/// Test-only scan operator that returns a caller-specified `PushdownCapability`.
///
/// Useful for exercising optimizer rules against Exact/Inexact/Unsupported
/// verdicts without having to stand up a real source implementation. Projection,
/// limit, and shard default to `Exact` when the corresponding `Pushdowns` field
/// is populated (matching the adapter's conservative behavior); callers control
/// the filter-conjunct verdicts directly.
#[derive(Debug, Default)]
pub struct VerdictScanOperator {
    pub schema: SchemaRef,
    pub num_scan_tasks: u32,
    /// One verdict per AND-conjunct of the incoming filter pushdown, in order.
    /// Empty means "no filter verdicts" (every conjunct falls through to
    /// `Unsupported` via the rule's missing-verdict fallback).
    pub filter_verdicts: Vec<PushdownVerdict>,
}

impl ScanOperator for VerdictScanOperator {
    fn name(&self) -> &'static str {
        "verdict"
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
        vec!["VerdictScanOperator".to_string()]
    }

    fn supports_pushdowns(&self, pushdowns: &Pushdowns) -> PushdownCapability {
        PushdownCapability {
            filters: self.filter_verdicts.clone(),
            partition_filters: Vec::new(),
            projection: if pushdowns.columns.is_some() {
                PushdownVerdict::Exact
            } else {
                PushdownVerdict::Unsupported
            },
            limit: if pushdowns.limit.is_some() {
                PushdownVerdict::Exact
            } else {
                PushdownVerdict::Unsupported
            },
            shard: if pushdowns.sharder.is_some() {
                PushdownVerdict::Exact
            } else {
                PushdownVerdict::Unsupported
            },
        }
    }

    fn to_scan_tasks(&self, pushdowns: Pushdowns) -> DaftResult<Vec<ScanTaskRef>> {
        Ok((0..self.num_scan_tasks)
            .map(|i| {
                Arc::new(ScanTask::new(
                    vec![ScanSource {
                        size_bytes: None,
                        metadata: None,
                        statistics: None,
                        partition_spec: None,
                        kind: ScanSourceKind::File {
                            path: format!("verdict_file_{}.txt", i),
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
}
