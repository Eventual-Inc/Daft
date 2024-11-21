use std::{
    fmt::{Debug, Display},
    hash::{Hash, Hasher},
    sync::Arc,
};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use daft_schema::schema::SchemaRef;

use crate::{PartitionField, Pushdowns, ScanTaskLikeRef};

pub trait ScanOperator: Send + Sync + Debug {
    fn schema(&self) -> SchemaRef;
    fn partitioning_keys(&self) -> &[PartitionField];
    fn file_path_column(&self) -> Option<&str>;
    // Although generated fields are often added to the partition spec, generated fields and
    // partition fields are handled differently:
    // 1. Generated fields: Currently from file paths or Hive partitions,
    //    although in the future these may be extended to generated and virtual columns.
    // 2. Partition fields: Originally from Iceberg, specifying both source and (possibly
    //    transformed) partition fields.
    //
    // Partition fields are automatically included in scan output schemas (e.g.,
    // in ScanTask::materialized_schema), while generated fields require special handling.
    // Thus, we maintain separate representations for partitioning keys and generated fields.
    fn generated_fields(&self) -> Option<SchemaRef>;

    fn can_absorb_filter(&self) -> bool;
    fn can_absorb_select(&self) -> bool;
    fn can_absorb_limit(&self) -> bool;
    fn multiline_display(&self) -> Vec<String>;

    /// If cfg provided, `to_scan_tasks` should apply the appropriate transformations
    /// (merging, splitting) to the outputted scan tasks
    fn to_scan_tasks(
        &self,
        pushdowns: Pushdowns,
        config: Option<&DaftExecutionConfig>,
    ) -> DaftResult<Vec<ScanTaskLikeRef>>;
}

impl Display for dyn ScanOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.multiline_display().join("\n"))
    }
}

/// Light transparent wrapper around an Arc<dyn ScanOperator> that implements Eq/PartialEq/Hash
/// functionality to be performed on the **pointer** instead of on the value in the pointer.
///
/// This lets us get around having to implement full hashing/equality on [`ScanOperator`]`, which
/// is difficult because we sometimes have weird Python implementations that can be hard to check.
///
/// [`ScanOperatorRef`] should be thus held by structs that need to check the "sameness" of the
/// underlying ScanOperator instance, for example in the Scan nodes in a logical plan which need
/// to check for sameness of Scan nodes during plan optimization.
#[derive(Debug, Clone)]
pub struct ScanOperatorRef(pub Arc<dyn ScanOperator>);

impl Hash for ScanOperatorRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state);
    }
}

impl PartialEq<Self> for ScanOperatorRef {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl std::cmp::Eq for ScanOperatorRef {}

impl Display for ScanOperatorRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}
