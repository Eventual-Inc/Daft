use std::{
    fmt::{Debug, Display},
    hash::{Hash, Hasher},
    sync::Arc,
};

use common_error::DaftResult;
use daft_algebra::boolean::split_conjunction;
use daft_schema::schema::SchemaRef;

use crate::{
    PartitionField, PushdownCapability, PushdownVerdict, Pushdowns, ScanTaskRef, Statistics,
    SupportsPushdownFilters,
};

pub trait ScanOperator: Send + Sync + Debug {
    fn name(&self) -> &str;

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

    #[deprecated(note = "override supports_pushdowns instead")]
    fn can_absorb_filter(&self) -> bool;
    #[deprecated(note = "override supports_pushdowns instead")]
    fn can_absorb_select(&self) -> bool;
    #[deprecated(note = "override supports_pushdowns instead")]
    fn can_absorb_limit(&self) -> bool;
    #[deprecated(note = "override supports_pushdowns instead")]
    fn can_absorb_shard(&self) -> bool;
    fn multiline_display(&self) -> Vec<String>;

    /// Declares how this source can honor the given pushdowns.
    ///
    /// Returns per-conjunct verdicts for filter-style pushdowns and single
    /// verdicts for projection / limit / shard. The default adapter derives a
    /// conservative capability from the legacy `can_absorb_*` booleans and
    /// (if present) `as_pushdown_filter()`, preserving today's behavior for
    /// every source that does not override this method.
    ///
    /// Sources should override this method rather than the deprecated boolean
    /// methods. Returning `Inexact` for a filter verdict tells the optimizer
    /// to push the predicate *and* keep a residual filter above the scan, so
    /// partial-pushdown correctness is no longer the source author's burden.
    #[allow(deprecated)]
    fn supports_pushdowns(&self, pushdowns: &Pushdowns) -> PushdownCapability {
        let filter_conjuncts = pushdowns
            .filters
            .as_ref()
            .map(split_conjunction)
            .unwrap_or_default();
        let part_conjuncts = pushdowns
            .partition_filters
            .as_ref()
            .map(split_conjunction)
            .unwrap_or_default();

        // Preserve SupportsPushdownFilters behavior: the split it declares
        // (pushed, residual) maps to (Exact, Unsupported) per conjunct.
        let filter_verdicts = if let Some(splitter) = self.as_pushdown_filter() {
            let (pushed, _residual) = splitter.push_filters(&filter_conjuncts);
            let pushed_set: std::collections::HashSet<&daft_dsl::ExprRef> = pushed.iter().collect();
            filter_conjuncts
                .iter()
                .map(|e| {
                    if pushed_set.contains(e) {
                        PushdownVerdict::Exact
                    } else {
                        PushdownVerdict::Unsupported
                    }
                })
                .collect()
        } else if self.can_absorb_filter() {
            vec![PushdownVerdict::Exact; filter_conjuncts.len()]
        } else {
            vec![PushdownVerdict::Unsupported; filter_conjuncts.len()]
        };

        PushdownCapability {
            filters: filter_verdicts,
            // Partition filters are treated as exact today (sources evaluate
            // them on partition metadata directly). Preserve that.
            partition_filters: vec![PushdownVerdict::Exact; part_conjuncts.len()],
            // Projection and shard are unconditionally pushed today, regardless
            // of the legacy `can_absorb_select` / `can_absorb_shard` booleans
            // (which no rule consults). Keep that behavior by keying off the
            // presence of the field on `Pushdowns` rather than the dead
            // booleans.
            projection: if pushdowns.columns.is_some() {
                PushdownVerdict::Exact
            } else {
                PushdownVerdict::Unsupported
            },
            limit: if pushdowns.limit.is_some() && self.can_absorb_limit() {
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

    fn supports_count_pushdown(&self) -> bool {
        false
    }

    /// Pre-computed statistics for this source, if known cheaply (e.g. from
    /// manifest metadata). The optimizer populates `PlanStats` from this when
    /// available, skipping per-scan-task aggregation. Returning `None` (the
    /// default) preserves today's behavior of summing stats from scan tasks.
    fn statistics(&self) -> Option<Statistics> {
        None
    }

    fn supported_count_modes(&self) -> Vec<daft_core::count_mode::CountMode> {
        Vec::new()
    }

    /// If cfg provided, `to_scan_tasks` should apply the appropriate transformations
    /// (merging, splitting) to the outputted scan tasks
    fn to_scan_tasks(&self, pushdowns: Pushdowns) -> DaftResult<Vec<ScanTaskRef>>;

    fn as_pushdown_filter(&self) -> Option<&dyn SupportsPushdownFilters> {
        None
    }
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
