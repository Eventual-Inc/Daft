use std::sync::Arc;

use common_display::DisplayAs;
use daft_algebra::boolean::split_conjunction;
use daft_dsl::{ExprRef, estimated_selectivity};
use daft_schema::schema::Schema;
use serde::{Deserialize, Serialize};

use crate::Sharder;

pub trait SupportsPushdownFilters {
    /// Applies filters to the scan operator and returns the pushable filters and the remaining filters.
    fn push_filters(&self, filter: &[ExprRef]) -> (Vec<ExprRef>, Vec<ExprRef>);
}

/// The precision with which a source can honor a pushdown.
///
/// - `Exact`: the source applies the pushdown with full fidelity; the optimizer
///   may drop the corresponding operator from above the scan.
/// - `Inexact`: the source may apply the pushdown as a best-effort filter
///   (e.g. row-group pruning), but callers must re-verify by keeping a residual
///   operator above the scan.
/// - `Unsupported`: the source cannot honor the pushdown; the optimizer must
///   keep the corresponding operator above the scan.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PushdownVerdict {
    Unsupported,
    Inexact,
    Exact,
}

impl PushdownVerdict {
    /// True when the source promises exact application.
    #[must_use]
    pub fn is_exact(self) -> bool {
        matches!(self, Self::Exact)
    }

    /// True when the source can apply the pushdown at all (Exact or Inexact).
    #[must_use]
    pub fn can_push(self) -> bool {
        matches!(self, Self::Exact | Self::Inexact)
    }

    /// True when the optimizer must keep the corresponding operator above the
    /// scan as a residual (Inexact or Unsupported).
    #[must_use]
    pub fn needs_residual(self) -> bool {
        matches!(self, Self::Inexact | Self::Unsupported)
    }
}

/// Declaration of how a source can honor the pushdowns presented by the optimizer.
///
/// Verdict vectors for `filters` / `partition_filters` are aligned with the
/// AND-conjuncts of `Pushdowns::filters` / `Pushdowns::partition_filters`
/// respectively, in the order produced by
/// [`daft_algebra::boolean::split_conjunction`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushdownCapability {
    /// One verdict per AND-conjunct of `Pushdowns::filters`.
    pub filters: Vec<PushdownVerdict>,
    /// One verdict per AND-conjunct of `Pushdowns::partition_filters`.
    pub partition_filters: Vec<PushdownVerdict>,
    pub projection: PushdownVerdict,
    pub limit: PushdownVerdict,
    pub shard: PushdownVerdict,
}

impl PushdownCapability {
    /// All-`Unsupported`, empty conjunct vectors. Safe default meaning
    /// "the optimizer must keep every operator above the scan."
    #[must_use]
    pub const fn none() -> Self {
        Self {
            filters: Vec::new(),
            partition_filters: Vec::new(),
            projection: PushdownVerdict::Unsupported,
            limit: PushdownVerdict::Unsupported,
            shard: PushdownVerdict::Unsupported,
        }
    }

    /// Returns `Exact` for every field populated on `pushdowns`, and
    /// `Unsupported` for every field that is `None`/empty. Useful as a starting
    /// point for impls that fully honor every pushdown.
    #[must_use]
    pub fn all_exact_for(pushdowns: &Pushdowns) -> Self {
        let filter_n = pushdowns
            .filters
            .as_ref()
            .map(|e| split_conjunction(e).len())
            .unwrap_or(0);
        let part_n = pushdowns
            .partition_filters
            .as_ref()
            .map(|e| split_conjunction(e).len())
            .unwrap_or(0);
        Self {
            filters: vec![PushdownVerdict::Exact; filter_n],
            partition_filters: vec![PushdownVerdict::Exact; part_n],
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
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Pushdowns {
    /// Optional filters to apply to the source data.
    pub filters: Option<ExprRef>,
    /// Optional filters to apply on partitioning keys.
    pub partition_filters: Option<ExprRef>,
    /// Optional columns to select from the source data.
    pub columns: Option<Arc<Vec<String>>>,
    /// Optional number of rows to read.
    pub limit: Option<usize>,
    /// Sharding information.
    pub sharder: Option<Sharder>,
    /// Optional filters that have been pushed down to the scan operator.
    /// The `filters` field is kept for backward compatibility;
    /// it represents all current filters.
    pub pushed_filters: Option<Vec<ExprRef>>,

    // /// Optional aggregation pushdown.
    /// This is used to indicate that the scan operator can perform an aggregation.
    /// This is useful for scans that can perform aggregations like `count`
    pub aggregation: Option<ExprRef>,
}

impl Pushdowns {
    #[must_use]
    pub fn new(
        filters: Option<ExprRef>,
        partition_filters: Option<ExprRef>,
        columns: Option<Arc<Vec<String>>>,
        limit: Option<usize>,
        sharder: Option<Sharder>,
        aggregation: Option<ExprRef>,
    ) -> Self {
        Self {
            filters,
            partition_filters,
            columns,
            limit,
            sharder,
            pushed_filters: None,
            aggregation,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.filters.is_none()
            && self.partition_filters.is_none()
            && self.columns.is_none()
            && self.limit.is_none()
    }

    #[must_use]
    pub fn with_limit(&self, limit: Option<usize>) -> Self {
        Self {
            limit,
            ..self.clone()
        }
    }

    #[must_use]
    pub fn with_filters(&self, filters: Option<ExprRef>) -> Self {
        Self {
            filters,
            ..self.clone()
        }
    }

    #[must_use]
    pub fn with_partition_filters(&self, partition_filters: Option<ExprRef>) -> Self {
        Self {
            partition_filters,
            ..self.clone()
        }
    }

    #[must_use]
    pub fn with_columns(&self, columns: Option<Arc<Vec<String>>>) -> Self {
        Self {
            columns,
            ..self.clone()
        }
    }

    #[must_use]
    pub fn with_sharder(&self, sharder: Option<Sharder>) -> Self {
        Self {
            sharder,
            ..self.clone()
        }
    }

    #[must_use]
    pub fn with_pushed_filters(&self, pushed_filters: Option<Vec<ExprRef>>) -> Self {
        Self {
            pushed_filters,
            ..self.clone()
        }
    }

    #[must_use]
    pub fn with_aggregation(&self, aggregation: Option<ExprRef>) -> Self {
        Self {
            aggregation,
            ..self.clone()
        }
    }

    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if let Some(columns) = &self.columns {
            res.push(format!("Projection pushdown = [{}]", columns.join(", ")));
        }
        if let Some(filters) = &self.filters {
            res.push(format!("Filter pushdown = {filters}"));
        }
        if let Some(pfilters) = &self.partition_filters {
            res.push(format!("Partition Filter = {pfilters}"));
        }
        if let Some(limit) = self.limit {
            res.push(format!("Limit pushdown = {limit}"));
        }
        if let Some(sharder) = &self.sharder {
            res.push(format!("Sharder = {sharder}"));
        }
        if let Some(aggregation) = &self.aggregation {
            res.push(format!("Aggregation pushdown = {aggregation}"));
        }
        res
    }

    pub fn estimated_selectivity(&self, schema: &Schema) -> f64 {
        if let Some(filters) = &self.filters {
            estimated_selectivity(filters, schema)
        } else {
            1.0
        }
    }
}

impl DisplayAs for Pushdowns {
    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        match level {
            common_display::DisplayLevel::Compact => {
                let mut s = String::new();
                s.push_str("Pushdowns: {");
                let mut sub_items = vec![];
                if let Some(columns) = &self.columns {
                    sub_items.push(format!("projection: [{}]", columns.join(", ")));
                }
                if let Some(filters) = &self.filters {
                    sub_items.push(format!("filter: {filters}"));
                }
                if let Some(pfilters) = &self.partition_filters {
                    sub_items.push(format!("partition_filter: {pfilters}"));
                }
                if let Some(limit) = self.limit {
                    sub_items.push(format!("limit: {limit}"));
                }
                if let Some(sharder) = &self.sharder {
                    sub_items.push(format!("sharder: {sharder}"));
                }
                if let Some(aggregation) = &self.aggregation {
                    sub_items.push(format!("aggregation: {aggregation}"));
                }
                s.push_str(&sub_items.join(", "));
                s.push('}');
                s
            }
            _ => self.multiline_display().join("\n"),
        }
    }
}
