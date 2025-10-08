use std::sync::Arc;

use common_display::DisplayAs;
use daft_dsl::{ExprRef, estimated_selectivity};
use daft_schema::schema::Schema;
use serde::{Deserialize, Serialize};

use crate::Sharder;

pub trait SupportsPushdownFilters {
    /// Applies filters to the scan operator and returns the pushable filters and the remaining filters.
    fn push_filters(&self, filter: &[ExprRef]) -> (Vec<ExprRef>, Vec<ExprRef>);
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
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

impl Default for Pushdowns {
    fn default() -> Self {
        Self::new(None, None, None, None, None, None)
    }
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
            filters: self.filters.clone(),
            partition_filters: self.partition_filters.clone(),
            columns: self.columns.clone(),
            limit,
            sharder: self.sharder.clone(),
            pushed_filters: self.pushed_filters.clone(),
            aggregation: self.aggregation.clone(),
        }
    }

    #[must_use]
    pub fn with_filters(&self, filters: Option<ExprRef>) -> Self {
        Self {
            filters,
            partition_filters: self.partition_filters.clone(),
            columns: self.columns.clone(),
            limit: self.limit,
            sharder: self.sharder.clone(),
            pushed_filters: self.pushed_filters.clone(),
            aggregation: self.aggregation.clone(),
        }
    }

    #[must_use]
    pub fn with_partition_filters(&self, partition_filters: Option<ExprRef>) -> Self {
        Self {
            filters: self.filters.clone(),
            partition_filters,
            columns: self.columns.clone(),
            limit: self.limit,
            sharder: self.sharder.clone(),
            pushed_filters: self.pushed_filters.clone(),
            aggregation: self.aggregation.clone(),
        }
    }

    #[must_use]
    pub fn with_columns(&self, columns: Option<Arc<Vec<String>>>) -> Self {
        Self {
            filters: self.filters.clone(),
            partition_filters: self.partition_filters.clone(),
            columns,
            limit: self.limit,
            sharder: self.sharder.clone(),
            pushed_filters: self.pushed_filters.clone(),
            aggregation: self.aggregation.clone(),
        }
    }

    #[must_use]
    pub fn with_sharder(&self, sharder: Option<Sharder>) -> Self {
        Self {
            filters: self.filters.clone(),
            partition_filters: self.partition_filters.clone(),
            columns: self.columns.clone(),
            limit: self.limit,
            sharder,
            pushed_filters: self.pushed_filters.clone(),
            aggregation: self.aggregation.clone(),
        }
    }

    #[must_use]
    pub fn with_pushed_filters(&self, pushed_filters: Option<Vec<ExprRef>>) -> Self {
        Self {
            filters: self.filters.clone(),
            partition_filters: self.partition_filters.clone(),
            columns: self.columns.clone(),
            limit: self.limit,
            sharder: self.sharder.clone(),
            pushed_filters,
            aggregation: self.aggregation.clone(),
        }
    }

    #[must_use]
    pub fn with_aggregation(&self, aggregation: Option<ExprRef>) -> Self {
        Self {
            filters: self.filters.clone(),
            partition_filters: self.partition_filters.clone(),
            columns: self.columns.clone(),
            limit: self.limit,
            sharder: self.sharder.clone(),
            pushed_filters: self.pushed_filters.clone(),
            aggregation,
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
