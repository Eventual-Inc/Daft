use std::sync::Arc;

use common_display::DisplayAs;
use daft_dsl::ExprRef;
use serde::{Deserialize, Serialize};

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
}

impl Default for Pushdowns {
    fn default() -> Self {
        Self::new(None, None, None, None)
    }
}

impl Pushdowns {
    #[must_use]
    pub fn new(
        filters: Option<ExprRef>,
        partition_filters: Option<ExprRef>,
        columns: Option<Arc<Vec<String>>>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            filters,
            partition_filters,
            columns,
            limit,
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
        }
    }

    #[must_use]
    pub fn with_filters(&self, filters: Option<ExprRef>) -> Self {
        Self {
            filters,
            partition_filters: self.partition_filters.clone(),
            columns: self.columns.clone(),
            limit: self.limit,
        }
    }

    #[must_use]
    pub fn with_partition_filters(&self, partition_filters: Option<ExprRef>) -> Self {
        Self {
            filters: self.filters.clone(),
            partition_filters,
            columns: self.columns.clone(),
            limit: self.limit,
        }
    }

    #[must_use]
    pub fn with_columns(&self, columns: Option<Arc<Vec<String>>>) -> Self {
        Self {
            filters: self.filters.clone(),
            partition_filters: self.partition_filters.clone(),
            columns,
            limit: self.limit,
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
        res
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
                s.push_str(&sub_items.join(", "));
                s.push('}');
                s
            }
            _ => self.multiline_display().join("\n"),
        }
    }
}
