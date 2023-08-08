use std::sync::Arc;

use daft_core::schema::SchemaRef;

use crate::{ops::*, PartitionSpec};

#[derive(Clone, Debug)]
pub enum LogicalPlan {
    Source(Source),
    Filter(Filter),
    Limit(Limit),
}

impl LogicalPlan {
    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Source(Source { schema, .. }) => schema.clone(),
            Self::Filter(Filter { input, .. }) => input.schema(),
            Self::Limit(Limit { input, .. }) => input.schema(),
        }
    }

    pub fn partition_spec(&self) -> Arc<PartitionSpec> {
        match self {
            Self::Source(Source { partition_spec, .. }) => partition_spec.clone(),
            Self::Filter(Filter { input, .. }) => input.partition_spec(),
            Self::Limit(Limit { input, .. }) => input.partition_spec(),
        }
    }

    pub fn children(&self) -> Vec<&Self> {
        match self {
            Self::Source(..) => vec![],
            Self::Filter(filter) => vec![&filter.input],
            Self::Limit(limit) => vec![&limit.input],
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        match self {
            Self::Source(source) => source.multiline_display(),
            Self::Filter(Filter { predicate, .. }) => vec![format!("Filter: {predicate}")],
            Self::Limit(Limit { limit, .. }) => vec![format!("Limit: {limit}")],
        }
    }

    pub fn repr_ascii(&self) -> String {
        let mut s = String::new();
        crate::display::TreeDisplay::fmt_tree(self, &mut s).unwrap();
        s
    }
}
