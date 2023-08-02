use daft_core::schema::SchemaRef;

use crate::ops::*;

#[derive(Clone, Debug)]
pub enum LogicalPlan {
    Source(Source),
    Filter(Filter),
}

impl LogicalPlan {
    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Source(Source { schema, .. }) => schema.clone(),
            Self::Filter(Filter { input, .. }) => input.schema(),
        }
    }
}
