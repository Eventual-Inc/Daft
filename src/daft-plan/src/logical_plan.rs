use daft_core::schema::SchemaRef;

use crate::ops::*;

#[derive(Clone)]
pub enum LogicalPlan {
    Source(Source),
}

impl LogicalPlan {
    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Source(Source { schema, .. }) => schema.clone(),
        }
    }
}
