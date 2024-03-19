use std::sync::Arc;

use daft_core::{
    datatypes::Field,
    schema::{Schema, SchemaRef},
    DataType,
};
use lazy_static::lazy_static;

use crate::LogicalPlan;

lazy_static! {
    pub static ref COUNT_SCHEMA: SchemaRef =
        Arc::new(Schema::new(vec![Field::new("count", DataType::UInt64)]).unwrap());
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Count {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
}

impl Count {
    pub(crate) fn new(input: Arc<LogicalPlan>) -> Self {
        Self { input }
    }
}
