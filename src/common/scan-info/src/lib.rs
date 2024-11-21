#![feature(let_chains)]
#![feature(if_let_guard)]

mod expr_rewriter;
mod partitioning;
mod pushdowns;
#[cfg(feature = "python")]
pub mod python;
mod scan_operator;
mod scan_task;
pub mod test;

use std::{fmt::Debug, hash::Hash};

use daft_schema::schema::SchemaRef;
pub use expr_rewriter::{rewrite_predicate_for_partitioning, PredicateGroups};
pub use partitioning::{PartitionField, PartitionTransform};
pub use pushdowns::Pushdowns;
#[cfg(feature = "python")]
pub use python::register_modules;
pub use scan_operator::{ScanOperator, ScanOperatorRef};
pub use scan_task::{BoxScanTaskLikeIter, ScanTaskLike, ScanTaskLikeRef};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PhysicalScanInfo {
    pub scan_op: ScanOperatorRef,
    pub source_schema: SchemaRef,
    pub partitioning_keys: Vec<PartitionField>,
    pub pushdowns: Pushdowns,
}

impl PhysicalScanInfo {
    #[must_use]
    pub fn new(
        scan_op: ScanOperatorRef,
        source_schema: SchemaRef,
        partitioning_keys: Vec<PartitionField>,
        pushdowns: Pushdowns,
    ) -> Self {
        Self {
            scan_op,
            source_schema,
            partitioning_keys,
            pushdowns,
        }
    }

    #[must_use]
    pub fn with_pushdowns(&self, pushdowns: Pushdowns) -> Self {
        Self {
            scan_op: self.scan_op.clone(),
            source_schema: self.source_schema.clone(),
            partitioning_keys: self.partitioning_keys.clone(),
            pushdowns,
        }
    }
}
