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

use std::{fmt::Debug, hash::Hash, sync::Arc};

use daft_schema::schema::SchemaRef;
pub use expr_rewriter::{rewrite_predicate_for_partitioning, PredicateGroups};
pub use partitioning::{PartitionField, PartitionTransform};
pub use pushdowns::Pushdowns;
#[cfg(feature = "python")]
pub use python::register_modules;
pub use scan_operator::{ScanOperator, ScanOperatorRef};
pub use scan_task::{ScanTaskLike, ScanTaskLikeRef, SPLIT_AND_MERGE_PASS};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScanState {
    Operator(ScanOperatorRef),
    Tasks(Arc<Vec<ScanTaskLikeRef>>),
}

impl ScanState {
    pub fn multiline_display(&self) -> Vec<String> {
        match self {
            Self::Operator(scan_op) => scan_op.0.multiline_display(),
            Self::Tasks(scan_tasks) => {
                vec![format!("Num Scan Tasks = {}", scan_tasks.len())]
            }
        }
    }

    pub fn get_scan_op(&self) -> &ScanOperatorRef {
        match self {
            Self::Operator(scan_op) => scan_op,
            Self::Tasks(_) => panic!("Tried to get scan op from materialized physical scan info"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PhysicalScanInfo {
    pub scan_state: ScanState,
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
            scan_state: ScanState::Operator(scan_op),
            source_schema,
            partitioning_keys,
            pushdowns,
        }
    }

    #[must_use]
    pub fn with_pushdowns(&self, pushdowns: Pushdowns) -> Self {
        Self {
            scan_state: self.scan_state.clone(),
            source_schema: self.source_schema.clone(),
            partitioning_keys: self.partitioning_keys.clone(),
            pushdowns,
        }
    }
}
