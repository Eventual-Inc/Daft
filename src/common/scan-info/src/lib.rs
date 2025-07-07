#![feature(let_chains)]
#![feature(if_let_guard)]

mod expr_rewriter;
mod partitioning;
mod pushdowns;
#[cfg(feature = "python")]
pub mod python;
mod scan_operator;
mod scan_task;
mod sharder;
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
use serde::{Deserialize, Serialize};
pub use sharder::{Sharder, ShardingStrategy};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ScanState {
    Tasks(Arc<Vec<ScanTaskLikeRef>>),
    #[serde(
        serialize_with = "serialize_invalid",
        deserialize_with = "deserialize_invalid"
    )]
    Operator(ScanOperatorRef),
}

fn serialize_invalid<S>(_: &ScanOperatorRef, _: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    Err(serde::ser::Error::custom(
        "ScanOperatorRef cannot be serialized",
    ))
}

fn deserialize_invalid<'de, D>(_: D) -> Result<ScanOperatorRef, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Err(serde::de::Error::custom(
        "ScanOperatorRef cannot be deserialized",
    ))
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

    #[must_use]
    pub fn with_scan_state(&self, scan_state: ScanState) -> Self {
        Self {
            scan_state,
            source_schema: self.source_schema.clone(),
            partitioning_keys: self.partitioning_keys.clone(),
            pushdowns: self.pushdowns.clone(),
        }
    }
}
