use std::{
    fmt::Debug,
    hash::{Hash, Hasher},
    sync::Arc,
};

use common_display::{DisplayAs, DisplayLevel};
use crate::{PartitionField, Pushdowns};
use daft_schema::schema::SchemaRef;
use serde::{Deserialize, Serialize};

use crate::{ScanOperatorRef, ScanTaskRef};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScanState {
    Tasks(Arc<Vec<ScanTaskRef>>),
    #[serde(
        serialize_with = "serialize_invalid",
        deserialize_with = "deserialize_invalid"
    )]
    Operator(ScanOperatorRef),
}

// Pointer-based equality: two Tasks are equal iff they point to the same Arc.
// This is consistent with ScanOperatorRef, which also uses pointer equality.
impl PartialEq for ScanState {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Tasks(a), Self::Tasks(b)) => Arc::ptr_eq(a, b),
            (Self::Operator(a), Self::Operator(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for ScanState {}

impl Hash for ScanState {
    fn hash<H: Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        match self {
            Self::Tasks(tasks) => Arc::as_ptr(tasks).hash(state),
            Self::Operator(op) => op.hash(state),
        }
    }
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
                let mut result = vec![format!("Num Scan Tasks = {}", scan_tasks.len())];

                if scan_tasks.is_empty() {
                    return result;
                }

                // Display scan tasks similar to ScanTaskSource
                // Show pushdowns and schema from the first scan task
                let first_task = scan_tasks.first().unwrap();
                let pushdowns = &first_task.pushdowns;
                if !pushdowns.is_empty() {
                    result.push(pushdowns.display_as(DisplayLevel::Compact));
                }

                let schema = &first_task.schema;
                result.push(format!(
                    "Schema: {{{}}}",
                    schema.display_as(DisplayLevel::Compact)
                ));

                // List scan tasks using compact display (which includes sources/function names)
                result.push("Scan Tasks: [".to_string());
                for (i, scan_task) in scan_tasks.iter().enumerate() {
                    if i < 3 || i >= scan_tasks.len() - 3 {
                        result.push(scan_task.display_as(DisplayLevel::Compact));
                    } else if i == 3 {
                        result.push("...".to_string());
                    }
                }
                result.push("]".to_string());

                result
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
