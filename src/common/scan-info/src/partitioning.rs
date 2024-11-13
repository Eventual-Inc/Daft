use std::fmt::Display;

use common_error::{DaftError, DaftResult};
use daft_schema::field::Field;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum PartitionTransform {
    /// https://iceberg.apache.org/spec/#partitioning
    /// For Delta, Hudi and Hive, it should always be `Identity`.
    Identity,
    IcebergBucket(u64),
    IcebergTruncate(u64),
    Year,
    Month,
    Day,
    Hour,
    Void,
}

impl PartitionTransform {
    #[must_use]
    pub fn supports_equals(&self) -> bool {
        true
    }

    #[must_use]
    pub fn supports_not_equals(&self) -> bool {
        matches!(self, Self::Identity)
    }

    #[must_use]
    pub fn supports_comparison(&self) -> bool {
        use PartitionTransform::{Day, Hour, IcebergTruncate, Identity, Month, Year};
        matches!(
            self,
            Identity | IcebergTruncate(_) | Year | Month | Day | Hour
        )
    }
}

impl Display for PartitionTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PartitionField {
    pub field: Field,
    pub source_field: Option<Field>,
    pub transform: Option<PartitionTransform>,
}

impl PartitionField {
    pub fn new(
        field: Field,
        source_field: Option<Field>,
        transform: Option<PartitionTransform>,
    ) -> DaftResult<Self> {
        match (&source_field, &transform) {
            (Some(_), Some(_)) => {
                // TODO ADD VALIDATION OF TRANSFORM based on types
                Ok(Self {
                    field,
                    source_field,
                    transform,
                })
            }
            (None, Some(tfm)) => Err(DaftError::ValueError(format!(
                "transform set in PartitionField: {tfm} but source_field not set"
            ))),
            _ => Ok(Self {
                field,
                source_field,
                transform,
            }),
        }
    }

    pub fn clone_field(&self) -> Field {
        self.field.clone()
    }
}

impl Display for PartitionField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(tfm) = &self.transform {
            write!(
                f,
                "PartitionField({}, src={}, tfm={})",
                self.field,
                self.source_field.as_ref().unwrap(),
                tfm
            )
        } else {
            write!(f, "PartitionField({})", self.field)
        }
    }
}
