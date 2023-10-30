use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_dsl::Expr;

use crate::{LogicalPlan, PartitionScheme};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Repartition {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub num_partitions: Option<usize>,
    pub partition_by: Vec<Expr>,
    pub scheme: PartitionScheme,
}

impl Repartition {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        num_partitions: Option<usize>,
        partition_by: Vec<Expr>,
        scheme: PartitionScheme,
    ) -> DaftResult<Self> {
        if matches!(scheme, PartitionScheme::Range) {
            return Err(DaftError::ValueError(
                "Repartitioning with the Range partition scheme is not supported.".to_string(),
            ));
        }
        Ok(Self {
            input,
            num_partitions,
            partition_by,
            scheme,
        })
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Repartition: Scheme = {:?}", self.scheme));
        res.push(format!(
            "Number of partitions = {}",
            self.num_partitions
                .map(|n| n.to_string())
                .unwrap_or("Unknown".to_string())
        ));
        if !self.partition_by.is_empty() {
            res.push(format!(
                "Partition by = {}",
                self.partition_by
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        res
    }
}
