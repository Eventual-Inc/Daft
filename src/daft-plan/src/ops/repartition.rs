use std::sync::Arc;

use daft_dsl::Expr;

use crate::{LogicalPlan, PartitionScheme};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Repartition {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub num_partitions: usize,
    pub partition_by: Vec<Expr>,
    pub scheme: PartitionScheme,
}

impl Repartition {
    pub(crate) fn new(
        input: Arc<LogicalPlan>,
        num_partitions: usize,
        partition_by: Vec<Expr>,
        scheme: PartitionScheme,
    ) -> Self {
        Self {
            input,
            num_partitions,
            partition_by,
            scheme,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Repartition: Scheme = {:?}", self.scheme));
        res.push(format!("Number of partitions = {}", self.num_partitions));
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
