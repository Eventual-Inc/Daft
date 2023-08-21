use std::sync::Arc;

use daft_dsl::Expr;

use crate::{LogicalPlan, PartitionScheme};

#[derive(Clone, Debug)]
pub struct Repartition {
    pub num_partitions: usize,
    pub partition_by: Vec<Expr>,
    pub scheme: PartitionScheme,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
}

impl Repartition {
    pub(crate) fn new(
        num_partitions: usize,
        partition_by: Vec<Expr>,
        scheme: PartitionScheme,
        input: Arc<LogicalPlan>,
    ) -> Self {
        Self {
            num_partitions,
            partition_by,
            scheme,
            input,
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
