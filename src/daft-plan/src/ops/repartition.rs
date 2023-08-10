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
        res.push(format!(
            "Repartition ({:?}): n={}",
            self.scheme, self.num_partitions
        ));
        if !self.partition_by.is_empty() {
            res.push(format!("  Partition by: {:?}", self.partition_by));
        }
        res
    }
}
