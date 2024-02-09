use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_dsl::Expr;
use itertools::Itertools;

use crate::{partitioning::PartitionSchemeConfig, LogicalPlan};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Repartition {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub num_partitions: Option<usize>,
    pub partition_by: Vec<Expr>,
    pub scheme_config: PartitionSchemeConfig,
}

impl Repartition {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        num_partitions: Option<usize>,
        partition_by: Vec<Expr>,
        scheme_config: PartitionSchemeConfig,
    ) -> DaftResult<Self> {
        if matches!(scheme_config, PartitionSchemeConfig::Range(_)) {
            return Err(DaftError::ValueError(
                "Repartitioning with the Range partition scheme is not supported.".to_string(),
            ));
        }
        Ok(Self {
            input,
            num_partitions,
            partition_by,
            scheme_config,
        })
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        let scheme_config = self.scheme_config.multiline_display();
        res.push(format!(
            "Repartition: Scheme = {}{}",
            self.scheme_config.var_name(),
            if scheme_config.is_empty() {
                "".to_string()
            } else {
                format!("({})", scheme_config.join(", "))
            }
        ));
        res.push(format!(
            "Number of partitions = {}",
            self.num_partitions
                .map(|n| n.to_string())
                .unwrap_or("Unknown".to_string())
        ));
        if !self.partition_by.is_empty() {
            res.push(format!(
                "Partition by = {}",
                self.partition_by.iter().map(|e| e.to_string()).join(", ")
            ));
        }
        res
    }
}
