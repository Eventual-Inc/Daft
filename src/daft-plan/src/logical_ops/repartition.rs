use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::resolve_exprs;

use crate::{
    partitioning::{HashRepartitionConfig, RepartitionSpec},
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Repartition {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub repartition_spec: RepartitionSpec,
}

impl Repartition {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        repartition_spec: RepartitionSpec,
    ) -> DaftResult<Self> {
        let repartition_spec = match repartition_spec {
            RepartitionSpec::Hash(HashRepartitionConfig { num_partitions, by }) => {
                let (resolved_by, _) = resolve_exprs(by, &input.schema())?;
                RepartitionSpec::Hash(HashRepartitionConfig {
                    num_partitions,
                    by: resolved_by,
                })
            }
            RepartitionSpec::Random(_) | RepartitionSpec::IntoPartitions(_) => repartition_spec,
        };

        Ok(Self {
            input,
            repartition_spec,
        })
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "Repartition: Scheme = {}",
            self.repartition_spec.var_name(),
        ));
        res.extend(self.repartition_spec.multiline_display());
        res
    }
}
