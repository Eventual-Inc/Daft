use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprResolver;

use crate::{
    partitioning::{HashRepartitionConfig, RepartitionSpec},
    stats::StatsState,
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Repartition {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub repartition_spec: RepartitionSpec,
    pub stats_state: StatsState,
}

impl Repartition {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        repartition_spec: RepartitionSpec,
    ) -> DaftResult<Self> {
        let repartition_spec = match repartition_spec {
            RepartitionSpec::Hash(HashRepartitionConfig { num_partitions, by }) => {
                let expr_resolver = ExprResolver::default();

                let (resolved_by, _) = expr_resolver.resolve(by, &input.schema())?;
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
            stats_state: StatsState::NotMaterialized,
        })
    }

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        // Repartitioning does not affect cardinality.
        let input_stats = self.input.materialized_stats();
        self.stats_state = StatsState::Materialized(input_stats.clone().into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "Repartition: Scheme = {}",
            self.repartition_spec.var_name(),
        ));
        res.extend(self.repartition_spec.multiline_display());
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
