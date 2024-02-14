use std::sync::Arc;

use common_error::{DaftError, DaftResult};

use crate::{partitioning::ClusteringSpec, LogicalPlan};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Repartition {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub clustering_spec: ClusteringSpec,
}

impl Repartition {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        scheme_config: ClusteringSpec,
    ) -> DaftResult<Self> {
        if matches!(scheme_config, ClusteringSpec::Range(_)) {
            return Err(DaftError::ValueError(
                "Repartitioning with the Range partition scheme is not supported.".to_string(),
            ));
        }
        Ok(Self {
            input,
            clustering_spec: scheme_config,
        })
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "Repartition: Scheme = {}",
            self.clustering_spec.var_name(),
        ));
        res.extend(self.clustering_spec.multiline_display());
        res
    }
}
