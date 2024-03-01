use std::sync::Arc;

use common_error::DaftResult;

use crate::{partitioning::RepartitionSpec, LogicalPlan};

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
