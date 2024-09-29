use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{impl_default_tree_display, ClusteringSpec, PhysicalPlanRef};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExchangeOp {
    pub input: PhysicalPlanRef,
    pub strategy: ExchangeOpStrategy,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ExchangeOpStrategy {
    /// Fully materialize the data after the Map, and then pull results from the Reduce.
    FullyMaterializing { target_spec: Arc<ClusteringSpec> },
    /// Stand up Reducers and then send data from the mappers into the reducers eagerly
    FullyMaterializingPush { target_spec: Arc<ClusteringSpec> },
}

impl ExchangeOp {
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("ExchangeOp:".to_string());
        match &self.strategy {
            ExchangeOpStrategy::FullyMaterializing { target_spec } => {
                res.push("  Strategy: FullyMaterializing".to_string());
                res.push(format!("  Target Spec: {:?}", target_spec));
            }
            ExchangeOpStrategy::FullyMaterializingPush { target_spec } => {
                res.push("  Strategy: FullyMaterializingPush".to_string());
                res.push(format!("  Target Spec: {:?}", target_spec));
            }
        }
        res
    }
}

impl_default_tree_display!(ExchangeOp);
