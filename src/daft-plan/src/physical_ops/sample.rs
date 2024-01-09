use std::sync::Arc;

use crate::physical_plan::PhysicalPlan;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Sample {
    pub input: Arc<PhysicalPlan>,
    pub fraction: f64,
    pub with_replacement: bool,
    pub seed: Option<u64>,
}

impl Sample {
    pub(crate) fn new(
        input: Arc<PhysicalPlan>,
        fraction: &str,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> Self {
        Self {
            input,
            fraction: fraction.parse().unwrap(),
            with_replacement,
            seed,
        }
    }
}
