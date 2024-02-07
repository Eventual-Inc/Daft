use crate::physical_plan::PhysicalPlanRef;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    pub input: PhysicalPlanRef,
    pub fraction: f64,
    pub with_replacement: bool,
    pub seed: Option<u64>,
}

impl Sample {
    pub(crate) fn new(
        input: PhysicalPlanRef,
        fraction: f64,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> Self {
        Self {
            input,
            fraction,
            with_replacement,
            seed,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Sample: {}", self.fraction));
        res.push(format!("With replacement = {}", self.with_replacement));
        res.push(format!("Seed = {:?}", self.seed));
        res
    }
}
