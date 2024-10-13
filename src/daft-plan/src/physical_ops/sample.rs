use serde::{Deserialize, Serialize};

use crate::{logical_ops::SampleBy, physical_plan::PhysicalPlanRef};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    pub input: PhysicalPlanRef,
    pub sample_by: SampleBy,
    pub with_replacement: bool,
    pub seed: Option<u64>,
}

impl Sample {
    pub(crate) fn new(
        input: PhysicalPlanRef,
        sample_by: SampleBy,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> Self {
        Self {
            input,
            sample_by,
            with_replacement,
            seed,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Sample: {}", self.sample_by));
        res.push(format!("With replacement = {}", self.with_replacement));
        res.push(format!("Seed = {:?}", self.seed));
        res
    }
}

crate::impl_default_tree_display!(Sample);
