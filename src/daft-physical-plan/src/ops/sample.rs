use serde::{Deserialize, Serialize};

use crate::PhysicalPlanRef;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    pub input: PhysicalPlanRef,
    pub fraction: Option<f64>,
    pub size: Option<usize>,
    pub with_replacement: bool,
    pub seed: Option<u64>,
}

impl Sample {
    pub(crate) fn new(
        input: PhysicalPlanRef,
        fraction: Option<f64>,
        size: Option<usize>,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> Self {
        Self {
            input,
            fraction,
            size,
            with_replacement,
            seed,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if let Some(fraction) = self.fraction {
            res.push(format!("Sample: {} (fraction)", fraction));
        } else if let Some(size) = self.size {
            res.push(format!("Sample: {} rows", size));
        }
        res.push(format!("With replacement = {}", self.with_replacement));
        res.push(format!("Seed = {:?}", self.seed));
        res
    }
}

crate::impl_default_tree_display!(Sample);
