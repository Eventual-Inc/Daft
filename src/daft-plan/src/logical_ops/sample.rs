use crate::LogicalPlan;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub struct Sample {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub fraction: f64,
    pub with_replacement: bool,
    pub seed: Option<u64>,
}

impl Eq for Sample {}

impl Hash for Sample {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash the `input` field.
        self.input.hash(state);

        // Convert the `f64` to a stable format with 6 decimal places.
        let fraction_str = format!("{:.6}", self.fraction);
        fraction_str.hash(state);

        // Hash the rest of the fields.
        self.with_replacement.hash(state);
        self.seed.hash(state);
    }
}

impl Sample {
    pub(crate) fn new(
        input: Arc<LogicalPlan>,
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
