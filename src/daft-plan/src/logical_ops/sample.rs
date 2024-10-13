use std::{
    fmt::{Display, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};

use serde::{Deserialize, Serialize};

use crate::LogicalPlan;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SampleBy {
    Size(usize),
    Fraction(f64),
}

impl Display for SampleBy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Size(size) => write!(f, "Size={}", size),
            Self::Fraction(fraction) => write!(f, "Fraction={}", fraction),
        }
    }
}

impl Hash for SampleBy {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Size(size) => size.hash(state),
            Self::Fraction(fraction) => {
                // Convert the `f64` to a stable format with 6 decimal places.
                #[expect(clippy::collection_is_never_read, reason = "nursery bug pretty sure")]
                let fraction_str = format!("{:.6}", fraction);
                fraction_str.hash(state);
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Sample {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub sample_by: SampleBy,
    pub with_replacement: bool,
    pub seed: Option<u64>,
}

impl Eq for Sample {}

impl Hash for Sample {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash the `input` field.
        self.input.hash(state);

        // Hash the `sample_by` field.
        self.sample_by.hash(state);

        // Hash the rest of the fields.
        self.with_replacement.hash(state);
        self.seed.hash(state);
    }
}

impl Sample {
    pub(crate) fn new(
        input: Arc<LogicalPlan>,
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
