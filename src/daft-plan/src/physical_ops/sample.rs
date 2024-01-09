use std::sync::Arc;

use crate::physical_plan::PhysicalPlan;
use common_error::{DaftError, DaftResult};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Sample {
    pub input: Arc<PhysicalPlan>,
    pub fraction: f64,
    pub with_replacement: bool,
    pub seed: Option<u64>,
}

impl Sample {
    pub(crate) fn try_new(
        input: Arc<PhysicalPlan>,
        fraction: &str,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> DaftResult<Self> {
        let fraction_parsed = fraction.parse::<f64>();

        match fraction_parsed {
            Ok(fraction) => Ok(Self {
                input,
                fraction,
                with_replacement,
                seed,
            }),
            Err(_) => {
                // Return an error if the fraction could not be parsed.
                Err(DaftError::ValueError(format!(
                    "Invalid fraction format: {}",
                    fraction
                )))
            }
        }
    }
}
