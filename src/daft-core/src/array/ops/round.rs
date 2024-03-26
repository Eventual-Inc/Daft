use num_traits::Pow;

use crate::datatypes::{Float32Array, Float64Array};

use common_error::DaftResult;

impl Float32Array {
    pub fn round(&self, digits: i32) -> DaftResult<Self> {
        if digits == 0 {
            self.apply(|v| v.round())
        } else {
            let multiplier = 10.0.pow(digits as f64);
            self.apply(|v| ((v as f64 * multiplier).round() / multiplier) as f32)
        }
    }
}

impl Float64Array {
    pub fn round(&self, digits: i32) -> DaftResult<Self> {
        if digits == 0 {
            self.apply(|v| v.round())
        } else {
            let multiplier = 10.0.pow(digits as f64);
            self.apply(|v| ((v * multiplier).round() / multiplier))
        }
    }
}
