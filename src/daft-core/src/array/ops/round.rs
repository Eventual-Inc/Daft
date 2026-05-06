use common_error::DaftResult;
use num_traits::Pow;

use crate::datatypes::{Float16Array, Float32Array, Float64Array};

impl Float16Array {
    pub fn round(&self, decimal: i32) -> DaftResult<Self> {
        let multiplier: f64 = 10.0_f64.powi(decimal);
        self.apply(|v| half::f16::from_f64((v.to_f64() * multiplier).round() / multiplier))
    }
}

impl Float32Array {
    pub fn round(&self, decimal: i32) -> DaftResult<Self> {
        if decimal == 0 {
            self.apply(|v| v.round())
        } else {
            let multiplier: f64 = 10.0.pow(decimal);
            self.apply(|v| ((v as f64 * multiplier).round() / multiplier) as f32)
        }
    }
}

impl Float64Array {
    pub fn round(&self, decimal: i32) -> DaftResult<Self> {
        if decimal == 0 {
            self.apply(|v| v.round())
        } else {
            let multiplier: f64 = 10.0.pow(decimal);
            self.apply(|v| (v * multiplier).round() / multiplier)
        }
    }
}
