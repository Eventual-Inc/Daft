use common_error::DaftResult;

use crate::datatypes::{Float32Array, Float64Array};

impl Float32Array {
    pub fn pow(&self, by: f64) -> DaftResult<Self> {
        self.apply(|v| v.powf(by as f32))
    }
}

impl Float64Array {
    pub fn pow(&self, by: f64) -> DaftResult<Self> {
        self.apply(|v| v.powf(by))
    }
}
