use num_traits::Float;
use serde::{Deserialize, Serialize};

use common_error::DaftResult;

use crate::array::DataArray;
use crate::datatypes::DaftFloatType;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum TrigonometricFunction {
    Sin,
    Cos,
    Tan,
}

impl TrigonometricFunction {
    pub fn fn_name(&self) -> &'static str {
        use TrigonometricFunction::*;
        match self {
            Sin => "sin",
            Cos => "cos",
            Tan => "tan",
        }
    }
}

impl<T> DataArray<T>
where
    T: DaftFloatType,
    T::Native: Float,
{
    pub fn trigonometry(&self, func: &TrigonometricFunction) -> DaftResult<Self> {
        use TrigonometricFunction::*;
        match func {
            Sin => self.apply(|v| v.sin()),
            Cos => self.apply(|v| v.cos()),
            Tan => self.apply(|v| v.tan()),
        }
    }
}
