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
    Cot,
    ArcCos,
    ArcSin,
    ArcTan,
    Radians,
    Degrees,
}

impl TrigonometricFunction {
    pub fn fn_name(&self) -> &'static str {
        use TrigonometricFunction::*;
        match self {
            Sin => "sin",
            Cos => "cos",
            Tan => "tan",
            Cot => "cot",
            ArcSin => "arcsin",
            ArcCos => "arccos",
            ArcTan => "arctan",
            Radians => "radians",
            Degrees => "degrees",
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
            Cot => self.apply(|v| v.tan().powi(-1)),
            ArcSin => self.apply(|v| v.asin()),
            ArcCos => self.apply(|v| v.acos()),
            ArcTan => self.apply(|v| v.atan()),
            Radians => self.apply(|v| v.to_radians()),
            Degrees => self.apply(|v| v.to_degrees()),
        }
    }
}
