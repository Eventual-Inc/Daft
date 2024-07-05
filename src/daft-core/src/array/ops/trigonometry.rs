use num_traits::Float;
use serde::{Deserialize, Serialize};

use common_error::DaftResult;

use crate::array::DataArray;
use crate::datatypes::{DaftFloatType, Float32Array, Float64Array};

use super::DaftAtan2;

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
    ArcTanh,
    ArcCosh,
    ArcSinh,
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
            ArcTanh => "arctanh",
            ArcCosh => "arccosh",
            ArcSinh => "arcsinh",
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
            ArcTanh => self.apply(|v| v.atanh()),
            ArcCosh => self.apply(|v| v.acosh()),
            ArcSinh => self.apply(|v| v.asinh()),
        }
    }
}

macro_rules! impl_atan2_floating_array {
    ($arr:ident, $T:ident) => {
        impl DaftAtan2<$T> for $arr {
            type Output = DaftResult<Self>;

            fn atan2(&self, rhs: $T) -> Self::Output {
                self.apply(|v| v.atan2(rhs))
            }
        }

        impl DaftAtan2<&$arr> for $arr {
            type Output = DaftResult<Self>;

            fn atan2(&self, rhs: &$arr) -> Self::Output {
                self.binary_apply(rhs, |a, b| a.atan2(b))
            }
        }
    };
}

impl_atan2_floating_array!(Float32Array, f32);
impl_atan2_floating_array!(Float64Array, f64);
