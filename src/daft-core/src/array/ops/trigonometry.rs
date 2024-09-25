use common_error::DaftResult;
use num_traits::Float;
use serde::{Deserialize, Serialize};

use super::DaftAtan2;
use crate::{
    array::DataArray,
    datatypes::{DaftFloatType, Float32Array, Float64Array},
};

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
        match self {
            Self::Sin => "sin",
            Self::Cos => "cos",
            Self::Tan => "tan",
            Self::Cot => "cot",
            Self::ArcSin => "arcsin",
            Self::ArcCos => "arccos",
            Self::ArcTan => "arctan",
            Self::Radians => "radians",
            Self::Degrees => "degrees",
            Self::ArcTanh => "arctanh",
            Self::ArcCosh => "arccosh",
            Self::ArcSinh => "arcsinh",
        }
    }
}

impl<T> DataArray<T>
where
    T: DaftFloatType,
    T::Native: Float,
{
    pub fn trigonometry(&self, func: &TrigonometricFunction) -> DaftResult<Self> {
        match func {
            TrigonometricFunction::Sin => self.apply(|v| v.sin()),
            TrigonometricFunction::Cos => self.apply(|v| v.cos()),
            TrigonometricFunction::Tan => self.apply(|v| v.tan()),
            TrigonometricFunction::Cot => self.apply(|v| v.tan().powi(-1)),
            TrigonometricFunction::ArcSin => self.apply(|v| v.asin()),
            TrigonometricFunction::ArcCos => self.apply(|v| v.acos()),
            TrigonometricFunction::ArcTan => self.apply(|v| v.atan()),
            TrigonometricFunction::Radians => self.apply(|v| v.to_radians()),
            TrigonometricFunction::Degrees => self.apply(|v| v.to_degrees()),
            TrigonometricFunction::ArcTanh => self.apply(|v| v.atanh()),
            TrigonometricFunction::ArcCosh => self.apply(|v| v.acosh()),
            TrigonometricFunction::ArcSinh => self.apply(|v| v.asinh()),
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
