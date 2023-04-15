use crate::{
    array::ops::GroupIndices,
    error::{DaftError, DaftResult},
    series::Series,
    with_match_comparable_daft_types, with_match_daft_types,
};

use crate::array::BaseArray;
use crate::datatypes::*;

impl Series {
    pub fn count(&self) -> DaftResult<Series> {
        use crate::array::ops::DaftCountAggable;

        with_match_daft_types!(self.data_type(), |$T| {
            Ok(DaftCountAggable::count(&self.downcast::<$T>()?)?.into_series())
        })
    }

    pub fn grouped_count(&self, groups: &GroupIndices) -> DaftResult<Series> {
        use crate::array::ops::DaftCountAggable;

        with_match_daft_types!(self.data_type(), |$T| {
            Ok(DaftCountAggable::grouped_count(&self.downcast::<$T>()?, groups)?.into_series())
        })
    }

    pub fn sum(&self) -> DaftResult<Series> {
        use crate::array::ops::DaftSumAggable;
        use crate::datatypes::DataType::*;

        match self.data_type() {
            // intX -> int64 (in line with numpy)
            Int8 | Int16 | Int32 | Int64 => {
                let casted = self.cast(&Int64)?;
                Ok(DaftSumAggable::sum(&casted.i64()?)?.into_series())
            }
            // uintX -> uint64 (in line with numpy)
            UInt8 | UInt16 | UInt32 | UInt64 => {
                let casted = self.cast(&UInt64)?;
                Ok(DaftSumAggable::sum(&casted.u64()?)?.into_series())
            }
            // floatX -> floatX (in line with numpy)
            Float32 => Ok(DaftSumAggable::sum(&self.downcast::<Float32Type>()?)?.into_series()),
            Float64 => Ok(DaftSumAggable::sum(&self.downcast::<Float64Type>()?)?.into_series()),
            other => Err(DaftError::TypeError(format!(
                "Numeric sum is not implemented for type {}",
                other
            ))),
        }
    }

    pub fn grouped_sum(&self, groups: &GroupIndices) -> DaftResult<Series> {
        use crate::array::ops::DaftSumAggable;
        use crate::datatypes::DataType::*;

        match self.data_type() {
            // intX -> int64 (in line with numpy)
            Int8 | Int16 | Int32 | Int64 => {
                let casted = self.cast(&Int64)?;
                Ok(DaftSumAggable::grouped_sum(&casted.i64()?, groups)?.into_series())
            }
            // uintX -> uint64 (in line with numpy)
            UInt8 | UInt16 | UInt32 | UInt64 => {
                let casted = self.cast(&UInt64)?;
                Ok(DaftSumAggable::grouped_sum(&casted.u64()?, groups)?.into_series())
            }
            // floatX -> floatX (in line with numpy)
            Float32 => Ok(
                DaftSumAggable::grouped_sum(&self.downcast::<Float32Type>()?, groups)?
                    .into_series(),
            ),
            Float64 => Ok(
                DaftSumAggable::grouped_sum(&self.downcast::<Float64Type>()?, groups)?
                    .into_series(),
            ),
            other => Err(DaftError::TypeError(format!(
                "Numeric sum is not implemented for type {}",
                other
            ))),
        }
    }

    pub fn mean(&self) -> DaftResult<Series> {
        use crate::array::ops::DaftMeanAggable;
        use crate::datatypes::DataType::*;

        // Upcast all numeric types to float64 and use f64 mean kernel.
        match self.data_type() {
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float32 | Float64 => {
                let casted = self.cast(&Float64)?;
                Ok(DaftMeanAggable::mean(&casted.f64()?)?.into_series())
            }
            other => Err(DaftError::TypeError(format!(
                "Numeric mean is not implemented for type {}",
                other
            ))),
        }
    }

    pub fn grouped_mean(&self, groups: &GroupIndices) -> DaftResult<Series> {
        use crate::array::ops::DaftMeanAggable;
        use crate::datatypes::DataType::*;

        // Upcast all numeric types to float64 and use f64 mean kernel.
        match self.data_type() {
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float32 | Float64 => {
                let casted = self.cast(&Float64)?;
                Ok(DaftMeanAggable::grouped_mean(&casted.f64()?, groups)?.into_series())
            }
            other => Err(DaftError::TypeError(format!(
                "Numeric mean is not implemented for type {}",
                other
            ))),
        }
    }
    pub fn min(&self) -> DaftResult<Series> {
        use crate::array::ops::DaftCompareAggable;

        let s = self.as_physical()?;

        let result = with_match_comparable_daft_types!(s.data_type(), |$T| {
            DaftCompareAggable::min(&s.downcast::<$T>()?)?.into_series()
        });

        if result.data_type() != self.data_type() {
            return result.cast(self.data_type());
        }
        Ok(result)
    }

    pub fn grouped_min(&self, groups: &GroupIndices) -> DaftResult<Series> {
        use crate::array::ops::DaftCompareAggable;

        let s = self.as_physical()?;

        let result = with_match_comparable_daft_types!(s.data_type(), |$T| {
            DaftCompareAggable::grouped_min(&s.downcast::<$T>()?, groups)?.into_series()
        });

        if result.data_type() != self.data_type() {
            return result.cast(self.data_type());
        }
        Ok(result)
    }

    pub fn max(&self) -> DaftResult<Series> {
        use crate::array::ops::DaftCompareAggable;

        let s = self.as_physical()?;

        let result = with_match_comparable_daft_types!(s.data_type(), |$T| {
            DaftCompareAggable::max(&s.downcast::<$T>()?)?.into_series()
        });

        if result.data_type() != self.data_type() {
            return result.cast(self.data_type());
        }
        Ok(result)
    }

    pub fn grouped_max(&self, groups: &GroupIndices) -> DaftResult<Series> {
        use crate::array::ops::DaftCompareAggable;

        let s = self.as_physical()?;

        let result = with_match_comparable_daft_types!(s.data_type(), |$T| {
            DaftCompareAggable::grouped_max(&s.downcast::<$T>()?, groups)?.into_series()
        });

        if result.data_type() != self.data_type() {
            return result.cast(self.data_type());
        }
        Ok(result)
    }
}
