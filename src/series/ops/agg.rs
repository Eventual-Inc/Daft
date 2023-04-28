use crate::{
    array::ops::GroupIndices,
    error::{DaftError, DaftResult},
    series::Series,
    with_match_comparable_daft_types, with_match_daft_types,
};

use crate::array::BaseArray;
use crate::datatypes::*;

impl Series {
    pub fn count(&self, groups: Option<&GroupIndices>) -> DaftResult<Series> {
        use crate::array::ops::DaftCountAggable;

        with_match_daft_types!(self.data_type(), |$T| {
            match groups {
                Some(groups) => Ok(DaftCountAggable::grouped_count(&self.downcast::<$T>()?, groups)?.into_series()),
                None => Ok(DaftCountAggable::count(&self.downcast::<$T>()?)?.into_series())
            }
        })
    }

    pub fn sum(&self, groups: Option<&GroupIndices>) -> DaftResult<Series> {
        use crate::array::ops::DaftSumAggable;
        use crate::datatypes::DataType::*;

        match self.data_type() {
            // intX -> int64 (in line with numpy)
            Int8 | Int16 | Int32 | Int64 => {
                let casted = self.cast(&Int64)?;
                match groups {
                    Some(groups) => {
                        Ok(DaftSumAggable::grouped_sum(&casted.i64()?, groups)?.into_series())
                    }
                    None => Ok(DaftSumAggable::sum(&casted.i64()?)?.into_series()),
                }
            }
            // uintX -> uint64 (in line with numpy)
            UInt8 | UInt16 | UInt32 | UInt64 => {
                let casted = self.cast(&UInt64)?;
                match groups {
                    Some(groups) => {
                        Ok(DaftSumAggable::grouped_sum(&casted.u64()?, groups)?.into_series())
                    }
                    None => Ok(DaftSumAggable::sum(&casted.u64()?)?.into_series()),
                }
            }
            // floatX -> floatX (in line with numpy)
            Float32 => match groups {
                Some(groups) => Ok(DaftSumAggable::grouped_sum(
                    &self.downcast::<Float32Type>()?,
                    groups,
                )?
                .into_series()),
                None => Ok(DaftSumAggable::sum(&self.downcast::<Float32Type>()?)?.into_series()),
            },
            Float64 => match groups {
                Some(groups) => Ok(DaftSumAggable::grouped_sum(
                    &self.downcast::<Float64Type>()?,
                    groups,
                )?
                .into_series()),
                None => Ok(DaftSumAggable::sum(&self.downcast::<Float64Type>()?)?.into_series()),
            },
            other => Err(DaftError::TypeError(format!(
                "Numeric sum is not implemented for type {}",
                other
            ))),
        }
    }

    pub fn mean(&self, groups: Option<&GroupIndices>) -> DaftResult<Series> {
        use crate::array::ops::DaftMeanAggable;
        use crate::datatypes::DataType::*;

        // Upcast all numeric types to float64 and use f64 mean kernel.
        match self.data_type() {
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float32 | Float64 => {
                let casted = self.cast(&Float64)?;
                match groups {
                    Some(groups) => {
                        Ok(DaftMeanAggable::grouped_mean(&casted.f64()?, groups)?.into_series())
                    }
                    None => Ok(DaftMeanAggable::mean(&casted.f64()?)?.into_series()),
                }
            }
            other => Err(DaftError::TypeError(format!(
                "Numeric mean is not implemented for type {}",
                other
            ))),
        }
    }

    pub fn min(&self, groups: Option<&GroupIndices>) -> DaftResult<Series> {
        use crate::array::ops::DaftCompareAggable;

        let s = self.as_physical()?;

        let result = with_match_comparable_daft_types!(s.data_type(), |$T| {
            match groups {
                Some(groups) => DaftCompareAggable::grouped_min(&s.downcast::<$T>()?, groups)?.into_series(),
                None => DaftCompareAggable::min(&s.downcast::<$T>()?)?.into_series()
            }
        });

        if result.data_type() != self.data_type() {
            return result.cast(self.data_type());
        }
        Ok(result)
    }

    pub fn max(&self, groups: Option<&GroupIndices>) -> DaftResult<Series> {
        use crate::array::ops::DaftCompareAggable;

        let s = self.as_physical()?;

        let result = with_match_comparable_daft_types!(s.data_type(), |$T| {
            match groups {
                Some(groups) => DaftCompareAggable::grouped_max(&s.downcast::<$T>()?, groups)?.into_series(),
                None => DaftCompareAggable::max(&s.downcast::<$T>()?)?.into_series()
            }
        });

        if result.data_type() != self.data_type() {
            return result.cast(self.data_type());
        }
        Ok(result)
    }

    pub fn agg_list(&self, groups: Option<&GroupIndices>) -> DaftResult<Series> {
        use crate::array::ops::DaftListAggable;
        with_match_daft_types!(self.data_type(), |$T| {
            match groups {
                Some(groups) => Ok(DaftListAggable::grouped_list(self.downcast::<$T>()?, groups)?.into_series()),
                None => Ok(DaftListAggable::list(self.downcast::<$T>()?)?.into_series())
            }
        })
    }

    pub fn agg_concat(&self, groups: Option<&GroupIndices>) -> DaftResult<Series> {
        if !matches!(self.data_type(), DataType::List(..)) {
            return Err(DaftError::TypeError(format!(
                "concat aggregation is only valid for List Types, got {}",
                self.data_type()
            )));
        }
        let downcasted = self.downcast()?;
        use crate::array::ops::DaftConcatAggable;
        match groups {
            Some(groups) => {
                Ok(DaftConcatAggable::grouped_concat(downcasted, groups)?.into_series())
            }
            None => Ok(DaftConcatAggable::concat(downcasted)?.into_series()),
        }
    }
}
