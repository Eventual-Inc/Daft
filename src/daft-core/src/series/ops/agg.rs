use crate::array::ListArray;
use crate::count_mode::CountMode;
use crate::series::IntoSeries;
use crate::{array::ops::GroupIndices, series::Series, with_match_physical_daft_types};
use common_error::{DaftError, DaftResult};

use crate::datatypes::*;

impl Series {
    pub fn count(&self, groups: Option<&GroupIndices>, mode: CountMode) -> DaftResult<Series> {
        use crate::array::ops::DaftCountAggable;
        let s = self.as_physical()?;
        with_match_physical_daft_types!(s.data_type(), |$T| {
            match groups {
                Some(groups) => Ok(DaftCountAggable::grouped_count(&s.downcast::<<$T as DaftDataType>::ArrayType>()?, groups, mode)?.into_series()),
                None => Ok(DaftCountAggable::count(&s.downcast::<<$T as DaftDataType>::ArrayType>()?, mode)?.into_series())
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
                    &self.downcast::<Float32Array>()?,
                    groups,
                )?
                .into_series()),
                None => Ok(DaftSumAggable::sum(&self.downcast::<Float32Array>()?)?.into_series()),
            },
            Float64 => match groups {
                Some(groups) => Ok(DaftSumAggable::grouped_sum(
                    &self.downcast::<Float64Array>()?,
                    groups,
                )?
                .into_series()),
                None => Ok(DaftSumAggable::sum(&self.downcast::<Float64Array>()?)?.into_series()),
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
        self.inner.min(groups)
    }

    pub fn max(&self, groups: Option<&GroupIndices>) -> DaftResult<Series> {
        self.inner.max(groups)
    }

    pub fn agg_list(&self, groups: Option<&GroupIndices>) -> DaftResult<Series> {
        self.inner.agg_list(groups)
    }

    pub fn agg_concat(&self, groups: Option<&GroupIndices>) -> DaftResult<Series> {
        use crate::array::ops::DaftConcatAggable;
        match self.data_type() {
            DataType::List(..) => {
                let downcasted = self.downcast::<ListArray>()?;
                match groups {
                    Some(groups) => {
                        Ok(DaftConcatAggable::grouped_concat(downcasted, groups)?.into_series())
                    }
                    None => Ok(DaftConcatAggable::concat(downcasted)?.into_series()),
                }
            }
            #[cfg(feature = "python")]
            DataType::Python => {
                let downcasted = self.downcast::<PythonArray>()?;
                match groups {
                    Some(groups) => {
                        Ok(DaftConcatAggable::grouped_concat(downcasted, groups)?.into_series())
                    }
                    None => Ok(DaftConcatAggable::concat(downcasted)?.into_series()),
                }
            }
            _ => Err(DaftError::TypeError(format!(
                "concat aggregation is only valid for List or Python types, got {}",
                self.data_type()
            ))),
        }
    }
}
