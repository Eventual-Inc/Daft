use arrow2::array::PrimitiveArray;
use common_error::{DaftError, DaftResult};

use crate::{
    array::{
        ops::{
            DaftApproxSketchAggable, DaftHllMergeAggable, DaftMeanAggable, DaftStddevAggable,
            DaftSumAggable, GroupIndices,
        },
        ListArray,
    },
    count_mode::CountMode,
    datatypes::*,
    series::{IntoSeries, Series},
    with_match_physical_daft_types,
};

impl Series {
    pub fn count(&self, groups: Option<&GroupIndices>, mode: CountMode) -> DaftResult<Self> {
        use crate::array::ops::DaftCountAggable;
        let s = self.as_physical()?;
        with_match_physical_daft_types!(s.data_type(), |$T| {
            match groups {
                Some(groups) => Ok(DaftCountAggable::grouped_count(&s.downcast::<<$T as DaftDataType>::ArrayType>()?, groups, mode)?.into_series()),
                None => Ok(DaftCountAggable::count(&s.downcast::<<$T as DaftDataType>::ArrayType>()?, mode)?.into_series())
            }
        })
    }

    pub fn sum(&self, groups: Option<&GroupIndices>) -> DaftResult<Self> {
        match self.data_type() {
            // intX -> int64 (in line with numpy)
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                let casted = self.cast(&DataType::Int64)?;
                match groups {
                    Some(groups) => {
                        Ok(DaftSumAggable::grouped_sum(&casted.i64()?, groups)?.into_series())
                    }
                    None => Ok(DaftSumAggable::sum(&casted.i64()?)?.into_series()),
                }
            }
            // uintX -> uint64 (in line with numpy)
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                let casted = self.cast(&DataType::UInt64)?;
                match groups {
                    Some(groups) => {
                        Ok(DaftSumAggable::grouped_sum(&casted.u64()?, groups)?.into_series())
                    }
                    None => Ok(DaftSumAggable::sum(&casted.u64()?)?.into_series()),
                }
            }
            // floatX -> floatX (in line with numpy)
            DataType::Float32 => match groups {
                Some(groups) => Ok(DaftSumAggable::grouped_sum(
                    &self.downcast::<Float32Array>()?,
                    groups,
                )?
                .into_series()),
                None => Ok(DaftSumAggable::sum(&self.downcast::<Float32Array>()?)?.into_series()),
            },
            DataType::Float64 => match groups {
                Some(groups) => Ok(DaftSumAggable::grouped_sum(
                    &self.downcast::<Float64Array>()?,
                    groups,
                )?
                .into_series()),
                None => Ok(DaftSumAggable::sum(&self.downcast::<Float64Array>()?)?.into_series()),
            },
            DataType::Decimal128(_, _) => {
                let casted = self.cast(&try_sum_supertype(self.data_type())?)?;

                match groups {
                    Some(groups) => Ok(DaftSumAggable::grouped_sum(
                        &casted.downcast::<Decimal128Array>()?,
                        groups,
                    )?
                    .into_series()),
                    None => {
                        Ok(DaftSumAggable::sum(&casted.downcast::<Decimal128Array>()?)?
                            .into_series())
                    }
                }
            }
            other => Err(DaftError::TypeError(format!(
                "Numeric sum is not implemented for type {}",
                other
            ))),
        }
    }

    pub fn approx_sketch(&self, groups: Option<&GroupIndices>) -> DaftResult<Self> {
        // Upcast all numeric types to float64 and compute approx_sketch.
        match self.data_type() {
            dt if dt.is_numeric() => {
                let casted = self.cast(&DataType::Float64)?;
                match groups {
                    Some(groups) => Ok(DaftApproxSketchAggable::grouped_approx_sketch(
                        &casted.f64()?,
                        groups,
                    )?
                    .into_series()),
                    None => {
                        Ok(DaftApproxSketchAggable::approx_sketch(&casted.f64()?)?.into_series())
                    }
                }
            }
            other => Err(DaftError::TypeError(format!(
                "Approx sketch is not implemented for type {}",
                other
            ))),
        }
    }

    pub fn merge_sketch(&self, groups: Option<&GroupIndices>) -> DaftResult<Self> {
        use crate::{array::ops::DaftMergeSketchAggable, datatypes::DataType::*};

        match self.data_type() {
            Struct(_) => match groups {
                Some(groups) => Ok(DaftMergeSketchAggable::grouped_merge_sketch(
                    &self.struct_()?,
                    groups,
                )?
                .into_series()),
                None => Ok(DaftMergeSketchAggable::merge_sketch(&self.struct_()?)?.into_series()),
            },
            other => Err(DaftError::TypeError(format!(
                "Merge sketch is not implemented for type {}",
                other
            ))),
        }
    }

    pub fn hll_merge(&self, groups: Option<&GroupIndices>) -> DaftResult<Self> {
        let downcasted_self = self.downcast::<FixedSizeBinaryArray>()?;
        let series = match groups {
            Some(groups) => downcasted_self.grouped_hll_merge(groups),
            None => downcasted_self.hll_merge(),
        }?
        .into_series();
        Ok(series)
    }

    pub fn mean(&self, groups: Option<&GroupIndices>) -> DaftResult<Self> {
        let target_type = try_mean_aggregation_supertype(self.data_type())?;
        match target_type {
            DataType::Float64 => {
                let casted = self.cast(&DataType::Float64)?;
                let casted = casted.f64()?;
                let series = groups
                    .map_or_else(|| casted.mean(), |groups| casted.grouped_mean(groups))?
                    .into_series();
                Ok(series)
            }
            DataType::Decimal128(..) => {
                let casted = self.cast(&target_type)?;
                let casted = casted.decimal128()?;
                let series = groups
                    .map_or_else(|| casted.mean(), |groups| casted.grouped_mean(groups))?
                    .into_series();
                Ok(series)
            }

            _ => Err(DaftError::not_implemented(format!(
                "Mean not implemented for {target_type}, source type: {}",
                self.data_type()
            ))),
        }
    }

    pub fn stddev(&self, groups: Option<&GroupIndices>) -> DaftResult<Self> {
        let target_type = try_stddev_aggregation_supertype(self.data_type())?;
        match target_type {
            DataType::Float64 => {
                let casted = self.cast(&DataType::Float64)?;
                let casted = casted.f64()?;
                let series = groups
                    .map_or_else(|| casted.stddev(), |groups| casted.grouped_stddev(groups))?
                    .into_series();
                Ok(series)
            }
            _ => Err(DaftError::not_implemented(format!(
                "StdDev not implemented for {target_type}, source type: {}",
                self.data_type()
            ))),
        }
    }

    pub fn min(&self, groups: Option<&GroupIndices>) -> DaftResult<Self> {
        self.inner.min(groups)
    }

    pub fn max(&self, groups: Option<&GroupIndices>) -> DaftResult<Self> {
        self.inner.max(groups)
    }

    pub fn any_value(&self, groups: Option<&GroupIndices>, ignore_nulls: bool) -> DaftResult<Self> {
        let indices = match groups {
            Some(groups) => {
                if self.data_type().is_null() {
                    Box::new(PrimitiveArray::new_null(
                        arrow2::datatypes::DataType::UInt64,
                        groups.len(),
                    ))
                } else if ignore_nulls && let Some(validity) = self.validity() {
                    Box::new(PrimitiveArray::from_trusted_len_iter(groups.iter().map(
                        |g| g.iter().find(|i| validity.get_bit(**i as usize)).copied(),
                    )))
                } else {
                    Box::new(PrimitiveArray::from_trusted_len_iter(
                        groups.iter().map(|g| g.first().copied()),
                    ))
                }
            }
            None => {
                let idx = if self.data_type().is_null() || self.is_empty() {
                    None
                } else if ignore_nulls && let Some(validity) = self.validity() {
                    validity.iter().position(|v| v).map(|i| i as u64)
                } else {
                    Some(0)
                };

                Box::new(PrimitiveArray::from([idx]))
            }
        };

        self.take(&Self::from_arrow(
            Field::new("", DataType::UInt64).into(),
            indices,
        )?)
    }

    pub fn agg_list(&self, groups: Option<&GroupIndices>) -> DaftResult<Self> {
        self.inner.agg_list(groups)
    }

    pub fn agg_concat(&self, groups: Option<&GroupIndices>) -> DaftResult<Self> {
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
            DataType::Utf8 => {
                let downcasted = self.downcast::<Utf8Array>()?;
                match groups {
                    Some(groups) => {
                        Ok(DaftConcatAggable::grouped_concat(downcasted, groups)?.into_series())
                    }
                    None => Ok(DaftConcatAggable::concat(downcasted)?.into_series()),
                }
            }
            _ => Err(DaftError::TypeError(format!(
                "concat aggregation is only valid for List, Python types, or Utf8, got {}",
                self.data_type()
            ))),
        }
    }
}
