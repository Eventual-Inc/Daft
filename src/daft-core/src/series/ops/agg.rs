use std::collections::HashMap;

use arrow2::{
    array::{ListArray as Arrow2ListArray, PrimitiveArray},
    offset::OffsetsBuffer,
};
use common_error::{DaftError, DaftResult};

use crate::{
    array::{
        growable::make_growable,
        ops::{
            arrow2::comparison::build_is_equal, as_arrow::AsArrow, DaftApproxSketchAggable,
            DaftCountAggable, DaftHllMergeAggable, DaftMeanAggable, DaftSetAggable,
            DaftStddevAggable, DaftSumAggable, GroupIndices,
        },
        ListArray,
    },
    count_mode::CountMode,
    datatypes::*,
    series::{IntoSeries, Series},
    with_match_physical_daft_types,
};

fn deduplicate_indices(series: &Series) -> DaftResult<Vec<u64>> {
    // Special handling for Null type
    if series.data_type() == &DataType::Null {
        return Ok(vec![]);
    }

    // Special handling for List type
    if let DataType::List(_) = series.data_type() {
        let mut unique_indices = Vec::new();

        let list_array = series.to_arrow();
        let list_array = list_array
            .as_any()
            .downcast_ref::<Arrow2ListArray<i64>>()
            .ok_or_else(|| DaftError::ValueError("Failed to downcast to ListArray".to_string()))?;
        let values = list_array
            .values()
            .as_any()
            .downcast_ref::<PrimitiveArray<i64>>()
            .ok_or_else(|| {
                DaftError::ValueError(
                    "Failed to downcast list values to PrimitiveArray".to_string(),
                )
            })?;

        let hash_series = series.hash(None).map_err(|_| {
            DaftError::ValueError(
                "Cannot perform set aggregation on elements that are not hashable".to_string(),
            )
        })?;
        let hash_array = hash_series.as_arrow();
        let hash_values = hash_array.values();

        for idx in 0..series.len() {
            if !series.is_valid(idx) {
                continue;
            }

            let start = list_array.offsets()[idx] as usize;
            let end = list_array.offsets()[idx + 1] as usize;
            let current_list = &values.values()[start..end];
            let _hash = hash_values.get(idx).unwrap();

            let mut is_duplicate = false;
            for &existing_idx in &unique_indices {
                let start = list_array.offsets()[existing_idx as usize] as usize;
                let end = list_array.offsets()[(existing_idx as usize) + 1] as usize;
                let other_list = &values.values()[start..end];

                if current_list == other_list {
                    is_duplicate = true;
                    break;
                }
            }

            if !is_duplicate {
                unique_indices.push(idx as u64);
            }
        }

        return Ok(unique_indices);
    }

    // Special handling for Struct type
    if let DataType::Struct(_) = series.data_type() {
        let mut unique_indices = Vec::new();

        let hash_series = series.hash(None).map_err(|_| {
            DaftError::ValueError(
                "Cannot perform set aggregation on elements that are not hashable".to_string(),
            )
        })?;
        let hash_array = hash_series.as_arrow();
        let hash_values = hash_array.values();

        for idx in 0..series.len() {
            if !series.is_valid(idx) {
                continue;
            }

            let _hash = hash_values.get(idx).unwrap();
            let mut is_duplicate = false;

            for &existing_idx in &unique_indices {
                let existing_hash = hash_values.get(existing_idx as usize).unwrap();
                if _hash == existing_hash {
                    is_duplicate = true;
                    break;
                }
            }

            if !is_duplicate {
                unique_indices.push(idx as u64);
            }
        }

        return Ok(unique_indices);
    }

    let hashes = series.hash(None).map_err(|_| {
        DaftError::ValueError(
            "Cannot perform set aggregation on elements that are not hashable".to_string(),
        )
    })?;

    let array = series.to_arrow();
    let comparator = build_is_equal(&*array, &*array, true, false)?;
    let mut seen_hashes = HashMap::<u64, Vec<usize>>::new();
    let mut unique_indices = Vec::new();

    let hash_array = hashes.as_arrow();
    for (idx, hash) in hash_array.values_iter().enumerate() {
        if series.is_valid(idx) {
            let mut is_duplicate = false;
            if let Some(existing_indices) = seen_hashes.get(hash) {
                for &existing_idx in existing_indices {
                    if comparator(idx, existing_idx) {
                        is_duplicate = true;
                        break;
                    }
                }
            }

            if !is_duplicate {
                seen_hashes.entry(*hash).or_default().push(idx);
                unique_indices.push(idx as u64);
            }
        }
    }

    Ok(unique_indices)
}

fn deduplicate_series(series: &Series) -> DaftResult<(Series, Vec<u64>)> {
    let unique_indices = deduplicate_indices(series)?;
    let indices_array = UInt64Array::from(("", unique_indices.clone())).into_series();
    let result = series.take(&indices_array)?;
    Ok((result, unique_indices))
}

impl Series {
    pub fn count(&self, groups: Option<&GroupIndices>, mode: CountMode) -> DaftResult<Self> {
        let s = self.as_physical()?;
        with_match_physical_daft_types!(s.data_type(), |$T| {
            match groups {
                Some(groups) => Ok(DaftCountAggable::grouped_count(&s.downcast::<<$T as DaftDataType>::ArrayType>()?, groups, mode)?.into_series()),
                None => Ok(DaftCountAggable::count(&s.downcast::<<$T as DaftDataType>::ArrayType>()?, mode)?.into_series())
            }
        })
    }

    pub fn count_distinct(&self, groups: Option<&GroupIndices>) -> DaftResult<Self> {
        let series = self.agg_list(groups)?.list_count_distinct()?;
        Ok(series)
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

    pub fn agg_set(&self, groups: Option<&GroupIndices>) -> DaftResult<Self> {
        self.inner.agg_set(groups)
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

    pub fn bool_and(&self, groups: Option<&GroupIndices>) -> DaftResult<Self> {
        use crate::array::ops::DaftBoolAggable;
        match self.data_type() {
            DataType::Boolean => {
                let downcasted = self.bool()?;
                Ok(match groups {
                    Some(groups) => downcasted.grouped_bool_and(groups)?,
                    None => downcasted.bool_and()?,
                }
                .into_series())
            }
            DataType::Null => {
                // Return a single null value for null type
                Ok(Self::full_null(
                    self.field().name.as_str(),
                    &DataType::Boolean,
                    1,
                ))
            }
            other => Err(DaftError::TypeError(format!(
                "bool_and is not implemented for type {}",
                other
            ))),
        }
    }

    pub fn bool_or(&self, groups: Option<&GroupIndices>) -> DaftResult<Self> {
        use crate::array::ops::DaftBoolAggable;
        match self.data_type() {
            DataType::Boolean => {
                let downcasted = self.bool()?;
                Ok(match groups {
                    Some(groups) => downcasted.grouped_bool_or(groups)?,
                    None => downcasted.bool_or()?,
                }
                .into_series())
            }
            DataType::Null => {
                // Return a single null value for null type
                Ok(Self::full_null(
                    self.field().name.as_str(),
                    &DataType::Boolean,
                    1,
                ))
            }
            other => Err(DaftError::TypeError(format!(
                "bool_or is not implemented for type {}",
                other
            ))),
        }
    }
}

impl DaftSetAggable for Series {
    type Output = DaftResult<ListArray>;

    fn distinct(&self) -> Self::Output {
        let child_series = self.clone();
        let (deduped_series, _) = deduplicate_series(&child_series)?;

        let offsets = OffsetsBuffer::try_from(vec![0, deduped_series.len() as i64])?;
        let list_field = self.field().to_list_field()?;
        Ok(ListArray::new(list_field, deduped_series, offsets, None))
    }

    fn grouped_distinct(&self, groups: &GroupIndices) -> Self::Output {
        let series = self.clone();

        let mut offsets = Vec::with_capacity(groups.len() + 1);
        offsets.push(0);

        let mut growable = make_growable(
            self.name(),
            self.data_type(),
            vec![self],
            self.validity().is_some(),
            series.len(),
        );

        for group in groups {
            if group.is_empty() {
                offsets.push(*offsets.last().unwrap());
                continue;
            }

            let group_indices = UInt64Array::from(("", group.clone())).into_series();
            let group_series = series.take(&group_indices)?;

            let unique_indices = deduplicate_indices(&group_series)?;

            for &local_idx in &unique_indices {
                let orig_idx = group[local_idx as usize];
                growable.extend(0, orig_idx as usize, 1);
            }

            offsets.push(offsets.last().unwrap() + unique_indices.len() as i64);
        }

        let list_field = self.field().to_list_field()?;
        let result = ListArray::new(
            list_field,
            growable.build()?,
            OffsetsBuffer::try_from(offsets)?,
            None,
        );

        Ok(result)
    }
}
