use std::collections::HashMap;

use common_error::{DaftError, DaftResult};

use super::{DaftNotNull, DaftSetAggable, GroupIndices};
use crate::{
    array::{
        growable::{Growable, GrowableArray},
        DataArray, FixedSizeListArray, ListArray, StructArray,
    },
    datatypes::{DaftArrowBackedType, UInt64Array},
    series::{IntoSeries, Series},
};

fn deduplicate_series(series: &Series) -> DaftResult<(Series, Vec<u64>)> {
    // Try to get hashes for each element, error out if elements are not hashable
    let hashes = series.hash(None).map_err(|_| {
        DaftError::ValueError(
            "Cannot perform set aggregation on elements that are not hashable".to_string(),
        )
    })?;

    // Create a hashmap to track unique elements
    let mut seen_hashes = HashMap::new();
    let mut unique_indices = Vec::new();
    let mut has_null = false;

    // Iterate through hashes, keeping track of first occurrence
    for (idx, hash) in hashes.into_iter().enumerate() {
        if !series.is_valid(idx) {
            // For actual nulls, only include the first one
            if !has_null {
                has_null = true;
                unique_indices.push(idx as u64);
            }
        } else {
            // For non-null values (including empty values), use hash-based deduplication
            if let Some(hash) = hash {
                if let std::collections::hash_map::Entry::Vacant(e) = seen_hashes.entry(hash) {
                    e.insert(idx);
                    unique_indices.push(idx as u64);
                }
            }
        }
    }

    // Take only the unique elements in their original order
    let indices_array = UInt64Array::from(("", unique_indices.clone())).into_series();
    let result = series.take(&indices_array)?;

    Ok((result, unique_indices))
}

macro_rules! impl_daft_set_agg {
    () => {
        type Output = DaftResult<ListArray>;

        fn distinct(&self, ignore_nulls: bool) -> Self::Output {
            let mut child_series = self.clone().into_series();

            // Filter nulls if needed
            if ignore_nulls {
                let not_null_mask = DaftNotNull::not_null(self)?.into_series();
                child_series = child_series.filter(not_null_mask.bool()?)?;
            }

            // Deduplicate the series
            let (deduped_series, _) = deduplicate_series(&child_series)?;

            // Create list array from deduplicated series
            let offsets =
                arrow2::offset::OffsetsBuffer::try_from(vec![0, deduped_series.len() as i64])?;
            let list_field = self.field.to_list_field()?;
            Ok(ListArray::new(list_field, deduped_series, offsets, None))
        }

        fn grouped_distinct(&self, groups: &GroupIndices, ignore_nulls: bool) -> Self::Output {
            // Convert self to series once at the start for reuse
            let series = self.clone().into_series();

            // Create index mapping and filter nulls if needed
            let (filtered_series, index_mapping) = if ignore_nulls {
                let not_null_mask = DaftNotNull::not_null(self)?.into_series();
                let not_null_array = not_null_mask.bool()?;

                // Create mapping from original indices to new indices
                let mut new_positions = vec![None; series.len()];
                let mut current_new_pos = 0;

                for i in 0..not_null_array.len() {
                    if not_null_array.get(i).unwrap_or(false) {
                        new_positions[i] = Some(current_new_pos);
                        current_new_pos += 1;
                    }
                }

                // Filter the series
                let filtered = series.filter(&not_null_array)?;
                (filtered, new_positions)
            } else {
                // If including nulls, use identity mapping
                let identity_mapping = (0..series.len()).map(Some).collect();
                (series, identity_mapping)
            };

            // Create offsets for the final list array
            let mut offsets = Vec::with_capacity(groups.len() + 1);
            offsets.push(0);

            // Create growable array for building result
            let mut growable: Box<dyn Growable> = Box::new(Self::make_growable(
                self.name(),
                self.data_type(),
                vec![self],
                self.null_count() > 0,
                filtered_series.len(),
            ));

            // Process each group
            for group in groups.iter() {
                // Skip empty groups
                if group.is_empty() {
                    offsets.push(*offsets.last().unwrap());
                    continue;
                }

                // Map group indices to filtered indices
                let filtered_group: Vec<u64> = group
                    .iter()
                    .filter_map(|&idx| index_mapping[idx as usize].map(|x| x as u64))
                    .collect();

                if filtered_group.is_empty() {
                    offsets.push(*offsets.last().unwrap());
                    continue;
                }

                // Take the slice for this group and convert to series
                let group_indices = UInt64Array::from(("", filtered_group.clone())).into_series();
                let group_series = filtered_series.take(&group_indices)?;

                // Deduplicate the group's values and get their original indices
                let (_, unique_indices) = deduplicate_series(&group_series)?;

                // Add deduplicated values to growable array using the original indices
                for &local_idx in unique_indices.iter() {
                    let filtered_idx = filtered_group[local_idx as usize];
                    // Find the original index that maps to this filtered index
                    for (orig_idx, &mapped_idx) in index_mapping.iter().enumerate() {
                        if mapped_idx == Some(filtered_idx as usize) {
                            growable.extend(0, orig_idx, 1);
                            break;
                        }
                    }
                }

                // Update offsets
                offsets.push(offsets.last().unwrap() + unique_indices.len() as i64);
            }

            // Create the final list array
            let list_field = self.field.to_list_field()?;
            let result = ListArray::new(
                list_field,
                growable.build()?,
                arrow2::offset::OffsetsBuffer::try_from(offsets)?,
                None,
            );

            Ok(result)
        }
    };
}

impl<T> DaftSetAggable for DataArray<T>
where
    T: DaftArrowBackedType,
    Self: IntoSeries,
    Self: GrowableArray,
{
    impl_daft_set_agg!();
}

impl DaftSetAggable for ListArray {
    impl_daft_set_agg!();
}

impl DaftSetAggable for FixedSizeListArray {
    impl_daft_set_agg!();
}

impl DaftSetAggable for StructArray {
    impl_daft_set_agg!();
}

#[cfg(feature = "python")]
impl DaftSetAggable for crate::datatypes::PythonArray {
    type Output = DaftResult<Self>;

    fn distinct(&self, _: bool) -> Self::Output {
        Err(DaftError::ValueError(
            "Cannot perform set aggregation on elements that are not hashable".to_string(),
        ))
    }

    fn grouped_distinct(&self, _: &GroupIndices, _: bool) -> Self::Output {
        Err(DaftError::ValueError(
            "Cannot perform set aggregation on elements that are not hashable".to_string(),
        ))
    }
}
