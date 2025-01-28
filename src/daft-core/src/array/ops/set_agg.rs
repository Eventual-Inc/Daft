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
    println!(
        "Starting deduplication for series with length: {}",
        series.len()
    );

    // Try to get hashes for each element, error out if elements are not hashable
    println!("Computing hashes...");
    let hashes = series.hash(None).map_err(|_| {
        println!("Error: Failed to compute hashes - elements are not hashable");
        DaftError::ValueError(
            "Cannot perform set aggregation on elements that are not hashable".to_string(),
        )
    })?;
    println!("Successfully computed hashes");

    // Create a hashmap to track unique elements
    let mut seen_hashes = HashMap::new();
    let mut unique_indices = Vec::new();
    let mut has_null = false;

    // Iterate through hashes, keeping track of first occurrence
    println!("Starting hash iteration...");
    for (idx, hash) in hashes.into_iter().enumerate() {
        if !series.is_valid(idx) {
            println!("Found null at index {}", idx);
            // For actual nulls, only include the first one
            if !has_null {
                println!("First null encountered, including index {}", idx);
                has_null = true;
                unique_indices.push(idx as u64);
            }
        } else {
            // For non-null values (including empty values), use hash-based deduplication
            if let Some(hash) = hash {
                if let std::collections::hash_map::Entry::Vacant(e) = seen_hashes.entry(hash) {
                    println!("New unique value at index {} with hash {:?}", idx, hash);
                    e.insert(idx);
                    unique_indices.push(idx as u64);
                } else {
                    println!("Duplicate value at index {} with hash {:?}", idx, hash);
                }
            }
        }
    }

    println!("Found {} unique values", unique_indices.len());
    println!("Unique indices: {:?}", unique_indices);

    // Take only the unique elements in their original order
    let indices_array = UInt64Array::from(("", unique_indices.clone())).into_series();
    println!("Created indices array, taking values from original series...");
    let result = series.take(&indices_array)?;
    println!("Successfully created deduplicated series");

    Ok((result, unique_indices))
}

macro_rules! impl_daft_set_agg {
    () => {
        type Output = DaftResult<ListArray>;

        fn distinct(&self, include_nulls: bool) -> Self::Output {
            let mut child_series = self.clone().into_series();

            // Filter nulls if needed
            if !include_nulls {
                let not_null_mask = DaftNotNull::not_null(self)?.into_series();
                child_series = child_series.filter(not_null_mask.bool()?)?;
            }

            // Deduplicate the series
            child_series = deduplicate_series(&child_series)?.0;

            // Create list array from deduplicated series
            let offsets =
                arrow2::offset::OffsetsBuffer::try_from(vec![0, child_series.len() as i64])?;
            let list_field = self.field.to_list_field()?;
            Ok(ListArray::new(list_field, child_series, offsets, None))
        }

        fn grouped_distinct(&self, groups: &GroupIndices, include_nulls: bool) -> Self::Output {
            println!(
                "\nStarting grouped_distinct with include_nulls={}",
                include_nulls
            );

            // Convert self to series once at the start for reuse
            let series = self.clone().into_series();
            println!("Original series length: {}", series.len());

            // Create index mapping and filter nulls if needed
            let (filtered_series, index_mapping) = if !include_nulls {
                println!("Filtering nulls from series...");
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
                println!("Created index mapping: {:?}", new_positions);

                // Filter the series
                let filtered = series.filter(&not_null_array)?;
                println!("After null filtering, series length: {}", filtered.len());
                (filtered, new_positions)
            } else {
                // If including nulls, use identity mapping
                let identity_mapping = (0..series.len()).map(Some).collect();
                (series, identity_mapping)
            };

            // Create offsets for the final list array
            let mut offsets = Vec::with_capacity(groups.len() + 1);
            offsets.push(0);
            println!("Processing {} groups", groups.len());

            // Create growable array for building result
            let mut growable: Box<dyn Growable> = Box::new(Self::make_growable(
                self.name(),
                self.data_type(),
                vec![self],
                self.null_count() > 0,
                filtered_series.len(),
            ));

            // Process each group
            for (group_idx, group) in groups.iter().enumerate() {
                println!(
                    "\nProcessing group {} with {} elements",
                    group_idx,
                    group.len()
                );

                // Skip empty groups
                if group.is_empty() {
                    println!("Group {} is empty, skipping", group_idx);
                    offsets.push(*offsets.last().unwrap());
                    continue;
                }

                // Map group indices to filtered indices
                let filtered_group: Vec<u64> = group
                    .iter()
                    .filter_map(|&idx| index_mapping[idx as usize].map(|x| x as u64))
                    .collect();

                println!("Original group indices: {:?}", group);
                println!("Filtered group indices: {:?}", filtered_group);

                if filtered_group.is_empty() {
                    println!("Group {} has no non-null elements, skipping", group_idx);
                    offsets.push(*offsets.last().unwrap());
                    continue;
                }

                // Take the slice for this group and convert to series
                let group_indices = UInt64Array::from(("", filtered_group.clone())).into_series();
                let group_series = filtered_series.take(&group_indices)?;
                println!(
                    "Group {} series created with length {}",
                    group_idx,
                    group_series.len()
                );

                // Deduplicate the group's values and get their original indices
                println!("Deduplicating group {}...", group_idx);
                let (_, unique_indices) = deduplicate_series(&group_series)?;
                println!(
                    "Group {} has {} unique values",
                    group_idx,
                    unique_indices.len()
                );

                // Add deduplicated values to growable array using the original indices
                println!(
                    "Adding unique values to result array for group {}",
                    group_idx
                );
                for (i, &local_idx) in unique_indices.iter().enumerate() {
                    let filtered_idx = filtered_group[local_idx as usize];
                    // Find the original index that maps to this filtered index
                    for (orig_idx, &mapped_idx) in index_mapping.iter().enumerate() {
                        if mapped_idx == Some(filtered_idx as usize) {
                            println!(
                                "  Value {}: local_idx={}, filtered_idx={}, orig_idx={}",
                                i, local_idx, filtered_idx, orig_idx
                            );
                            growable.extend(0, orig_idx, 1);
                            break;
                        }
                    }
                }

                // Update offsets
                offsets.push(offsets.last().unwrap() + unique_indices.len() as i64);
                println!("Updated offsets for group {}: {:?}", group_idx, offsets);
            }

            // Create the final list array
            println!("\nBuilding final list array");
            let list_field = self.field.to_list_field()?;
            println!("Final offsets: {:?}", offsets);
            let result = ListArray::new(
                list_field,
                growable.build()?,
                arrow2::offset::OffsetsBuffer::try_from(offsets)?,
                None,
            );
            println!("Successfully built list array");

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
