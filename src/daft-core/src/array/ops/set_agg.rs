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
    let hashes = series.hash(None).map_err(|_| {
        DaftError::ValueError(
            "Cannot perform set aggregation on elements that are not hashable".to_string(),
        )
    })?;

    let mut seen_hashes = HashMap::new();
    let mut unique_indices = Vec::new();
    let mut has_null = false;

    for (idx, hash) in hashes.into_iter().enumerate() {
        if !series.is_valid(idx) {
            if !has_null {
                has_null = true;
                unique_indices.push(idx as u64);
            }
        } else if let Some(hash) = hash {
            if let std::collections::hash_map::Entry::Vacant(e) = seen_hashes.entry(hash) {
                e.insert(idx);
                unique_indices.push(idx as u64);
            }
        }
    }

    let indices_array = UInt64Array::from(("", unique_indices.clone())).into_series();
    let result = series.take(&indices_array)?;

    Ok((result, unique_indices))
}

macro_rules! impl_daft_set_agg {
    () => {
        type Output = DaftResult<ListArray>;

        fn distinct(&self, ignore_nulls: bool) -> Self::Output {
            let mut child_series = self.clone().into_series();

            if ignore_nulls {
                let not_null_mask = DaftNotNull::not_null(self)?.into_series();
                child_series = child_series.filter(not_null_mask.bool()?)?;
            }

            let (deduped_series, _) = deduplicate_series(&child_series)?;

            let offsets =
                arrow2::offset::OffsetsBuffer::try_from(vec![0, deduped_series.len() as i64])?;
            let list_field = self.field.to_list_field()?;
            Ok(ListArray::new(list_field, deduped_series, offsets, None))
        }

        fn grouped_distinct(&self, groups: &GroupIndices, ignore_nulls: bool) -> Self::Output {
            let series = self.clone().into_series();

            let (filtered_series, index_mapping) = if ignore_nulls {
                let not_null_mask = DaftNotNull::not_null(self)?.into_series();
                let not_null_array = not_null_mask.bool()?;

                let mut new_positions = vec![None; series.len()];
                let mut current_new_pos = 0;

                for i in 0..not_null_array.len() {
                    if not_null_array.get(i).unwrap_or(false) {
                        new_positions[i] = Some(current_new_pos);
                        current_new_pos += 1;
                    }
                }

                let filtered = series.filter(&not_null_array)?;
                (filtered, new_positions)
            } else {
                let identity_mapping = (0..series.len()).map(Some).collect();
                (series, identity_mapping)
            };

            let mut offsets = Vec::with_capacity(groups.len() + 1);
            offsets.push(0);

            let mut growable: Box<dyn Growable> = Box::new(Self::make_growable(
                self.name(),
                self.data_type(),
                vec![self],
                self.null_count() > 0,
                filtered_series.len(),
            ));

            for group in groups.iter() {
                if group.is_empty() {
                    offsets.push(*offsets.last().unwrap());
                    continue;
                }

                let filtered_group: Vec<u64> = group
                    .iter()
                    .filter_map(|&idx| index_mapping[idx as usize].map(|x| x as u64))
                    .collect();

                if filtered_group.is_empty() {
                    offsets.push(*offsets.last().unwrap());
                    continue;
                }

                let group_indices = UInt64Array::from(("", filtered_group.clone())).into_series();
                let group_series = filtered_series.take(&group_indices)?;

                let (_, unique_indices) = deduplicate_series(&group_series)?;

                for &local_idx in unique_indices.iter() {
                    let filtered_idx = filtered_group[local_idx as usize];
                    for (orig_idx, &mapped_idx) in index_mapping.iter().enumerate() {
                        if mapped_idx == Some(filtered_idx as usize) {
                            growable.extend(0, orig_idx, 1);
                            break;
                        }
                    }
                }

                offsets.push(offsets.last().unwrap() + unique_indices.len() as i64);
            }

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
