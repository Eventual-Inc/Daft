use std::sync::Arc;

use arrow::buffer::OffsetBuffer;
use common_error::{DaftError, DaftResult};
use daft_core::{
    array::{ListArray, ops::GroupIndices},
    prelude::{CountMode, DataType, Field, Int64Array, UInt64Array, Utf8Array},
    series::{IntoSeries, Series},
};

use crate::kernels::{ListArrayAggExtension, ListArrayExtension};

pub trait SeriesListExtension: Sized {
    fn count_distinct(&self, groups: Option<&GroupIndices>) -> DaftResult<Self>;
    fn list_value_counts(&self) -> DaftResult<Self>;
    fn list_bool_and(&self) -> DaftResult<Self>;
    fn list_bool_or(&self) -> DaftResult<Self>;
    fn explode(&self, ignore_empty_and_null: bool) -> DaftResult<Self>;
    fn list_flatten(&self) -> DaftResult<Self>;
    fn list_count(&self, mode: CountMode) -> DaftResult<UInt64Array>;
    fn join(&self, delimiter: &Utf8Array) -> DaftResult<Utf8Array>;
    fn list_get(&self, idx: &Self, default: &Self) -> DaftResult<Self>;
    fn list_chunk(&self, size: usize) -> DaftResult<Self>;
    fn list_sum(&self) -> DaftResult<Self>;
    fn list_mean(&self) -> DaftResult<Self>;
    fn list_min(&self) -> DaftResult<Self>;
    fn list_max(&self) -> DaftResult<Self>;
    fn list_sort(&self, desc: &Self, nulls_first: &Self) -> DaftResult<Self>;
    fn list_count_distinct(&self) -> DaftResult<Self>;
    fn list_fill(&self, num: &Int64Array) -> DaftResult<Self>;
    fn list_distinct(&self) -> DaftResult<Self>;
    fn list_append(&self, other: &Self) -> DaftResult<Self>;
    fn list_contains(&self, item: &Self) -> DaftResult<Self>;
    fn list_compact(&self) -> DaftResult<Self>;
    fn list_position(&self, item: &Self) -> DaftResult<Self>;
    fn list_except(&self, other: &Self) -> DaftResult<Self>;
    fn list_intersect(&self, other: &Self) -> DaftResult<Self>;
    fn list_union(&self, other: &Self) -> DaftResult<Self>;
}

impl SeriesListExtension for Series {
    fn count_distinct(&self, groups: Option<&GroupIndices>) -> DaftResult<Self> {
        let series = self.agg_list(groups)?.list_count_distinct()?;
        Ok(series)
    }
    fn list_value_counts(&self) -> DaftResult<Self> {
        let series = match self.data_type() {
            DataType::List(_) => self.list()?.value_counts(),
            DataType::FixedSizeList(..) => self.fixed_size_list()?.value_counts(),
            dt => {
                return Err(DaftError::TypeError(format!(
                    "List contains not implemented for {}",
                    dt
                )));
            }
        }?
        .into_series();

        Ok(series)
    }

    fn list_bool_and(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::List(_) => Ok(self.list()?.list_bool_and()?.into_series()),
            DataType::FixedSizeList(..) => {
                Ok(self.fixed_size_list()?.list_bool_and()?.into_series())
            }
            dt => Err(DaftError::TypeError(format!(
                "list_bool_and not implemented for {}",
                dt
            ))),
        }
    }

    fn list_bool_or(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::List(_) => Ok(self.list()?.list_bool_or()?.into_series()),
            DataType::FixedSizeList(..) => {
                Ok(self.fixed_size_list()?.list_bool_or()?.into_series())
            }
            dt => Err(DaftError::TypeError(format!(
                "list_bool_or not implemented for {}",
                dt
            ))),
        }
    }

    fn explode(&self, ignore_empty_and_null: bool) -> DaftResult<Self> {
        match self.data_type() {
            DataType::List(_) => self.list()?.explode(ignore_empty_and_null),
            DataType::FixedSizeList(..) => self.fixed_size_list()?.explode(ignore_empty_and_null),
            dt => Err(DaftError::TypeError(format!(
                "explode not implemented for {}",
                dt
            ))),
        }
    }

    fn list_flatten(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::List(_) => self.list()?.list_flatten().map(IntoSeries::into_series),
            DataType::FixedSizeList(..) => self
                .fixed_size_list()?
                .list_flatten()
                .map(IntoSeries::into_series),
            dt => Err(DaftError::TypeError(format!(
                "list flatten not implemented for {}",
                dt
            ))),
        }
    }

    fn list_count(&self, mode: CountMode) -> DaftResult<UInt64Array> {
        match self.data_type() {
            DataType::List(_) => self.list()?.count(mode),
            DataType::FixedSizeList(..) => self.fixed_size_list()?.count(mode),
            DataType::Embedding(..) | DataType::FixedShapeImage(..) => {
                self.as_physical()?.list_count(mode)
            }
            DataType::Image(..) => {
                let struct_array = self.as_physical()?;
                let data_array = struct_array.struct_()?.children[0].list().unwrap();
                let offsets = data_array.offsets();
                UInt64Array::from_values(self.name(), offsets.lengths().map(|l| l as u64))
                    .with_nulls(data_array.nulls().cloned())
            }
            dt => Err(DaftError::TypeError(format!(
                "Count not implemented for {}",
                dt
            ))),
        }
    }

    fn join(&self, delimiter: &Utf8Array) -> DaftResult<Utf8Array> {
        match self.data_type() {
            DataType::List(_) => self.list()?.join(delimiter),
            DataType::FixedSizeList(..) => self.fixed_size_list()?.join(delimiter),
            dt => Err(DaftError::TypeError(format!(
                "Join not implemented for {}",
                dt
            ))),
        }
    }

    fn list_get(&self, idx: &Self, default: &Self) -> DaftResult<Self> {
        let idx = idx.cast(&DataType::Int64)?;
        let idx_arr = idx.i64().unwrap();

        match self.data_type() {
            DataType::List(_) => self.list()?.get_children(idx_arr, default),
            DataType::FixedSizeList(..) => self.fixed_size_list()?.get_children(idx_arr, default),
            dt => Err(DaftError::TypeError(format!(
                "Get not implemented for {}",
                dt
            ))),
        }
    }

    fn list_chunk(&self, size: usize) -> DaftResult<Self> {
        match self.data_type() {
            DataType::List(_) => self.list()?.get_chunks(size),
            DataType::FixedSizeList(..) => self.fixed_size_list()?.get_chunks(size),
            dt => Err(DaftError::TypeError(format!(
                "list chunk not implemented for {dt}"
            ))),
        }
    }

    fn list_sum(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::List(_) => self.list()?.sum(),
            DataType::FixedSizeList(..) => self.fixed_size_list()?.sum(),
            dt => Err(DaftError::TypeError(format!(
                "Sum not implemented for {}",
                dt
            ))),
        }
    }

    fn list_mean(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::List(_) => self.list()?.mean(),
            DataType::FixedSizeList(..) => self.fixed_size_list()?.mean(),
            dt => Err(DaftError::TypeError(format!(
                "Mean not implemented for {}",
                dt
            ))),
        }
    }

    fn list_min(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::List(_) => self.list()?.min(),
            DataType::FixedSizeList(..) => self.fixed_size_list()?.min(),
            dt => Err(DaftError::TypeError(format!(
                "Min not implemented for {}",
                dt
            ))),
        }
    }

    fn list_max(&self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::List(_) => self.list()?.max(),
            DataType::FixedSizeList(..) => self.fixed_size_list()?.max(),
            dt => Err(DaftError::TypeError(format!(
                "Max not implemented for {}",
                dt
            ))),
        }
    }

    fn list_sort(&self, desc: &Self, nulls_first: &Self) -> DaftResult<Self> {
        let desc_arr = desc.bool()?;
        let nulls_first = nulls_first.bool()?;

        match self.data_type() {
            DataType::List(_) => Ok(self.list()?.list_sort(desc_arr, nulls_first)?.into_series()),
            DataType::FixedSizeList(..) => Ok(self
                .fixed_size_list()?
                .list_sort(desc_arr, nulls_first)?
                .into_series()),
            dt => Err(DaftError::TypeError(format!(
                "List sort not implemented for {}",
                dt
            ))),
        }
    }

    /// Given a series of `List` or `FixedSizeList`, return the count of distinct elements in the list.
    ///
    /// # Note
    /// `NULL` values are not counted.
    ///
    /// # Example
    /// ```txt
    /// [[1, 2, 3], [1, 1, 1], [NULL, NULL, 5]] -> [3, 1, 1]
    /// ```
    fn list_count_distinct(&self) -> DaftResult<Self> {
        let field = Field::new(self.name(), DataType::UInt64);
        match self.data_type() {
            DataType::List(..) => {
                let iter = self.list()?.into_iter().map(|sub_series| {
                    let sub_series = sub_series?;
                    let length = sub_series
                        .build_probe_table_without_nulls()
                        .expect("Building the probe table should always work")
                        .len() as u64;
                    Some(length)
                });
                Ok(UInt64Array::from_iter(field, iter).into_series())
            }
            DataType::FixedSizeList(..) => {
                let iter = self.fixed_size_list()?.into_iter().map(|sub_series| {
                    let sub_series = sub_series?;
                    let length = sub_series
                        .build_probe_table_without_nulls()
                        .expect("Building the probe table should always work")
                        .len() as u64;
                    Some(length)
                });
                Ok(UInt64Array::from_iter(field, iter).into_series())
            }
            _ => Err(DaftError::TypeError(format!(
                "List count distinct not implemented for {}",
                self.data_type()
            ))),
        }
    }

    /// Given a series of data T, repeat each data T with num times to create a list, returns
    /// a series of repeated list.
    /// # Example
    /// ```txt
    /// repeat([1, 2, 3], [2, 0, 1]) --> [[1, 1], [], [3]]
    /// ```
    fn list_fill(&self, num: &Int64Array) -> DaftResult<Self> {
        crate::kernels::list_fill(self, num).map(|arr| arr.into_series())
    }

    /// Returns a list of unique elements in each list, preserving order of first occurrence and ignoring nulls.
    ///
    /// # Example
    /// ```txt
    /// [[1, 2, 3], [1, 1, 1], [NULL, NULL, 5]] -> [[1, 2, 3], [1], [5]]
    /// ```
    fn list_distinct(&self) -> DaftResult<Self> {
        let input = if let DataType::FixedSizeList(inner_type, _) = self.data_type() {
            self.cast(&DataType::List(inner_type.clone()))?
        } else {
            self.clone()
        };

        let list = input.list()?;
        let mut offsets = Vec::new();
        offsets.push(0i64);
        let mut current_offset = 0i64;

        // Collect indices of unique elements from each sub-series
        let mut take_indices = Vec::new();
        let list_offsets = list.offsets();
        for (i, sub_series) in list.into_iter().enumerate() {
            let start_offset = list_offsets.get(i).unwrap();
            if let Some(sub_series) = sub_series {
                let probe_table = sub_series.build_probe_table_without_nulls()?;
                let indices: Vec<_> = probe_table.keys().map(|k| k.idx).collect();
                let unique_count = indices.len();
                for idx in indices {
                    take_indices.push((*start_offset as usize + idx as usize) as u64);
                }
                current_offset += unique_count as i64;
            }
            offsets.push(current_offset);
        }

        // Use take to extract unique elements
        let take_indices_array = UInt64Array::from_vec("indices", take_indices);
        let distinct_child = list.flat_child.take(&take_indices_array)?;

        let list_array = ListArray::new(
            Arc::new(Field::new(input.name(), input.data_type().clone())),
            distinct_child,
            OffsetBuffer::new(offsets.into()),
            input.nulls().cloned(),
        );

        Ok(list_array.into_series())
    }

    fn list_append(&self, other: &Self) -> DaftResult<Self> {
        let input = if let DataType::FixedSizeList(inner_type, _) = self.data_type() {
            self.cast(&DataType::List(inner_type.clone()))?
        } else {
            self.clone()
        };
        let input = input.list()?;

        let other = other.cast(input.child_data_type())?;

        // Collect indices for take operation: original list elements + appended element
        let mut take_indices = Vec::new();
        let offsets = input.offsets();
        let mut new_lengths = Vec::with_capacity(input.len());
        let flat_child_len = input.flat_child.len();

        for i in 0..self.len() {
            if input.is_valid(i) {
                let start = *offsets.get(i).unwrap();
                let end = *offsets.get(i + 1).unwrap();
                let list_size = end - start;
                // Add indices from original list
                for idx in start..end {
                    take_indices.push(idx as u64);
                }
                // Add index from other series (appended element)
                take_indices.push((flat_child_len + i) as u64);
                new_lengths.push((list_size + 1) as usize);
            } else {
                // For null lists, just append the element from other
                take_indices.push((flat_child_len + i) as u64);
                new_lengths.push(1);
            }
        }

        // Concatenate flat_child and other, then take the selected indices
        let concatenated = Self::concat(&[&input.flat_child, &other])?;
        let take_indices_array = UInt64Array::from_vec("indices", take_indices);
        let child_arr = concatenated.take(&take_indices_array)?;

        let new_offsets = OffsetBuffer::from_lengths(new_lengths.into_iter());
        let list_array = ListArray::new(
            input.field.clone(),
            child_arr,
            new_offsets.into(),
            None, // All outputs are valid because of the append
        );

        Ok(list_array.into_series())
    }

    fn list_contains(&self, item: &Self) -> DaftResult<Self> {
        match self.data_type() {
            DataType::List(_) => Ok(self.list()?.list_contains(item)?.into_series()),
            DataType::FixedSizeList(..) => {
                Ok(self.fixed_size_list()?.list_contains(item)?.into_series())
            }
            dt => Err(DaftError::TypeError(format!(
                "List contains not implemented for {dt}"
            ))),
        }
    }

    /// Removes null values from each list, preserving the order of remaining elements.
    ///
    /// # Example
    /// ```txt
    /// [[1, NULL, 2, NULL, 3], [NULL, NULL], [1, 2]] -> [[1, 2, 3], [], [1, 2]]
    /// ```
    fn list_compact(&self) -> DaftResult<Self> {
        let input = if let DataType::FixedSizeList(inner_type, _) = self.data_type() {
            self.cast(&DataType::List(inner_type.clone()))?
        } else {
            self.clone()
        };

        let list = input.list()?;
        let mut offsets = Vec::with_capacity(list.len() + 1);
        offsets.push(0i64);
        let mut current_offset = 0i64;

        // Collect indices of non-null elements from the flat child.
        let mut take_indices = Vec::new();
        let list_offsets = list.offsets();
        for i in 0..list.len() {
            let start = *list_offsets.get(i).unwrap();
            let end = *list_offsets.get(i + 1).unwrap();
            if list.is_valid(i) {
                for idx in start..end {
                    if list.flat_child.is_valid(idx as usize) {
                        take_indices.push(idx as u64);
                        current_offset += 1;
                    }
                }
            }
            offsets.push(current_offset);
        }

        let take_indices_array = UInt64Array::from_vec("indices", take_indices);
        let compact_child = list.flat_child.take(&take_indices_array)?;

        let list_array = ListArray::new(
            Arc::new(Field::new(input.name(), input.data_type().clone())),
            compact_child,
            OffsetBuffer::new(offsets.into()),
            input.nulls().cloned(),
        );

        Ok(list_array.into_series())
    }

    /// Returns the 1-based position of the first occurrence of `item` in each list,
    /// or 0 if the element is not found. Returns NULL if either the list or item is NULL.
    fn list_position(&self, item: &Self) -> DaftResult<Self> {
        let input = if let DataType::FixedSizeList(inner_type, _) = self.data_type() {
            self.cast(&DataType::List(inner_type.clone()))?
        } else {
            self.clone()
        };

        let list = input.list()?;

        // If the list element type is null, no value can ever be found.
        if list.flat_child.data_type() == &DataType::Null {
            let mut values: Vec<i64> = Vec::with_capacity(list.len());
            let mut validity: Vec<bool> = Vec::with_capacity(list.len());
            for i in 0..list.len() {
                let valid = list.is_valid(i) && item.is_valid(i);
                values.push(0);
                validity.push(valid);
            }
            let field = Field::new(self.name(), DataType::Int64);
            let arr = Int64Array::from_iter(
                field,
                values
                    .into_iter()
                    .zip(validity)
                    .map(|(v, ok)| if ok { Some(v) } else { None }),
            );
            return Ok(arr.into_series());
        }

        let item = item.cast(list.flat_child.data_type())?;

        // Use the same hash + comparator approach as list_contains.
        let item_hashes = item.hash(None)?;
        let child_hashes = list.flat_child.hash(None)?;
        let child_arrow = list.flat_child.to_arrow()?;
        let item_arrow = item.to_arrow()?;
        // Surface a typed error instead of panicking when the element type
        // is not orderable/comparable by arrow's `make_comparator`.
        let comparator = arrow::array::make_comparator(
            child_arrow.as_ref(),
            item_arrow.as_ref(),
            Default::default(),
        )
        .map_err(|e| {
            DaftError::TypeError(format!(
                "list_position: cannot compare elements of type {}: {}",
                list.flat_child.data_type(),
                e
            ))
        })?;

        let list_offsets = list.offsets();
        let mut values: Vec<i64> = Vec::with_capacity(list.len());
        let mut validity: Vec<bool> = Vec::with_capacity(list.len());
        for list_idx in 0..list.len() {
            if !list.is_valid(list_idx) || !item.is_valid(list_idx) {
                values.push(0);
                validity.push(false);
                continue;
            }
            let item_hash = item_hashes.get(list_idx).unwrap();
            let start = *list_offsets.get(list_idx).unwrap();
            let end = *list_offsets.get(list_idx + 1).unwrap();
            let mut found_pos: i64 = 0;
            for (offset, elem_idx) in (start..end).enumerate() {
                let elem_idx = elem_idx as usize;
                if child_arrow.is_null(elem_idx) {
                    continue;
                }
                let elem_hash = child_hashes.get(elem_idx).unwrap();
                if elem_hash == item_hash && comparator(elem_idx, list_idx).is_eq() {
                    found_pos = (offset as i64) + 1;
                    break;
                }
            }
            values.push(found_pos);
            validity.push(true);
        }

        let field = Field::new(self.name(), DataType::Int64);
        let arr = Int64Array::from_iter(
            field,
            values
                .into_iter()
                .zip(validity)
                .map(|(v, ok)| if ok { Some(v) } else { None }),
        );
        Ok(arr.into_series())
    }

    /// Returns the elements in `self` that are not in `other`, with duplicates removed.
    /// Returns the elements in `self` that are not in `other`, with duplicates removed.
    /// Uses null-safe-equal semantics: a null element in `self` is dropped only if
    /// `other` also contains a null. The order of first occurrence is preserved from `self`.
    fn list_except(&self, other: &Self) -> DaftResult<Self> {
        list_set_op(self, other, SetOp::Except)
    }

    /// Returns the elements that are in both `self` and `other`, with duplicates removed.
    /// Uses null-safe-equal semantics: a null is kept in the result only if both
    /// inputs contain a null. The order of first occurrence is preserved from `self`.
    fn list_intersect(&self, other: &Self) -> DaftResult<Self> {
        list_set_op(self, other, SetOp::Intersect)
    }

    /// Returns the union of elements in `self` and `other`, with duplicates removed.
    /// Uses null-safe-equal semantics: if either input contains a null, the result
    /// contains a single null. The order of first occurrence (from `self` then `other`)
    /// is preserved.
    fn list_union(&self, other: &Self) -> DaftResult<Self> {
        list_set_op(self, other, SetOp::Union)
    }
}

#[derive(Clone, Copy)]
enum SetOp {
    Except,
    Intersect,
    Union,
}

/// Common helper that performs per-row set operations between two list series.
/// Both sides are first normalized to `List` and promoted to a common element
/// supertype. Spark-compatible null-safe-equal semantics are used: NULL elements
/// inside the lists participate as a regular value (e.g. NULL is equal to NULL).
fn list_set_op(lhs: &Series, rhs: &Series, op: SetOp) -> DaftResult<Series> {
    let lhs_norm = if let DataType::FixedSizeList(inner_type, _) = lhs.data_type() {
        lhs.cast(&DataType::List(inner_type.clone()))?
    } else {
        lhs.clone()
    };
    let rhs_norm = if let DataType::FixedSizeList(inner_type, _) = rhs.data_type() {
        rhs.cast(&DataType::List(inner_type.clone()))?
    } else {
        rhs.clone()
    };

    let lhs_list = lhs_norm.list()?;
    let rhs_list = rhs_norm.list()?;

    if lhs_list.len() != rhs_list.len() {
        return Err(DaftError::ValueError(format!(
            "list set operation expects equal-length inputs, got {} vs {}",
            lhs_list.len(),
            rhs_list.len()
        )));
    }

    // Promote both children to a common supertype so mixed numeric inputs like
    // `array_union(int_list, float_list)` work, matching Spark behavior.
    let lhs_child_dtype = lhs_list.flat_child.data_type();
    let rhs_child_dtype = rhs_list.flat_child.data_type();
    let target_inner_dtype = if lhs_child_dtype == rhs_child_dtype {
        lhs_child_dtype.clone()
    } else {
        daft_core::utils::supertype::try_get_supertype(lhs_child_dtype, rhs_child_dtype)?
    };

    let lhs_child_casted = if lhs_child_dtype == &target_inner_dtype {
        lhs_list.flat_child.clone()
    } else {
        lhs_list.flat_child.cast(&target_inner_dtype)?
    };
    let rhs_child_casted = if rhs_child_dtype == &target_inner_dtype {
        rhs_list.flat_child.clone()
    } else {
        rhs_list.flat_child.cast(&target_inner_dtype)?
    };

    // Use the concatenation of the two (promoted) flat children as the source for
    // take indices. We append rhs_child after lhs_child, so any take index
    // >= lhs_child.len() refers to rhs.
    let lhs_child_len = lhs_child_casted.len();
    let combined = Series::concat(&[&lhs_child_casted, &rhs_child_casted])?;

    let lhs_offsets = lhs_list.offsets();
    let rhs_offsets = rhs_list.offsets();

    let mut take_indices: Vec<u64> = Vec::new();
    let mut out_offsets: Vec<i64> = Vec::with_capacity(lhs_list.len() + 1);
    out_offsets.push(0);
    let mut current_offset: i64 = 0;

    let lhs_outer_nulls = lhs_list.nulls();
    let rhs_outer_nulls = rhs_list.nulls();
    let mut out_validity: Vec<bool> = Vec::with_capacity(lhs_list.len());

    for i in 0..lhs_list.len() {
        let lhs_valid = lhs_outer_nulls.is_none_or(|n| n.is_valid(i));
        let rhs_valid = rhs_outer_nulls.is_none_or(|n| n.is_valid(i));

        // Spark semantics: if either list is NULL, the result is NULL.
        if !lhs_valid || !rhs_valid {
            out_validity.push(false);
            out_offsets.push(current_offset);
            continue;
        }
        out_validity.push(true);

        let l_start = *lhs_offsets.get(i).unwrap() as usize;
        let l_end = *lhs_offsets.get(i + 1).unwrap() as usize;
        let r_start = *rhs_offsets.get(i).unwrap() as usize;
        let r_end = *rhs_offsets.get(i + 1).unwrap() as usize;

        let lhs_sub = lhs_child_casted.slice(l_start, l_end)?;
        let rhs_sub = rhs_child_casted.slice(r_start, r_end)?;

        // Build probe tables for both sides (these intentionally exclude nulls;
        // null-equality is handled separately below).
        let lhs_probe = lhs_sub.build_probe_table_without_nulls()?;
        let rhs_probe = rhs_sub.build_probe_table_without_nulls()?;

        // Track null-element presence on each side so we can preserve nulls as a
        // first-class value (Spark null-safe-equal semantics).
        let lhs_first_null_local: Option<usize> =
            (0..lhs_sub.len()).find(|&j| !lhs_sub.is_valid(j));
        let rhs_first_null_local: Option<usize> =
            (0..rhs_sub.len()).find(|&j| !rhs_sub.is_valid(j));
        let rhs_has_null: bool = rhs_first_null_local.is_some();

        // Build an ordered list of unique lhs items (by first-occurrence local
        // index), treating null as a regular value. Each entry is
        // (local_idx, is_null). The probe table iteration preserves
        // first-occurrence order for non-null elements; we splice in the null
        // at its original position to preserve the input ordering (Spark
        // semantics).
        let mut lhs_unique_items: Vec<(usize, bool)> =
            lhs_probe.keys().map(|k| (k.idx as usize, false)).collect();
        if let Some(null_local) = lhs_first_null_local {
            // Insert null at the position that maintains ascending local_idx
            // order. Non-null entries are already in ascending order.
            let insert_pos = lhs_unique_items
                .iter()
                .position(|&(idx, _)| idx > null_local)
                .unwrap_or(lhs_unique_items.len());
            lhs_unique_items.insert(insert_pos, (null_local, true));
        }

        // Determine which lhs unique items to keep based on op, preserving
        // their original first-occurrence order.
        let mut kept_count = 0i64;
        for &(local_idx, is_null) in &lhs_unique_items {
            let keep = if is_null {
                // Null-safe-equal semantics for nulls in lhs:
                //   - Except:    keep null only if rhs has NO null.
                //   - Intersect: keep null only if rhs also has a null.
                //   - Union:     always keep null from lhs.
                match op {
                    SetOp::Except => !rhs_has_null,
                    SetOp::Intersect => rhs_has_null,
                    SetOp::Union => true,
                }
            } else {
                let in_rhs = probe_contains(&rhs_sub, &lhs_sub, local_idx, &rhs_probe)?;
                match op {
                    SetOp::Except => !in_rhs,
                    SetOp::Intersect => in_rhs,
                    SetOp::Union => true,
                }
            };
            if keep {
                let absolute_idx = l_start + local_idx;
                take_indices.push(absolute_idx as u64);
                kept_count += 1;
            }
        }

        // For Union, append unique rhs items that are not already in lhs,
        // preserving the rhs first-occurrence order. A null on rhs is included
        // only when lhs had no null (otherwise it was already emitted above).
        if matches!(op, SetOp::Union) {
            let mut rhs_unique_items: Vec<(usize, bool)> =
                rhs_probe.keys().map(|k| (k.idx as usize, false)).collect();
            if lhs_first_null_local.is_none()
                && let Some(null_local) = rhs_first_null_local
            {
                let insert_pos = rhs_unique_items
                    .iter()
                    .position(|&(idx, _)| idx > null_local)
                    .unwrap_or(rhs_unique_items.len());
                rhs_unique_items.insert(insert_pos, (null_local, true));
            }

            for &(local_idx, is_null) in &rhs_unique_items {
                let absolute_idx_in_combined = lhs_child_len + r_start + local_idx;
                if is_null {
                    // Already filtered above: only reach here if lhs has no null.
                    take_indices.push(absolute_idx_in_combined as u64);
                    kept_count += 1;
                } else {
                    let in_lhs = probe_contains(&lhs_sub, &rhs_sub, local_idx, &lhs_probe)?;
                    if !in_lhs {
                        take_indices.push(absolute_idx_in_combined as u64);
                        kept_count += 1;
                    }
                }
            }
        }

        current_offset += kept_count;
        out_offsets.push(current_offset);
    }

    let take_indices_array = UInt64Array::from_vec("indices", take_indices);
    let result_child = combined.take(&take_indices_array)?;

    let outer_nulls = if out_validity.iter().all(|v| *v) {
        None
    } else {
        Some(arrow::buffer::NullBuffer::from_iter(
            out_validity.into_iter(),
        ))
    };

    let inner_dtype = result_child.data_type().clone();
    let list_field = Arc::new(Field::new(
        lhs.name(),
        DataType::List(Box::new(inner_dtype)),
    ));
    let list_array = ListArray::new(
        list_field,
        result_child,
        OffsetBuffer::new(out_offsets.into()),
        outer_nulls,
    );
    Ok(list_array.into_series())
}

/// Returns true if the element at `query_local_idx` of `query` exists in `target`.
/// Both `query` and `target` are sub-series (already sliced for this row).
/// `target_probe` is the probe table built from `target` (nulls excluded).
fn probe_contains(
    target: &Series,
    query: &Series,
    query_local_idx: usize,
    target_probe: &indexmap::IndexMap<
        daft_core::utils::identity_hash_set::IndexHash,
        (),
        daft_core::utils::identity_hash_set::IdentityBuildHasher,
    >,
) -> DaftResult<bool> {
    use indexmap::map::RawEntryApiV1;

    // Null query elements are never "contained" (Spark drops nulls in set ops).
    if !query.is_valid(query_local_idx) {
        return Ok(false);
    }
    if target.is_empty() {
        return Ok(false);
    }
    let query_hash = query.hash_with_nulls(None)?;
    let h = match query_hash.get(query_local_idx) {
        Some(h) => h,
        None => return Ok(false),
    };
    let entry = target_probe.raw_entry_v1().from_hash(h, |other| {
        h == other.hash && target.get_lit(other.idx as usize) == query.get_lit(query_local_idx)
    });
    Ok(entry.is_some())
}
