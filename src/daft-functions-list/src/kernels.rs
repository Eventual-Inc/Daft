use std::{iter::repeat_n, sync::Arc};

use arrow::{
    array::{ArrayRef, AsArray, BooleanBufferBuilder, BooleanBuilder, make_comparator},
    buffer::OffsetBuffer,
};
use common_error::DaftResult;
use daft_core::{
    array::{
        FixedSizeListArray, ListArray, StructArray,
        growable::{Growable, make_growable},
    },
    datatypes::{try_mean_aggregation_supertype, try_sum_supertype},
    prelude::{
        BooleanArray, CountMode, DataType, Field, Int64Array, MapArray, UInt64Array, Utf8Array,
    },
    series::{IntoSeries, Series},
    utils::identity_hash_set::IdentityBuildHasher,
};
use indexmap::{
    IndexMap,
    map::{RawEntryApiV1, raw_entry_v1::RawEntryMut},
};
pub trait ListArrayExtension: Sized {
    fn value_counts(&self) -> DaftResult<MapArray>;
    fn count(&self, mode: CountMode) -> DaftResult<UInt64Array>;
    fn explode(&self, ignore_empty_and_null: bool) -> DaftResult<Series>;
    fn join(&self, delimiter: &Utf8Array) -> DaftResult<Utf8Array>;
    fn get_children(&self, idx: &Int64Array, default: &Series) -> DaftResult<Series>;
    fn get_chunks(&self, size: usize) -> DaftResult<Series>;
    fn list_sort(&self, desc: &BooleanArray, nulls_first: &BooleanArray) -> DaftResult<Self>;
    fn list_bool_and(&self) -> DaftResult<BooleanArray>;
    fn list_bool_or(&self) -> DaftResult<BooleanArray>;
    fn list_contains(&self, item: &Series) -> DaftResult<BooleanArray>;
}

pub trait ListArrayAggExtension: Sized {
    fn sum(&self) -> DaftResult<Series>;
    fn mean(&self) -> DaftResult<Series>;
    fn min(&self) -> DaftResult<Series>;
    fn max(&self) -> DaftResult<Series>;
}

pub fn list_fill(elem: &Series, num_array: &Int64Array) -> DaftResult<ListArray> {
    let generated = general_list_fill_helper(elem, num_array)?;
    let generated_refs: Vec<&Series> = generated.iter().collect();
    let lengths = generated.iter().map(|arr| arr.len());
    let offsets = OffsetBuffer::from_lengths(lengths);
    let flat_child = if generated_refs.is_empty() {
        // when there's no output, we should create an empty series
        Series::empty(elem.name(), elem.data_type())
    } else {
        Series::concat(&generated_refs)?
    };
    Ok(ListArray::new(
        elem.field().to_list_field(),
        flat_child,
        offsets,
        None,
    ))
}
impl ListArrayExtension for ListArray {
    fn value_counts(&self) -> DaftResult<MapArray> {
        struct IndexRef {
            index: usize,
            hash: u64,
        }

        impl std::hash::Hash for IndexRef {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                self.hash.hash(state);
            }
        }

        let original_name = self.name();

        let hashes = self.flat_child.hash(None)?;

        let flat_child = self.flat_child.to_arrow()?;
        let flat_child = flat_child.as_ref();

        let comparator = make_comparator(flat_child, flat_child, Default::default()).unwrap();
        let is_eq = |i, j| comparator(i, j).is_eq();

        let key_type = self.flat_child.data_type().clone();
        let count_type = DataType::UInt64;

        let mut include_mask = Vec::with_capacity(self.flat_child.len());
        let mut count_array = Vec::new();

        let mut offsets = Vec::with_capacity(self.len());

        offsets.push(0_i64);

        let mut map: IndexMap<IndexRef, u64, IdentityBuildHasher> = IndexMap::default();

        for range in self.offsets().windows(2).map(|w| {
            let from = w[0];
            let to = w[1];
            debug_assert!(from <= to, "offsets must be monotonically increasing");
            from..to
        }) {
            map.clear();

            for index in range {
                let index = index as usize;
                if !flat_child.is_valid(index) {
                    include_mask.push(false);
                    // skip nulls
                    continue;
                }

                let hash = hashes.get(index).unwrap();

                let entry = map
                    .raw_entry_mut_v1()
                    .from_hash(hash, |other| is_eq(other.index, index));

                match entry {
                    RawEntryMut::Occupied(mut entry) => {
                        include_mask.push(false);
                        *entry.get_mut() += 1;
                    }
                    RawEntryMut::Vacant(vacant) => {
                        include_mask.push(true);
                        vacant.insert(IndexRef { index, hash }, 1);
                    }
                }
            }

            // IndexMap maintains insertion order, so we iterate over its values
            // in the same order that elements were added. This ensures that
            // the count_array values correspond to the same order in which
            // the include_mask was set earlier in the loop. Each 'true' in
            // include_mask represents a unique key, and its corresponding
            // count is now added to count_array in the same sequence.
            for v in map.values() {
                count_array.push(*v);
            }

            offsets.push(count_array.len() as i64);
        }

        let values = UInt64Array::from_vec("count", count_array).into_series();
        let include_mask = BooleanArray::from_values("boolean", include_mask.into_iter());

        let keys = self.flat_child.filter(&include_mask)?;

        let keys = Series::from_arrow(Field::new("key", key_type.clone()), keys.to_arrow()?)?;

        let values =
            Series::from_arrow(Field::new("value", count_type.clone()), values.to_arrow()?)?;

        let struct_type = DataType::Struct(vec![
            Field::new("key", key_type.clone()),
            Field::new("value", count_type.clone()),
        ]);

        let struct_array = StructArray::new(
            Arc::new(Field::new("entries", struct_type.clone())),
            vec![keys, values],
            None,
        );

        let list_type = DataType::List(Box::new(struct_type));

        let offsets = OffsetBuffer::new(offsets.into());

        let list_array = Self::new(
            Arc::new(Field::new("entries", list_type)),
            struct_array.into_series(),
            offsets,
            None,
        );

        let map_type = DataType::Map {
            key: Box::new(key_type),
            value: Box::new(count_type),
        };

        Ok(MapArray::new(
            Field::new(original_name, map_type),
            list_array,
        ))
    }

    fn count(&self, mode: CountMode) -> DaftResult<UInt64Array> {
        let counts: Vec<_> = match (mode, self.flat_child.nulls()) {
            (CountMode::All, _) | (CountMode::Valid, None) => {
                self.offsets().lengths().map(|l| l as u64).collect()
            }
            (CountMode::Valid, Some(nulls)) => self
                .offsets()
                .windows(2)
                .map(|w| {
                    (w[0]..w[1])
                        .map(|i| nulls.is_valid(i as usize) as u64)
                        .sum()
                })
                .collect(),
            (CountMode::Null, None) => repeat_n(0, self.offsets().len() - 1).collect(),
            (CountMode::Null, Some(nulls)) => self
                .offsets()
                .windows(2)
                .map(|w| {
                    (w[0]..w[1])
                        .map(|i| !nulls.is_valid(i as usize) as u64)
                        .sum()
                })
                .collect(),
        };
        UInt64Array::from_values(self.name(), counts).with_nulls(self.nulls().cloned())
    }

    fn explode(&self, ignore_empty_and_null: bool) -> DaftResult<Series> {
        let offsets = self.offsets();

        let total_capacity: usize = (0..self.len())
            .map(|i| {
                let is_valid = self.is_valid(i);
                let len: usize = (offsets.get(i + 1).unwrap() - offsets.get(i).unwrap()) as usize;
                match (is_valid, len) {
                    (false, _) => usize::from(!ignore_empty_and_null),
                    (true, 0) => usize::from(!ignore_empty_and_null),
                    (true, l) => l,
                }
            })
            .sum();
        let mut growable: Box<dyn Growable> = make_growable(
            self.name(),
            self.child_data_type(),
            vec![&self.flat_child],
            true,
            total_capacity,
        );

        for i in 0..self.len() {
            let is_valid = self.is_valid(i);
            let start = offsets.get(i).unwrap();
            let len = offsets.get(i + 1).unwrap() - start;
            match (is_valid, len) {
                (false, _) => {
                    if !ignore_empty_and_null {
                        growable.add_nulls(1);
                    }
                }
                (true, 0) => {
                    if !ignore_empty_and_null {
                        growable.add_nulls(1);
                    }
                }
                (true, l) => growable.extend(0, *start as usize, l as usize),
            }
        }

        growable.build()
    }

    fn join(&self, delimiter: &Utf8Array) -> DaftResult<Utf8Array> {
        assert_eq!(self.child_data_type(), &DataType::Utf8,);

        let delimiter_iter: Box<dyn Iterator<Item = Option<&str>>> = if delimiter.len() == 1 {
            Box::new(repeat_n(delimiter.get(0), self.len()))
        } else {
            assert_eq!(delimiter.len(), self.len());

            Box::new(delimiter.into_iter())
        };
        let self_iter = (0..self.len()).map(|i| self.get(i));

        let result = self_iter
            .zip(delimiter_iter)
            .map(|(list_element, delimiter)| {
                join_arrow_list_of_utf8s(
                    list_element.as_ref().map(|l| l.utf8().unwrap().to_arrow()),
                    delimiter.unwrap_or(""),
                )
            })
            .collect::<Utf8Array>();

        Ok(result.rename(self.name()))
    }

    fn get_children(&self, idx: &Int64Array, default: &Series) -> DaftResult<Series> {
        let idx_iter = create_iter(idx, self.len());
        get_children_helper(self, idx_iter, default)
    }

    fn get_chunks(&self, size: usize) -> DaftResult<Series> {
        let mut to_skip = Vec::with_capacity(self.flat_child.len());
        let mut new_offsets = Vec::with_capacity(self.flat_child.len() + 1);
        let mut total_elements_to_skip = 0;
        new_offsets.push(0);
        for i in 0..self.offsets().len() - 1 {
            let slice_len = self.offsets().get(i + 1).unwrap() - self.offsets().get(i).unwrap();
            let modulo = slice_len as usize % size;
            to_skip.push(modulo);
            total_elements_to_skip += modulo;
            new_offsets.push(new_offsets.last().unwrap() + (slice_len / size as i64));
        }
        let to_skip = if total_elements_to_skip == 0 {
            None
        } else {
            Some(to_skip.iter().copied())
        };
        get_chunks_helper(
            &self.flat_child,
            self.field.clone(),
            self.nulls(),
            size,
            total_elements_to_skip,
            to_skip,
            new_offsets,
        )
    }

    // Sorts the lists within a list column
    fn list_sort(&self, desc: &BooleanArray, nulls_first: &BooleanArray) -> DaftResult<Self> {
        let offsets = self.offsets();
        let child_series = if desc.len() == 1 {
            let desc_iter = repeat_n(desc.get(0).unwrap(), self.len());
            let nulls_first_iter = repeat_n(nulls_first.get(0).unwrap(), self.len());
            if let Some(nulls) = self.nulls() {
                list_sort_helper(
                    &self.flat_child,
                    offsets,
                    desc_iter,
                    nulls_first_iter,
                    nulls.iter(),
                )?
            } else {
                list_sort_helper(
                    &self.flat_child,
                    offsets,
                    desc_iter,
                    nulls_first_iter,
                    repeat_n(true, self.len()),
                )?
            }
        } else {
            let desc_iter = desc.values()?;

            let nulls_first_iter = nulls_first.values()?;
            if let Some(nulls) = self.nulls() {
                list_sort_helper(
                    &self.flat_child,
                    offsets,
                    desc_iter,
                    nulls_first_iter,
                    nulls.iter(),
                )?
            } else {
                list_sort_helper(
                    &self.flat_child,
                    offsets,
                    desc_iter,
                    nulls_first_iter,
                    repeat_n(true, self.len()),
                )?
            }
        };

        let child_refs: Vec<&Series> = child_series.iter().collect();
        let child = if child_refs.is_empty() {
            Series::empty(self.name(), self.child_data_type())
        } else {
            Series::concat(&child_refs)?
        };

        // Calculate new offsets based on the lengths of the sorted series.
        let lengths = child_series.iter().map(|s| s.len());
        let new_offsets = OffsetBuffer::from_lengths(lengths);

        Ok(Self::new(
            self.field.clone(),
            child,
            new_offsets,
            self.nulls().cloned(),
        ))
    }

    fn list_bool_and(&self) -> DaftResult<BooleanArray> {
        let child = &self.flat_child;
        let offsets = self.offsets();
        let nulls = self.nulls();

        let mut result = BooleanBufferBuilder::new(self.len());
        let mut result_nulls = Vec::with_capacity(self.len());

        for i in 0..self.len() {
            let is_valid = nulls.is_none_or(|v| v.is_valid(i));
            if !is_valid {
                result.append(false);
                result_nulls.push(false);
                continue;
            }

            let start = *offsets.get(i).unwrap() as usize;
            let end = *offsets.get(i + 1).unwrap() as usize;
            let slice = child.slice(start, end)?;

            // If slice is empty or all null, return null
            if slice.is_empty() || slice.nulls().is_some_and(|v| v.null_count() == slice.len()) {
                result.append(false);
                result_nulls.push(false);
                continue;
            }

            // Look for first non-null false value
            let mut all_true = true;
            let bool_slice = slice.bool()?;
            let bool_nulls = bool_slice.nulls();
            for j in 0..bool_slice.len() {
                if bool_nulls.is_none_or(|v| v.is_valid(j)) && !bool_slice.get(j).unwrap() {
                    all_true = false;
                    break;
                }
            }
            result.append(all_true);
            result_nulls.push(true);
        }

        let null_buffer = daft_arrow::buffer::NullBuffer::from_iter(result_nulls.iter().copied());

        let arrow_array = Arc::new(arrow::array::BooleanArray::new(
            result.finish(),
            Some(null_buffer),
        ));
        BooleanArray::from_arrow(Field::new(self.name(), DataType::Boolean), arrow_array)
    }

    fn list_bool_or(&self) -> DaftResult<BooleanArray> {
        let child = &self.flat_child;
        let offsets = self.offsets();
        let nulls = self.nulls();

        let mut result = Vec::with_capacity(self.len());
        let mut result_nulls = Vec::with_capacity(self.len());

        for i in 0..self.len() {
            let is_valid = nulls.is_none_or(|v| v.is_valid(i));
            if !is_valid {
                result.push(false);
                result_nulls.push(false);
                continue;
            }

            let start = *offsets.get(i).unwrap() as usize;
            let end = *offsets.get(i + 1).unwrap() as usize;
            let slice = child.slice(start, end)?;

            // If slice is empty or all null, return null
            if slice.is_empty() || slice.nulls().is_some_and(|v| v.null_count() == slice.len()) {
                result.push(false);
                result_nulls.push(false);
                continue;
            }

            // Look for first non-null true value
            let mut any_true = false;
            let bool_slice = slice.bool()?;
            let bool_nulls = bool_slice.nulls();

            for j in 0..bool_slice.len() {
                if bool_nulls.is_none_or(|v| v.is_valid(j)) && bool_slice.get(j).unwrap() {
                    any_true = true;
                    break;
                }
            }
            result.push(any_true);
            result_nulls.push(true);
        }

        let null_buffer = daft_arrow::buffer::NullBuffer::from_iter(result_nulls.iter().copied());
        BooleanArray::from_values(self.name(), result).with_nulls(Some(null_buffer))
    }

    fn list_contains(&self, item: &Series) -> DaftResult<BooleanArray> {
        let list_nulls = self.nulls();
        let mut builder = BooleanBuilder::new();
        let field = Field::new(self.name(), DataType::Boolean);

        if self.flat_child.data_type() == &DataType::Null {
            for list_idx in 0..self.len() {
                let valid = list_nulls.is_none_or(|nulls| nulls.is_valid(list_idx))
                    && item.is_valid(list_idx);
                if valid {
                    builder.append_value(false);
                } else {
                    builder.append_null();
                }
            }

            let arrow_array = Arc::new(builder.finish());
            return BooleanArray::from_arrow(field, arrow_array);
        }

        let item = item.cast(self.flat_child.data_type())?;
        let item_hashes = item.hash(None)?;
        let child_hashes = self.flat_child.hash(None)?;

        let child_arrow = self.flat_child.to_arrow()?;
        let item_arrow = item.to_arrow()?;
        let comparator = make_comparator(
            child_arrow.as_ref(),
            item_arrow.as_ref(),
            Default::default(),
        )
        .unwrap();

        for (list_idx, range) in self
            .offsets()
            .windows(2)
            .map(|w| {
                let from = w[0];
                let to = w[1];
                debug_assert!(from <= to, "offsets must be monotonically increasing");
                from..to
            })
            .enumerate()
        {
            if list_nulls.is_some_and(|nulls| nulls.is_null(list_idx)) {
                builder.append_null();
                continue;
            }

            if !item.is_valid(list_idx) {
                builder.append_null();
                continue;
            }

            let item_hash = item_hashes.get(list_idx).unwrap();
            let mut found = false;
            for elem_idx in range {
                let elem_idx = elem_idx as usize;
                if child_arrow.is_null(elem_idx) {
                    continue;
                }
                let elem_hash = child_hashes.get(elem_idx).unwrap();
                if elem_hash == item_hash && comparator(elem_idx, list_idx).is_eq() {
                    found = true;
                    break;
                }
            }

            builder.append_value(found);
        }

        Ok(BooleanArray::from_builder(self.name(), builder))
    }
}

impl ListArrayExtension for FixedSizeListArray {
    fn list_bool_and(&self) -> DaftResult<BooleanArray> {
        self.to_list().list_bool_and()
    }

    fn list_bool_or(&self) -> DaftResult<BooleanArray> {
        self.to_list().list_bool_or()
    }

    fn list_contains(&self, item: &Series) -> DaftResult<BooleanArray> {
        self.to_list().list_contains(item)
    }

    fn value_counts(&self) -> DaftResult<MapArray> {
        self.to_list().value_counts()
    }

    fn count(&self, mode: CountMode) -> DaftResult<UInt64Array> {
        let size = self.fixed_element_len();
        let counts = match (mode, self.flat_child.nulls()) {
            (CountMode::All, _) | (CountMode::Valid, None) => {
                UInt64Array::from_values(self.name(), repeat_n(size as u64, self.len()))
            }
            (CountMode::Valid, Some(nulls)) => UInt64Array::from_values(
                self.name(),
                (0..self.len())
                    .map(|i| (0..size).map(|j| nulls.is_valid(i * size + j) as u64).sum()),
            ),
            (CountMode::Null, None) => {
                UInt64Array::from_values(self.name(), repeat_n(0, self.len()))
            }
            (CountMode::Null, Some(nulls)) => UInt64Array::from_values(
                self.name(),
                (0..self.len()).map(|i| {
                    (0..size)
                        .map(|j| !nulls.is_valid(i * size + j) as u64)
                        .sum()
                }),
            ),
        };
        counts.with_nulls(self.nulls().cloned())
    }

    fn explode(&self, ignore_empty_and_null: bool) -> DaftResult<Series> {
        let list_size = self.fixed_element_len();
        let total_capacity = if list_size == 0 {
            if ignore_empty_and_null { 0 } else { self.len() }
        } else {
            let null_count = self.nulls().map(|v| v.null_count()).unwrap_or(0);
            if ignore_empty_and_null {
                list_size * (self.len() - null_count)
            } else {
                list_size * (self.len() - null_count) + (null_count)
            }
        };

        let mut child_growable: Box<dyn Growable> = make_growable(
            self.name(),
            self.child_data_type(),
            vec![&self.flat_child],
            true,
            total_capacity,
        );

        for i in 0..self.len() {
            let is_valid = self.is_valid(i) && (list_size > 0);
            match is_valid {
                false => {
                    if !ignore_empty_and_null {
                        child_growable.add_nulls(1);
                    }
                }
                true => child_growable.extend(0, i * list_size, list_size),
            }
        }
        child_growable.build()
    }

    fn join(&self, delimiter: &Utf8Array) -> DaftResult<Utf8Array> {
        assert_eq!(self.child_data_type(), &DataType::Utf8,);

        let delimiter_iter: Box<dyn Iterator<Item = Option<&str>>> = if delimiter.len() == 1 {
            Box::new(repeat_n(delimiter.get(0), self.len()))
        } else {
            assert_eq!(delimiter.len(), self.len());
            Box::new(delimiter.into_iter())
        };
        let self_iter = (0..self.len()).map(|i| self.get(i));

        let result = self_iter
            .zip(delimiter_iter)
            .map(|(list_element, delimiter)| {
                join_arrow_list_of_utf8s(
                    list_element.as_ref().map(|l| l.utf8().unwrap().to_arrow()),
                    delimiter.unwrap_or(""),
                )
            })
            .collect::<Utf8Array>();

        Ok(result.rename(self.name()))
    }

    fn get_children(&self, idx: &Int64Array, default: &Series) -> DaftResult<Series> {
        let idx_iter = create_iter(idx, self.len());

        assert!(
            default.len() == 1,
            "Only a single default value is supported"
        );
        let default = default.cast(self.child_data_type())?;

        let mut growable = make_growable(
            self.name(),
            self.child_data_type(),
            vec![&self.flat_child, &default],
            true,
            self.len(),
        );

        let list_size = self.fixed_element_len();

        for (i, child_idx) in idx_iter.enumerate() {
            let is_valid = self.is_valid(i);

            if is_valid && child_idx.abs() < list_size as i64 {
                let idx_offset = if child_idx >= 0 {
                    (i * list_size) as i64 + child_idx
                } else {
                    ((i + 1) * list_size) as i64 + child_idx
                };

                growable.extend(0, idx_offset as usize, 1);
            } else {
                // uses the default value in the case where the row is invalid or the index is out of bounds
                growable.extend(1, 0, 1);
            }
        }

        growable.build()
    }

    fn get_chunks(&self, size: usize) -> DaftResult<Series> {
        let list_size = self.fixed_element_len();
        let num_chunks = list_size / size;
        let modulo = list_size % size;
        let total_elements_to_skip = modulo * self.len();
        let new_offsets: Vec<i64> = if !self.is_empty() && num_chunks > 0 {
            (0..=((self.len() * num_chunks) as i64))
                .step_by(num_chunks)
                .collect()
        } else {
            vec![0; self.len() + 1]
        };
        let to_skip = if total_elements_to_skip == 0 {
            None
        } else {
            Some(std::iter::repeat_n(modulo, self.len()))
        };
        get_chunks_helper(
            &self.flat_child,
            self.field.clone(),
            self.nulls(),
            size,
            total_elements_to_skip,
            to_skip,
            new_offsets,
        )
    }

    // Sorts the lists within a list column
    fn list_sort(&self, desc: &BooleanArray, nulls_first: &BooleanArray) -> DaftResult<Self> {
        let fixed_size = self.fixed_element_len();

        let child_series = if desc.len() == 1 {
            let desc_iter = repeat_n(desc.get(0).unwrap(), self.len());
            let nulls_first_iter = repeat_n(nulls_first.get(0).unwrap(), self.len());
            if let Some(nulls) = self.nulls() {
                list_sort_helper_fixed_size(
                    &self.flat_child,
                    fixed_size,
                    desc_iter,
                    nulls_first_iter,
                    nulls.iter(),
                )?
            } else {
                list_sort_helper_fixed_size(
                    &self.flat_child,
                    fixed_size,
                    desc_iter,
                    nulls_first_iter,
                    repeat_n(true, self.len()),
                )?
            }
        } else {
            let desc_iter = desc.values()?;
            let nulls_first_iter = nulls_first.values()?;
            if let Some(nulls) = self.nulls() {
                list_sort_helper_fixed_size(
                    &self.flat_child,
                    fixed_size,
                    desc_iter,
                    nulls_first_iter,
                    nulls.iter(),
                )?
            } else {
                list_sort_helper_fixed_size(
                    &self.flat_child,
                    fixed_size,
                    desc_iter,
                    nulls_first_iter,
                    repeat_n(true, self.len()),
                )?
            }
        };

        let child_refs: Vec<&Series> = child_series.iter().collect();
        let child = if child_refs.is_empty() {
            Series::empty(self.name(), self.child_data_type())
        } else {
            Series::concat(&child_refs)?
        };
        Ok(Self::new(self.field.clone(), child, self.nulls().cloned()))
    }
}

fn join_arrow_list_of_utf8s(list_element: Option<ArrayRef>, delimiter_str: &str) -> Option<String> {
    list_element
        .map(|list_element| {
            list_element
                .as_string::<i64>()
                .iter()
                .fold(String::new(), |acc, str_item| {
                    acc + str_item.unwrap_or("") + delimiter_str
                })
            // Remove trailing `delimiter_str`
        })
        .map(|result| {
            let result_len = result.len();
            if result_len > 0 {
                result[..result_len - delimiter_str.len()].to_string()
            } else {
                result
            }
        })
}

/// Given an i64 array that may have either 1 or `self.len()` elements, create an iterator with
/// `self.len()` elements. If there was originally 1 element, we repeat this element `self.len()`
/// times, otherwise we simply take the original array.
fn create_iter<'a>(arr: &'a Int64Array, len: usize) -> Box<dyn Iterator<Item = i64> + 'a> {
    match arr.len() {
        1 => Box::new(repeat_n(arr.get(0).unwrap(), len)),
        arr_len => {
            assert_eq!(arr_len, len);
            Box::new(arr.into_iter().map(|x| x.unwrap()))
        }
    }
}

/// Helper function that gets chunks of a given `size` from each list in the Series. Discards excess
/// elements that do not fit into the chunks.
///
/// This function has two paths. The first is a fast path that is taken when all lists in the
/// Series have a length that is a multiple of `size`, which means they can be chunked cleanly
/// without leftover elements. In the fast path, we simply pass the underlying array of elements to
/// the result, but reinterpret it as a list of fixed sized lists.
///
/// If there is at least one list that cannot be chunked cleanly, the underlying array of elements
/// has to be compacted to remove the excess elements. In this case we take the slower path that
/// does this compaction.
///
///
/// # Arguments
///
/// * `flat_child`  - The Series that we're extracting chunks from.
/// * `field`       - The field of the parent list.
/// * `nulls`    - The parent list's nulls.
/// * `size`        - The size for each chunk.
/// * `total_elements_to_skip` - The number of elements in the Series that do not fit cleanly into
///   chunks. We take the fast path iff this value is 0.
/// * `to_skip`     - An optional iterator of the number of elements to skip for each list. Elements
///   are skipped when they cannot fit into their parent list's chunks.
/// * `new_offsets` - The new offsets to use for the topmost list array, this is computed based on
///   the number of chunks extracted from each list.
fn get_chunks_helper(
    flat_child: &Series,
    field: Arc<Field>,
    nulls: Option<&daft_arrow::buffer::NullBuffer>,
    size: usize,
    total_elements_to_skip: usize,
    to_skip: Option<impl Iterator<Item = usize>>,
    new_offsets: Vec<i64>,
) -> DaftResult<Series> {
    if total_elements_to_skip == 0 {
        let inner_list_field = field.to_exploded_field()?.to_fixed_size_list_field(size)?;
        let inner_list = FixedSizeListArray::new(
            inner_list_field.clone(),
            flat_child.clone(),
            None, // Since we're creating an extra layer of lists, this layer doesn't have any
                  // validity information. The topmost list takes the parent's validity, and the
                  // child list is unaffected by the chunking operation and maintains its validity.
                  // This reasoning applies to the places that follow where validity is set.
        );
        Ok(ListArray::new(
            inner_list_field.to_list_field(),
            inner_list.into_series(),
            OffsetBuffer::new(new_offsets.into()),
            nulls.cloned(), // Copy the parent's nulls.
        )
        .into_series())
    } else {
        let mut growable: Box<dyn Growable> = make_growable(
            &field.name,
            &field.to_exploded_field()?.dtype,
            vec![flat_child],
            false, // There's no validity to set, see the comment above.
            flat_child.len() - total_elements_to_skip,
        );
        let mut starting_idx = 0;
        for (i, to_skip) in to_skip.unwrap().enumerate() {
            let num_chunks = new_offsets.get(i + 1).unwrap() - new_offsets.get(i).unwrap();
            let slice_len = num_chunks as usize * size;
            growable.extend(0, starting_idx, slice_len);
            starting_idx += slice_len + to_skip;
        }
        let inner_list_field = field.to_exploded_field()?.to_fixed_size_list_field(size)?;
        let inner_list = FixedSizeListArray::new(inner_list_field.clone(), growable.build()?, None);
        Ok(ListArray::new(
            inner_list_field.to_list_field(),
            inner_list.into_series(),
            OffsetBuffer::new(new_offsets.into()),
            nulls.cloned(), // Copy the parent's nulls.
        )
        .into_series())
    }
}

fn list_sort_helper(
    flat_child: &Series,
    offsets: &OffsetBuffer<i64>,
    desc_iter: impl Iterator<Item = bool>,
    nulls_first_iter: impl Iterator<Item = bool>,
    nulls: impl Iterator<Item = bool>,
) -> DaftResult<Vec<Series>> {
    desc_iter
        .zip(nulls_first_iter)
        .zip(nulls)
        .enumerate()
        .map(|(i, ((desc, nulls_first), valid))| {
            let start = *offsets.get(i).unwrap() as usize;
            let end = *offsets.get(i + 1).unwrap() as usize;
            if valid {
                flat_child.slice(start, end)?.sort(desc, nulls_first)
            } else {
                Ok(Series::full_null(
                    flat_child.name(),
                    flat_child.data_type(),
                    end - start,
                ))
            }
        })
        .collect()
}

fn list_sort_helper_fixed_size(
    flat_child: &Series,
    fixed_size: usize,
    desc_iter: impl Iterator<Item = bool>,
    nulls_first_iter: impl Iterator<Item = bool>,
    nulls: impl Iterator<Item = bool>,
) -> DaftResult<Vec<Series>> {
    desc_iter
        .zip(nulls_first_iter)
        .zip(nulls)
        .enumerate()
        .map(|(i, ((desc, nulls_first), valid))| {
            let start = i * fixed_size;
            let end = (i + 1) * fixed_size;
            if valid {
                flat_child.slice(start, end)?.sort(desc, nulls_first)
            } else {
                Ok(Series::full_null(
                    flat_child.name(),
                    flat_child.data_type(),
                    end - start,
                ))
            }
        })
        .collect()
}

fn get_children_helper(
    arr: &ListArray,
    idx_iter: impl Iterator<Item = i64>,
    default: &Series,
) -> DaftResult<Series> {
    assert!(
        default.len() == 1,
        "Only a single default value is supported"
    );
    let default = default.cast(arr.child_data_type())?;

    let mut growable = make_growable(
        arr.name(),
        arr.child_data_type(),
        vec![&arr.flat_child, &default],
        true,
        arr.len(),
    );

    let offsets = arr.offsets();

    for (i, child_idx) in idx_iter.enumerate() {
        let is_valid = arr.is_valid(i);
        let start = *offsets.get(i).unwrap();
        let end = *offsets.get(i + 1).unwrap();

        let idx_offset = if child_idx >= 0 {
            start + child_idx
        } else {
            end + child_idx
        };

        if is_valid && idx_offset >= start && idx_offset < end {
            growable.extend(0, idx_offset as usize, 1);
        } else {
            // uses the default value in the case where the row is invalid or the index is out of bounds
            growable.extend(1, 0, 1);
        }
    }

    growable.build()
}

fn general_list_fill_helper(element: &Series, num_array: &Int64Array) -> DaftResult<Vec<Series>> {
    let num_iter = create_iter(num_array, element.len());
    let mut result = Vec::with_capacity(element.len());
    let element_data = element.as_physical()?;
    for (row_index, num) in num_iter.enumerate() {
        let list_arr = if element.is_valid(row_index) {
            let mut list_growable = make_growable(
                element.name(),
                element.data_type(),
                vec![&element_data],
                false,
                num as usize,
            );
            for _ in 0..num {
                list_growable.extend(0, row_index, 1);
            }
            list_growable.build()?
        } else {
            Series::full_null(element.name(), element.data_type(), num as usize)
        };
        result.push(list_arr);
    }
    Ok(result)
}

macro_rules! impl_aggs_list_array {
    ($la:ident, $agg_helper:ident) => {
        impl ListArrayAggExtension for $la {
            fn sum(&self) -> DaftResult<Series> {
                $agg_helper(self, |s| s.sum(None), try_sum_supertype)
            }

            fn mean(&self) -> DaftResult<Series> {
                $agg_helper(self, |s| s.mean(None), try_mean_aggregation_supertype)
            }

            fn min(&self) -> DaftResult<Series> {
                $agg_helper(self, |s| s.min(None), |dtype| Ok(dtype.clone()))
            }

            fn max(&self) -> DaftResult<Series> {
                $agg_helper(self, |s| s.max(None), |dtype| Ok(dtype.clone()))
            }
        }

        fn $agg_helper<T, F>(arr: &$la, op: T, target_type_getter: F) -> DaftResult<Series>
        where
            T: Fn(&Series) -> DaftResult<Series>,
            F: Fn(&DataType) -> DaftResult<DataType>,
        {
            // TODO(Kevin): Currently this requires full materialization of one Series for every list. We could avoid this by implementing either sorted aggregation or an array builder

            // Assumes `op`` returns a null Series given an empty Series
            let aggs = arr
                .into_iter()
                .map(|s| s.unwrap_or(Series::empty("", arr.child_data_type())))
                .map(|s| op(&s))
                .collect::<DaftResult<Vec<_>>>()?;

            let agg_refs: Vec<_> = aggs.iter().collect();

            if agg_refs.is_empty() {
                let target_type = target_type_getter(arr.child_data_type())?;
                Ok(Series::empty(arr.name(), &target_type))
            } else {
                Series::concat(agg_refs.as_slice()).map(|s| s.rename(arr.name()))
            }
        }
    };
}
impl_aggs_list_array!(ListArray, list_agg_helper);
impl_aggs_list_array!(FixedSizeListArray, fsl_agg_helper);

#[cfg(test)]
mod tests {
    use super::*;

    // ── helpers ──────────────────────────────────────────────────────────

    fn make_list_i64(name: &str, data: Vec<Option<Vec<i64>>>) -> ListArray {
        ListArray::from_vec(name, data)
    }

    fn make_list_utf8(name: &str, data: Vec<Option<Vec<&str>>>) -> ListArray {
        let series_data: Vec<Option<Series>> = data
            .into_iter()
            .map(|opt| opt.map(|v| Utf8Array::from_values("item", v).into_series()))
            .collect();
        ListArray::from_series(name, series_data).unwrap()
    }

    fn make_list_bool(name: &str, data: Vec<Option<Vec<Option<bool>>>>) -> ListArray {
        let series_data: Vec<Option<Series>> = data
            .into_iter()
            .map(|opt| opt.map(|v| BooleanArray::from_iter("item", v.into_iter()).into_series()))
            .collect();
        ListArray::from_series(name, series_data).unwrap()
    }

    fn make_fsl_i64(name: &str, data: &[i64], size: usize) -> FixedSizeListArray {
        let child = Int64Array::from_values("item", data.iter().copied()).into_series();
        let field = Field::new(
            name,
            DataType::FixedSizeList(Box::new(DataType::Int64), size),
        );
        FixedSizeListArray::new(field, child, None)
    }

    // ── count ───────────────────────────────────────────────────────────

    #[test]
    fn test_list_count_all() {
        let arr = make_list_i64(
            "a",
            vec![Some(vec![1, 2, 3]), Some(vec![4, 5]), Some(vec![]), None],
        );
        let result = arr.count(CountMode::All).unwrap();
        assert_eq!(result.get(0), Some(3));
        assert_eq!(result.get(1), Some(2));
        assert_eq!(result.get(2), Some(0));
        assert!(!result.is_valid(3));
    }

    #[test]
    fn test_list_count_valid() {
        let child = Int64Array::from_iter(
            Field::new("item", DataType::Int64),
            vec![Some(1), None, Some(3), Some(4)],
        )
        .into_series();
        let offsets = OffsetBuffer::from_lengths([3, 1]);
        let field = child.field().to_list_field();
        let arr = ListArray::new(field, child, offsets, None);

        let result = arr.count(CountMode::Valid).unwrap();
        assert_eq!(result.get(0), Some(2));
        assert_eq!(result.get(1), Some(1));
    }

    #[test]
    fn test_list_count_null() {
        let child = Int64Array::from_iter(
            Field::new("item", DataType::Int64),
            vec![Some(1), None, None, Some(4)],
        )
        .into_series();
        let offsets = OffsetBuffer::from_lengths([3, 1]);
        let field = child.field().to_list_field();
        let arr = ListArray::new(field, child, offsets, None);

        let result = arr.count(CountMode::Null).unwrap();
        assert_eq!(result.get(0), Some(2));
        assert_eq!(result.get(1), Some(0));
    }

    #[test]
    fn test_fsl_count_all() {
        let arr = make_fsl_i64("a", &[1, 2, 3, 4, 5, 6], 3);
        let result = arr.count(CountMode::All).unwrap();
        assert_eq!(result.get(0), Some(3));
        assert_eq!(result.get(1), Some(3));
    }

    // ── explode ─────────────────────────────────────────────────────────

    #[test]
    fn test_list_explode() {
        let arr = make_list_i64(
            "a",
            vec![Some(vec![1, 2]), Some(vec![3]), Some(vec![]), None],
        );
        let result = arr.explode().unwrap();
        // [1,2] + [3] + null(empty list) + null(null row) = 5
        assert_eq!(result.len(), 5);
        assert_eq!(result.i64().unwrap().get(0), Some(1));
        assert_eq!(result.i64().unwrap().get(1), Some(2));
        assert_eq!(result.i64().unwrap().get(2), Some(3));
        assert!(!result.is_valid(3)); // empty list -> null
        assert!(!result.is_valid(4)); // null row -> null
    }

    #[test]
    fn test_fsl_explode() {
        let arr = make_fsl_i64("a", &[10, 20, 30, 40], 2);
        let result = arr.explode().unwrap();
        assert_eq!(result.len(), 4);
        assert_eq!(result.i64().unwrap().get(0), Some(10));
        assert_eq!(result.i64().unwrap().get(3), Some(40));
    }

    // ── join ────────────────────────────────────────────────────────────

    #[test]
    fn test_list_join_single_delimiter() {
        let arr = make_list_utf8(
            "a",
            vec![
                Some(vec!["hello", "world"]),
                Some(vec!["foo", "bar", "baz"]),
            ],
        );
        let delim = Utf8Array::from_slice("d", &[","]);
        let result = arr.join(&delim).unwrap();
        assert_eq!(result.get(0), Some("hello,world"));
        assert_eq!(result.get(1), Some("foo,bar,baz"));
    }

    #[test]
    fn test_list_join_per_row_delimiter() {
        let arr = make_list_utf8("a", vec![Some(vec!["a", "b"]), Some(vec!["c", "d"])]);
        let delim = Utf8Array::from_slice("d", &["-", " "]);
        let result = arr.join(&delim).unwrap();
        assert_eq!(result.get(0), Some("a-b"));
        assert_eq!(result.get(1), Some("c d"));
    }

    #[test]
    fn test_list_join_empty_list() {
        let arr = make_list_utf8("a", vec![Some(vec![]), Some(vec!["x"])]);
        let delim = Utf8Array::from_slice("d", &[","]);
        let result = arr.join(&delim).unwrap();
        assert_eq!(result.get(0), Some(""));
        assert_eq!(result.get(1), Some("x"));
    }

    // ── get_children ────────────────────────────────────────────────────

    #[test]
    fn test_list_get_children_positive_idx() {
        let arr = make_list_i64("a", vec![Some(vec![10, 20, 30]), Some(vec![40, 50])]);
        let idx = Int64Array::from_values("idx", [0i64]);
        let default = Int64Array::from_values("default", [-1i64]).into_series();
        let result = arr.get_children(&idx, &default).unwrap();
        assert_eq!(result.i64().unwrap().get(0), Some(10));
        assert_eq!(result.i64().unwrap().get(1), Some(40));
    }

    #[test]
    fn test_list_get_children_negative_idx() {
        let arr = make_list_i64("a", vec![Some(vec![10, 20, 30]), Some(vec![40, 50])]);
        let idx = Int64Array::from_values("idx", [-1i64]);
        let default = Int64Array::from_values("default", [0i64]).into_series();
        let result = arr.get_children(&idx, &default).unwrap();
        assert_eq!(result.i64().unwrap().get(0), Some(30));
        assert_eq!(result.i64().unwrap().get(1), Some(50));
    }

    #[test]
    fn test_list_get_children_out_of_bounds() {
        let arr = make_list_i64("a", vec![Some(vec![1, 2])]);
        let idx = Int64Array::from_values("idx", [10i64]);
        let default = Int64Array::from_values("default", [-1i64]).into_series();
        let result = arr.get_children(&idx, &default).unwrap();
        assert_eq!(result.i64().unwrap().get(0), Some(-1));
    }

    #[test]
    fn test_fsl_get_children() {
        let arr = make_fsl_i64("a", &[10, 20, 30, 40], 2);
        let idx = Int64Array::from_values("idx", [1i64]);
        let default = Int64Array::from_values("default", [-1i64]).into_series();
        let result = arr.get_children(&idx, &default).unwrap();
        assert_eq!(result.i64().unwrap().get(0), Some(20));
        assert_eq!(result.i64().unwrap().get(1), Some(40));
    }

    // ── get_chunks ──────────────────────────────────────────────────────

    #[test]
    fn test_list_get_chunks_even() {
        let arr = make_list_i64("a", vec![Some(vec![1, 2, 3, 4])]);
        let result = arr.get_chunks(2).unwrap();
        // Result is a list of fixed-size-lists of size 2
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_list_get_chunks_with_remainder() {
        let arr = make_list_i64("a", vec![Some(vec![1, 2, 3, 4, 5])]);
        let result = arr.get_chunks(2).unwrap();
        // 5 elements chunked by 2 = 2 full chunks, 1 element discarded
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_fsl_get_chunks() {
        let arr = make_fsl_i64("a", &[1, 2, 3, 4, 5, 6], 6);
        let result = arr.get_chunks(3).unwrap();
        assert_eq!(result.len(), 1);
    }

    // ── list_sort ───────────────────────────────────────────────────────

    #[test]
    fn test_list_sort_ascending() {
        let arr = make_list_i64("a", vec![Some(vec![3, 1, 2]), Some(vec![6, 4, 5])]);
        let desc = BooleanArray::from_slice("desc", &[false]);
        let nulls_first = BooleanArray::from_slice("nf", &[false]);
        let result = arr.list_sort(&desc, &nulls_first).unwrap();

        let row0 = result.get(0).unwrap();
        assert_eq!(row0.i64().unwrap().get(0), Some(1));
        assert_eq!(row0.i64().unwrap().get(1), Some(2));
        assert_eq!(row0.i64().unwrap().get(2), Some(3));

        let row1 = result.get(1).unwrap();
        assert_eq!(row1.i64().unwrap().get(0), Some(4));
        assert_eq!(row1.i64().unwrap().get(1), Some(5));
        assert_eq!(row1.i64().unwrap().get(2), Some(6));
    }

    #[test]
    fn test_list_sort_descending() {
        let arr = make_list_i64("a", vec![Some(vec![1, 3, 2])]);
        let desc = BooleanArray::from_slice("desc", &[true]);
        let nulls_first = BooleanArray::from_slice("nf", &[false]);
        let result = arr.list_sort(&desc, &nulls_first).unwrap();

        let row = result.get(0).unwrap();
        assert_eq!(row.i64().unwrap().get(0), Some(3));
        assert_eq!(row.i64().unwrap().get(1), Some(2));
        assert_eq!(row.i64().unwrap().get(2), Some(1));
    }

    #[test]
    fn test_list_sort_with_null_row() {
        let arr = make_list_i64("a", vec![None, Some(vec![2, 1])]);
        let desc = BooleanArray::from_slice("desc", &[false]);
        let nulls_first = BooleanArray::from_slice("nf", &[false]);
        let result = arr.list_sort(&desc, &nulls_first).unwrap();

        assert!(!result.is_valid(0));
        let row1 = result.get(1).unwrap();
        assert_eq!(row1.i64().unwrap().get(0), Some(1));
        assert_eq!(row1.i64().unwrap().get(1), Some(2));
    }

    #[test]
    fn test_fsl_sort_ascending() {
        let arr = make_fsl_i64("a", &[3, 1, 2, 6, 4, 5], 3);
        let desc = BooleanArray::from_slice("desc", &[false]);
        let nulls_first = BooleanArray::from_slice("nf", &[false]);
        let result = arr.list_sort(&desc, &nulls_first).unwrap();

        let list = result.to_list();
        let row0 = list.get(0).unwrap();
        assert_eq!(row0.i64().unwrap().get(0), Some(1));
        assert_eq!(row0.i64().unwrap().get(1), Some(2));
        assert_eq!(row0.i64().unwrap().get(2), Some(3));
    }

    // ── list_bool_and ───────────────────────────────────────────────────

    #[test]
    fn test_list_bool_and_all_true() {
        let arr = make_list_bool("a", vec![Some(vec![Some(true), Some(true), Some(true)])]);
        let result = arr.list_bool_and().unwrap();
        assert_eq!(result.get(0), Some(true));
    }

    #[test]
    fn test_list_bool_and_has_false() {
        let arr = make_list_bool("a", vec![Some(vec![Some(true), Some(false), Some(true)])]);
        let result = arr.list_bool_and().unwrap();
        assert_eq!(result.get(0), Some(false));
    }

    #[test]
    fn test_list_bool_and_empty() {
        let arr = make_list_bool("a", vec![Some(vec![])]);
        let result = arr.list_bool_and().unwrap();
        assert!(!result.is_valid(0));
    }

    #[test]
    fn test_list_bool_and_null_row() {
        // Construct a single-element list array where that element is null.
        // We can't use make_list_bool with only None since concat needs >= 1 series.
        let arr = make_list_bool("a", vec![Some(vec![Some(true)]), None]);
        // We only care about the null row (index 1)
        let result = arr.list_bool_and().unwrap();
        assert_eq!(result.get(0), Some(true));
        assert!(!result.is_valid(1));
    }

    #[test]
    fn test_list_bool_and_with_null_elements() {
        // null elements should be ignored; [true, null, true] => true
        let arr = make_list_bool("a", vec![Some(vec![Some(true), None, Some(true)])]);
        let result = arr.list_bool_and().unwrap();
        assert_eq!(result.get(0), Some(true));
    }

    // ── list_bool_or ────────────────────────────────────────────────────

    #[test]
    fn test_list_bool_or_has_true() {
        let arr = make_list_bool("a", vec![Some(vec![Some(false), Some(true), Some(false)])]);
        let result = arr.list_bool_or().unwrap();
        assert_eq!(result.get(0), Some(true));
    }

    #[test]
    fn test_list_bool_or_all_false() {
        let arr = make_list_bool("a", vec![Some(vec![Some(false), Some(false)])]);
        let result = arr.list_bool_or().unwrap();
        assert_eq!(result.get(0), Some(false));
    }

    #[test]
    fn test_list_bool_or_empty() {
        let arr = make_list_bool("a", vec![Some(vec![])]);
        let result = arr.list_bool_or().unwrap();
        assert!(!result.is_valid(0));
    }

    #[test]
    fn test_list_bool_or_null_row() {
        let arr = make_list_bool("a", vec![Some(vec![Some(false)]), None]);
        let result = arr.list_bool_or().unwrap();
        assert_eq!(result.get(0), Some(false));
        assert!(!result.is_valid(1));
    }

    // ── list_contains ───────────────────────────────────────────────────

    #[test]
    fn test_list_contains_found() {
        let arr = make_list_i64("a", vec![Some(vec![1, 2, 3]), Some(vec![4, 5, 6])]);
        let item = Int64Array::from_values("item", [2i64, 6]).into_series();
        let result = arr.list_contains(&item).unwrap();
        assert_eq!(result.get(0), Some(true));
        assert_eq!(result.get(1), Some(true));
    }

    #[test]
    fn test_list_contains_not_found() {
        let arr = make_list_i64("a", vec![Some(vec![1, 2, 3]), Some(vec![4, 5, 6])]);
        let item = Int64Array::from_values("item", [99i64, 0]).into_series();
        let result = arr.list_contains(&item).unwrap();
        assert_eq!(result.get(0), Some(false));
        assert_eq!(result.get(1), Some(false));
    }

    #[test]
    fn test_list_contains_null_row() {
        let arr = make_list_i64("a", vec![None, Some(vec![1])]);
        let item = Int64Array::from_values("item", [1i64, 1]).into_series();
        let result = arr.list_contains(&item).unwrap();
        assert!(!result.is_valid(0));
        assert_eq!(result.get(1), Some(true));
    }

    #[test]
    fn test_list_contains_null_item() {
        let arr = make_list_i64("a", vec![Some(vec![1, 2])]);
        let item =
            Int64Array::from_iter(Field::new("item", DataType::Int64), vec![None]).into_series();
        let result = arr.list_contains(&item).unwrap();
        assert!(!result.is_valid(0));
    }

    // ── value_counts ────────────────────────────────────────────────────

    #[test]
    fn test_list_value_counts_basic() {
        let arr = make_list_i64("a", vec![Some(vec![1, 2, 1, 3, 2, 1])]);
        let result = arr.value_counts().unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_list_value_counts_multiple_rows() {
        let arr = make_list_i64("a", vec![Some(vec![1, 1]), Some(vec![2, 3, 2])]);
        let result = arr.value_counts().unwrap();
        assert_eq!(result.len(), 2);
    }

    // ── list_fill ───────────────────────────────────────────────────────

    #[test]
    fn test_list_fill_basic() {
        let elem = Int64Array::from_values("e", [10i64, 20, 30]).into_series();
        let num = Int64Array::from_values("n", [2i64, 3, 1]);
        let result = list_fill(&elem, &num).unwrap();
        assert_eq!(result.len(), 3);

        let row0 = result.get(0).unwrap();
        assert_eq!(row0.len(), 2);
        assert_eq!(row0.i64().unwrap().get(0), Some(10));
        assert_eq!(row0.i64().unwrap().get(1), Some(10));

        let row1 = result.get(1).unwrap();
        assert_eq!(row1.len(), 3);

        let row2 = result.get(2).unwrap();
        assert_eq!(row2.len(), 1);
        assert_eq!(row2.i64().unwrap().get(0), Some(30));
    }

    #[test]
    fn test_list_fill_broadcast_num() {
        let elem = Int64Array::from_values("e", [5i64, 6]).into_series();
        let num = Int64Array::from_values("n", [3i64]);
        let result = list_fill(&elem, &num).unwrap();
        assert_eq!(result.len(), 2);

        let row0 = result.get(0).unwrap();
        assert_eq!(row0.len(), 3);
        assert_eq!(row0.i64().unwrap().get(0), Some(5));

        let row1 = result.get(1).unwrap();
        assert_eq!(row1.len(), 3);
        assert_eq!(row1.i64().unwrap().get(0), Some(6));
    }

    // ── aggregation: sum ────────────────────────────────────────────────

    #[test]
    fn test_list_sum() {
        let arr = make_list_i64("a", vec![Some(vec![1, 2, 3]), Some(vec![10, 20])]);
        let result = arr.sum().unwrap();
        assert_eq!(result.i64().unwrap().get(0), Some(6));
        assert_eq!(result.i64().unwrap().get(1), Some(30));
    }

    #[test]
    fn test_list_sum_empty() {
        let arr = make_list_i64("a", vec![Some(vec![])]);
        let result = arr.sum().unwrap();
        assert_eq!(result.len(), 1);
    }

    // ── aggregation: mean ───────────────────────────────────────────────

    #[test]
    fn test_list_mean() {
        let arr = make_list_i64("a", vec![Some(vec![2, 4, 6])]);
        let result = arr.mean().unwrap();
        let val = result.f64().unwrap().get(0).unwrap();
        assert!((val - 4.0).abs() < f64::EPSILON);
    }

    // ── aggregation: min / max ──────────────────────────────────────────

    #[test]
    fn test_list_min() {
        let arr = make_list_i64("a", vec![Some(vec![3, 1, 2]), Some(vec![6, 4, 5])]);
        let result = arr.min().unwrap();
        assert_eq!(result.i64().unwrap().get(0), Some(1));
        assert_eq!(result.i64().unwrap().get(1), Some(4));
    }

    #[test]
    fn test_list_max() {
        let arr = make_list_i64("a", vec![Some(vec![3, 1, 2]), Some(vec![6, 4, 5])]);
        let result = arr.max().unwrap();
        assert_eq!(result.i64().unwrap().get(0), Some(3));
        assert_eq!(result.i64().unwrap().get(1), Some(6));
    }

    // ── fsl aggregation ─────────────────────────────────────────────────

    #[test]
    fn test_fsl_sum() {
        let arr = make_fsl_i64("a", &[1, 2, 3, 4, 5, 6], 3);
        let result = arr.sum().unwrap();
        assert_eq!(result.i64().unwrap().get(0), Some(6));
        assert_eq!(result.i64().unwrap().get(1), Some(15));
    }

    #[test]
    fn test_fsl_min() {
        let arr = make_fsl_i64("a", &[3, 1, 2, 6, 4, 5], 3);
        let result = arr.min().unwrap();
        assert_eq!(result.i64().unwrap().get(0), Some(1));
        assert_eq!(result.i64().unwrap().get(1), Some(4));
    }

    #[test]
    fn test_fsl_max() {
        let arr = make_fsl_i64("a", &[3, 1, 2, 6, 4, 5], 3);
        let result = arr.max().unwrap();
        assert_eq!(result.i64().unwrap().get(0), Some(3));
        assert_eq!(result.i64().unwrap().get(1), Some(6));
    }

    #[test]
    fn test_fsl_mean() {
        let arr = make_fsl_i64("a", &[2, 4, 6], 3);
        let result = arr.mean().unwrap();
        let val = result.f64().unwrap().get(0).unwrap();
        assert!((val - 4.0).abs() < f64::EPSILON);
    }

    // ── per-row desc/nulls_first for list_sort ──────────────────────────

    #[test]
    fn test_list_sort_per_row_desc() {
        let arr = make_list_i64("a", vec![Some(vec![3, 1, 2]), Some(vec![6, 4, 5])]);
        let desc = BooleanArray::from_slice("desc", &[false, true]);
        let nulls_first = BooleanArray::from_slice("nf", &[false, false]);
        let result = arr.list_sort(&desc, &nulls_first).unwrap();

        let row0 = result.get(0).unwrap();
        assert_eq!(row0.i64().unwrap().get(0), Some(1)); // asc

        let row1 = result.get(1).unwrap();
        assert_eq!(row1.i64().unwrap().get(0), Some(6)); // desc
    }

    // ── fsl list_bool_and / list_bool_or ────────────────────────────────

    #[test]
    fn test_fsl_bool_and() {
        let child = BooleanArray::from_slice("item", &[true, true, true, false]).into_series();
        let field = Field::new("a", DataType::FixedSizeList(Box::new(DataType::Boolean), 2));
        let arr = FixedSizeListArray::new(field, child, None);
        let result = arr.list_bool_and().unwrap();
        assert_eq!(result.get(0), Some(true));
        assert_eq!(result.get(1), Some(false));
    }

    #[test]
    fn test_fsl_bool_or() {
        let child = BooleanArray::from_slice("item", &[false, false, false, true]).into_series();
        let field = Field::new("a", DataType::FixedSizeList(Box::new(DataType::Boolean), 2));
        let arr = FixedSizeListArray::new(field, child, None);
        let result = arr.list_bool_or().unwrap();
        assert_eq!(result.get(0), Some(false));
        assert_eq!(result.get(1), Some(true));
    }

    // ── fsl list_contains ───────────────────────────────────────────────

    #[test]
    fn test_fsl_list_contains() {
        let arr = make_fsl_i64("a", &[1, 2, 3, 4], 2);
        let item = Int64Array::from_values("item", [2i64, 5]).into_series();
        let result = arr.list_contains(&item).unwrap();
        assert_eq!(result.get(0), Some(true));
        assert_eq!(result.get(1), Some(false));
    }

    // ── fsl value_counts ────────────────────────────────────────────────

    #[test]
    fn test_fsl_value_counts() {
        let arr = make_fsl_i64("a", &[1, 1, 2, 2, 2, 3], 3);
        let result = arr.value_counts().unwrap();
        assert_eq!(result.len(), 2);
    }

    // ── fsl join ────────────────────────────────────────────────────────

    #[test]
    fn test_fsl_join() {
        let child = Utf8Array::from_values("item", ["a", "b", "c", "d"]).into_series();
        let field = Field::new("a", DataType::FixedSizeList(Box::new(DataType::Utf8), 2));
        let arr = FixedSizeListArray::new(field, child, None);
        let delim = Utf8Array::from_slice("d", &[","]);
        let result = arr.join(&delim).unwrap();
        assert_eq!(result.get(0), Some("a,b"));
        assert_eq!(result.get(1), Some("c,d"));
    }

    // ── fsl explode ─────────────────────────────────────────────────────

    #[test]
    fn test_fsl_explode_with_nulls() {
        let child = Int64Array::from_values("item", [1i64, 2, 3, 4]).into_series();
        let field = Field::new("a", DataType::FixedSizeList(Box::new(DataType::Int64), 2));
        let nulls = daft_arrow::buffer::NullBuffer::from(&[true, false]);
        let arr = FixedSizeListArray::new(field, child, Some(nulls));
        let result = arr.explode().unwrap();
        // valid row: 2 elements, null row: 1 null sentinel
        assert_eq!(result.len(), 3);
        assert_eq!(result.i64().unwrap().get(0), Some(1));
        assert_eq!(result.i64().unwrap().get(1), Some(2));
        assert!(!result.is_valid(2));
    }
}
