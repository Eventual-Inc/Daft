use std::{iter::repeat_n, sync::Arc};

use arrow2::offset::{Offsets, OffsetsBuffer};
use common_error::DaftResult;
use daft_core::{
    array::{
        growable::{make_growable, Growable},
        ops::arrow2::comparison::build_is_equal,
        FixedSizeListArray, ListArray, StructArray,
    },
    datatypes::try_mean_aggregation_supertype,
    kernels::search_sorted::build_is_valid,
    prelude::{
        AsArrow, BooleanArray, CountMode, DataType, Field, Int64Array, MapArray, UInt64Array,
        Utf8Array,
    },
    series::{IntoSeries, Series},
    utils::identity_hash_set::IdentityBuildHasher,
};
use indexmap::{
    map::{raw_entry_v1::RawEntryMut, RawEntryApiV1},
    IndexMap,
};
pub trait ListArrayExtension: Sized {
    fn value_counts(&self) -> DaftResult<MapArray>;
    fn count(&self, mode: CountMode) -> DaftResult<UInt64Array>;
    fn explode(&self) -> DaftResult<Series>;
    fn join(&self, delimiter: &Utf8Array) -> DaftResult<Utf8Array>;
    fn get_children(&self, idx: &Int64Array, default: &Series) -> DaftResult<Series>;
    fn get_slices(&self, start: &Int64Array, end: Option<&Int64Array>) -> DaftResult<Series>;
    fn get_chunks(&self, size: usize) -> DaftResult<Series>;
    fn list_sort(&self, desc: &BooleanArray, nulls_first: &BooleanArray) -> DaftResult<Self>;
    fn list_bool_and(&self) -> DaftResult<BooleanArray>;
    fn list_bool_or(&self) -> DaftResult<BooleanArray>;
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
    let offsets = Offsets::try_from_lengths(lengths)?;
    let flat_child = if generated_refs.is_empty() {
        // when there's no output, we should create an empty series
        Series::empty(elem.name(), elem.data_type())
    } else {
        Series::concat(&generated_refs)?
    };
    Ok(ListArray::new(
        elem.field().to_list_field()?,
        flat_child,
        offsets.into(),
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

        let flat_child = self.flat_child.to_arrow();
        let flat_child = &*flat_child;

        let is_equal = build_is_equal(
            flat_child, flat_child,
            false, // this value does not matter; invalid (= nulls) are never included
            true,  // NaNs are equal so we do not get a bunch of {Nan: 1, Nan: 1, ...}
        )?;

        let is_valid = build_is_valid(flat_child);

        let key_type = self.flat_child.data_type().clone();
        let count_type = DataType::UInt64;

        let mut include_mask = Vec::with_capacity(self.flat_child.len());
        let mut count_array = Vec::new();

        let mut offsets = Vec::with_capacity(self.len());

        offsets.push(0_i64);

        let mut map: IndexMap<IndexRef, u64, IdentityBuildHasher> = IndexMap::default();
        for range in self.offsets().ranges() {
            map.clear();

            for index in range {
                let index = index as usize;
                if !is_valid(index) {
                    include_mask.push(false);
                    // skip nulls
                    continue;
                }

                let hash = hashes.get(index).unwrap();

                let entry = map
                    .raw_entry_mut_v1()
                    .from_hash(hash, |other| is_equal(other.index, index));

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

        let values = UInt64Array::from(("count", count_array)).into_series();
        let include_mask = BooleanArray::from(("boolean", include_mask.as_slice()));

        let keys = self.flat_child.filter(&include_mask)?;

        let keys = Series::try_from_field_and_arrow_array(
            Field::new("key", key_type.clone()),
            keys.to_arrow(),
        )?;

        let values = Series::try_from_field_and_arrow_array(
            Field::new("value", count_type.clone()),
            values.to_arrow(),
        )?;

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

        let offsets = OffsetsBuffer::try_from(offsets)?;

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
        let counts = match (mode, self.flat_child.validity()) {
            (CountMode::All, _) | (CountMode::Valid, None) => {
                self.offsets().lengths().map(|l| l as u64).collect()
            }
            (CountMode::Valid, Some(validity)) => self
                .offsets()
                .windows(2)
                .map(|w| {
                    (w[0]..w[1])
                        .map(|i| validity.get_bit(i as usize) as u64)
                        .sum()
                })
                .collect(),
            (CountMode::Null, None) => repeat_n(0, self.offsets().len() - 1).collect(),
            (CountMode::Null, Some(validity)) => self
                .offsets()
                .windows(2)
                .map(|w| {
                    (w[0]..w[1])
                        .map(|i| !validity.get_bit(i as usize) as u64)
                        .sum()
                })
                .collect(),
        };

        let array = Box::new(
            arrow2::array::PrimitiveArray::from_vec(counts).with_validity(self.validity().cloned()),
        );
        Ok(UInt64Array::from((self.name(), array)))
    }

    fn explode(&self) -> DaftResult<Series> {
        let offsets = self.offsets();

        let total_capacity: usize = (0..self.len())
            .map(|i| {
                let is_valid = self.is_valid(i);
                let len: usize = (offsets.get(i + 1).unwrap() - offsets.get(i).unwrap()) as usize;
                match (is_valid, len) {
                    (false, _) => 1,
                    (true, 0) => 1,
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
                (false, _) => growable.add_nulls(1),
                (true, 0) => growable.add_nulls(1),
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
            Box::new(delimiter.as_arrow().iter())
        };
        let self_iter = (0..self.len()).map(|i| self.get(i));

        let result = self_iter
            .zip(delimiter_iter)
            .map(|(list_element, delimiter)| {
                join_arrow_list_of_utf8s(
                    list_element.as_ref().map(|l| l.utf8().unwrap().data()),
                    delimiter.unwrap_or(""),
                )
            });

        Ok(Utf8Array::from((
            self.name(),
            Box::new(arrow2::array::Utf8Array::from_iter(result)),
        )))
    }

    fn get_children(&self, idx: &Int64Array, default: &Series) -> DaftResult<Series> {
        let idx_iter = create_iter(idx, self.len());
        get_children_helper(self, idx_iter, default)
    }

    fn get_slices(&self, start: &Int64Array, end: Option<&Int64Array>) -> DaftResult<Series> {
        let start_iter = create_iter(start, self.len());
        let end_iter = match end {
            Some(end) => create_iter(end, self.len()),
            None => Box::new(self.offsets().windows(2).map(|w| (w[1] - w[0]))),
        };
        get_slices_helper(
            self.offsets().iter().copied(),
            self.field.clone(),
            self.child_data_type(),
            &self.flat_child,
            self.validity(),
            start_iter,
            end_iter,
        )
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
            self.validity(),
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
            if let Some(validity) = self.validity() {
                list_sort_helper(
                    &self.flat_child,
                    offsets,
                    desc_iter,
                    nulls_first_iter,
                    validity.iter(),
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
            let desc_iter = desc.as_arrow().values_iter();
            let nulls_first_iter = nulls_first.as_arrow().values_iter();
            if let Some(validity) = self.validity() {
                list_sort_helper(
                    &self.flat_child,
                    offsets,
                    desc_iter,
                    nulls_first_iter,
                    validity.iter(),
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
        Ok(Self::new(
            self.field.clone(),
            child,
            self.offsets().clone(),
            self.validity().cloned(),
        ))
    }

    fn list_bool_and(&self) -> DaftResult<BooleanArray> {
        let child = &self.flat_child;
        let offsets = self.offsets();
        let validity = self.validity();

        let mut result = Vec::with_capacity(self.len());
        let mut result_validity = Vec::with_capacity(self.len());

        for i in 0..self.len() {
            let is_valid = validity.is_none_or(|v| v.get(i).unwrap());
            if !is_valid {
                result.push(false);
                result_validity.push(false);
                continue;
            }

            let start = *offsets.get(i).unwrap() as usize;
            let end = *offsets.get(i + 1).unwrap() as usize;
            let slice = child.slice(start, end)?;

            // If slice is empty or all null, return null
            if slice.is_empty()
                || slice
                    .validity()
                    .is_some_and(|v| v.unset_bits() == slice.len())
            {
                result.push(false);
                result_validity.push(false);
                continue;
            }

            // Look for first non-null false value
            let mut all_true = true;
            let bool_slice = slice.bool()?;
            let bool_validity = bool_slice.validity();
            let bool_data = bool_slice.as_arrow().values();
            for j in 0..bool_slice.len() {
                if bool_validity.is_none_or(|v| v.get(j).unwrap()) && !bool_data.get_bit(j) {
                    all_true = false;
                    break;
                }
            }
            result.push(all_true);
            result_validity.push(true);
        }

        let validity_bitmap = arrow2::bitmap::Bitmap::from_iter(result_validity.iter().copied());
        let values = arrow2::bitmap::Bitmap::from_iter(result.iter().copied());
        let arrow_array = arrow2::array::BooleanArray::new(
            arrow2::datatypes::DataType::Boolean,
            values,
            Some(validity_bitmap),
        );
        Ok(BooleanArray::from((self.name(), Box::new(arrow_array))))
    }

    fn list_bool_or(&self) -> DaftResult<BooleanArray> {
        let child = &self.flat_child;
        let offsets = self.offsets();
        let validity = self.validity();

        let mut result = Vec::with_capacity(self.len());
        let mut result_validity = Vec::with_capacity(self.len());

        for i in 0..self.len() {
            let is_valid = validity.is_none_or(|v| v.get(i).unwrap());
            if !is_valid {
                result.push(false);
                result_validity.push(false);
                continue;
            }

            let start = *offsets.get(i).unwrap() as usize;
            let end = *offsets.get(i + 1).unwrap() as usize;
            let slice = child.slice(start, end)?;

            // If slice is empty or all null, return null
            if slice.is_empty()
                || slice
                    .validity()
                    .is_some_and(|v| v.unset_bits() == slice.len())
            {
                result.push(false);
                result_validity.push(false);
                continue;
            }

            // Look for first non-null true value
            let mut any_true = false;
            let bool_slice = slice.bool()?;
            let bool_validity = bool_slice.validity();
            let bool_data = bool_slice.as_arrow().values();
            for j in 0..bool_slice.len() {
                if bool_validity.is_none_or(|v| v.get(j).unwrap()) && bool_data.get_bit(j) {
                    any_true = true;
                    break;
                }
            }
            result.push(any_true);
            result_validity.push(true);
        }

        let validity_bitmap = arrow2::bitmap::Bitmap::from_iter(result_validity.iter().copied());
        let values = arrow2::bitmap::Bitmap::from_iter(result.iter().copied());
        let arrow_array = arrow2::array::BooleanArray::new(
            arrow2::datatypes::DataType::Boolean,
            values,
            Some(validity_bitmap),
        );
        Ok(BooleanArray::from((self.name(), Box::new(arrow_array))))
    }
}

impl ListArrayExtension for FixedSizeListArray {
    fn list_bool_and(&self) -> DaftResult<BooleanArray> {
        self.to_list().list_bool_and()
    }

    fn list_bool_or(&self) -> DaftResult<BooleanArray> {
        self.to_list().list_bool_or()
    }

    fn value_counts(&self) -> DaftResult<MapArray> {
        self.to_list().value_counts()
    }

    fn count(&self, mode: CountMode) -> DaftResult<UInt64Array> {
        let size = self.fixed_element_len();
        let counts = match (mode, self.flat_child.validity()) {
            (CountMode::All, _) | (CountMode::Valid, None) => {
                repeat_n(size as u64, self.len()).collect()
            }
            (CountMode::Valid, Some(validity)) => (0..self.len())
                .map(|i| {
                    (0..size)
                        .map(|j| validity.get_bit(i * size + j) as u64)
                        .sum()
                })
                .collect(),
            (CountMode::Null, None) => repeat_n(0, self.len()).collect(),
            (CountMode::Null, Some(validity)) => (0..self.len())
                .map(|i| {
                    (0..size)
                        .map(|j| !validity.get_bit(i * size + j) as u64)
                        .sum()
                })
                .collect(),
        };

        let array = Box::new(
            arrow2::array::PrimitiveArray::from_vec(counts).with_validity(self.validity().cloned()),
        );
        Ok(UInt64Array::from((self.name(), array)))
    }

    fn explode(&self) -> DaftResult<Series> {
        let list_size = self.fixed_element_len();
        let total_capacity = if list_size == 0 {
            self.len()
        } else {
            let null_count = self.validity().map(|v| v.unset_bits()).unwrap_or(0);
            list_size * (self.len() - null_count)
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
                false => child_growable.add_nulls(1),
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
            Box::new(delimiter.as_arrow().iter())
        };
        let self_iter = (0..self.len()).map(|i| self.get(i));

        let result = self_iter
            .zip(delimiter_iter)
            .map(|(list_element, delimiter)| {
                join_arrow_list_of_utf8s(
                    list_element.as_ref().map(|l| l.utf8().unwrap().data()),
                    delimiter.unwrap_or(""),
                )
            });

        Ok(Utf8Array::from((
            self.name(),
            Box::new(arrow2::array::Utf8Array::from_iter(result)),
        )))
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

    fn get_slices(&self, start: &Int64Array, end: Option<&Int64Array>) -> DaftResult<Series> {
        let list_size = self.fixed_element_len();
        let start_iter = create_iter(start, self.len());
        let end_iter = match end {
            Some(end) => create_iter(end, self.len()),
            None => Box::new(repeat_n(list_size as i64, self.len())),
        };
        let new_field = Arc::new(self.field.to_exploded_field()?.to_list_field()?);
        get_slices_helper(
            (0..=((self.len() * list_size) as i64)).step_by(list_size),
            new_field,
            self.child_data_type(),
            &self.flat_child,
            self.validity(),
            start_iter,
            end_iter,
        )
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
            self.validity(),
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
            if let Some(validity) = self.validity() {
                list_sort_helper_fixed_size(
                    &self.flat_child,
                    fixed_size,
                    desc_iter,
                    nulls_first_iter,
                    validity.iter(),
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
            let desc_iter = desc.as_arrow().values_iter();
            let nulls_first_iter = nulls_first.as_arrow().values_iter();
            if let Some(validity) = self.validity() {
                list_sort_helper_fixed_size(
                    &self.flat_child,
                    fixed_size,
                    desc_iter,
                    nulls_first_iter,
                    validity.iter(),
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
        Ok(Self::new(
            self.field.clone(),
            child,
            self.validity().cloned(),
        ))
    }
}

fn join_arrow_list_of_utf8s(
    list_element: Option<&dyn arrow2::array::Array>,
    delimiter_str: &str,
) -> Option<String> {
    list_element
        .map(|list_element| {
            list_element
                .as_any()
                .downcast_ref::<arrow2::array::Utf8Array<i64>>()
                .unwrap()
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

// Given an i64 array that may have either 1 or `self.len()` elements, create an iterator with
// `self.len()` elements. If there was originally 1 element, we repeat this element `self.len()`
// times, otherwise we simply take the original array.
fn create_iter<'a>(arr: &'a Int64Array, len: usize) -> Box<dyn Iterator<Item = i64> + 'a> {
    match arr.len() {
        1 => Box::new(repeat_n(arr.get(0).unwrap(), len)),
        arr_len => {
            assert_eq!(arr_len, len);
            Box::new(arr.as_arrow().iter().map(|x| *x.unwrap()))
        }
    }
}

fn get_slices_helper(
    mut parent_offsets: impl Iterator<Item = i64>,
    field: Arc<Field>,
    child_data_type: &DataType,
    flat_child: &Series,
    validity: Option<&arrow2::bitmap::Bitmap>,
    start_iter: impl Iterator<Item = i64>,
    end_iter: impl Iterator<Item = i64>,
) -> DaftResult<Series> {
    let mut slicing_indexes = Vec::with_capacity(flat_child.len());
    let mut new_offsets = Vec::with_capacity(flat_child.len() + 1);
    new_offsets.push(0);
    let mut starting_idx = parent_offsets.next().unwrap();
    for (i, ((start, end), ending_idx)) in start_iter.zip(end_iter).zip(parent_offsets).enumerate()
    {
        let is_valid = validity.is_none_or(|v| v.get(i).unwrap());
        let slice_start = if start >= 0 {
            starting_idx + start
        } else {
            (ending_idx + start).max(starting_idx)
        };
        let slice_end = if end >= 0 {
            (starting_idx + end).min(ending_idx)
        } else {
            ending_idx + end
        };
        let slice_length = slice_end - slice_start;
        if is_valid && slice_start >= starting_idx && slice_length > 0 {
            slicing_indexes.push(slice_start);
            new_offsets.push(new_offsets.last().unwrap() + slice_length);
        } else {
            slicing_indexes.push(-1);
            new_offsets.push(*new_offsets.last().unwrap());
        }
        starting_idx = ending_idx;
    }
    let total_capacity = *new_offsets.last().unwrap();
    let mut growable: Box<dyn Growable> = make_growable(
        &field.name,
        child_data_type,
        vec![flat_child],
        false, // We don't set validity because we can simply copy the parent's validity.
        total_capacity as usize,
    );
    for (i, start) in slicing_indexes.iter().enumerate() {
        if *start >= 0 {
            let slice_len = new_offsets.get(i + 1).unwrap() - new_offsets.get(i).unwrap();
            growable.extend(0, *start as usize, slice_len as usize);
        }
    }
    Ok(ListArray::new(
        field,
        growable.build()?,
        arrow2::offset::OffsetsBuffer::try_from(new_offsets)?,
        validity.cloned(),
    )
    .into_series())
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
/// * `validity`    - The parent list's validity.
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
    validity: Option<&arrow2::bitmap::Bitmap>,
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
            inner_list_field.to_list_field()?,
            inner_list.into_series(),
            arrow2::offset::OffsetsBuffer::try_from(new_offsets)?,
            validity.cloned(), // Copy the parent's validity.
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
            inner_list_field.to_list_field()?,
            inner_list.into_series(),
            arrow2::offset::OffsetsBuffer::try_from(new_offsets)?,
            validity.cloned(), // Copy the parent's validity.
        )
        .into_series())
    }
}

fn list_sort_helper(
    flat_child: &Series,
    offsets: &OffsetsBuffer<i64>,
    desc_iter: impl Iterator<Item = bool>,
    nulls_first_iter: impl Iterator<Item = bool>,
    validity: impl Iterator<Item = bool>,
) -> DaftResult<Vec<Series>> {
    desc_iter
        .zip(nulls_first_iter)
        .zip(validity)
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
    validity: impl Iterator<Item = bool>,
) -> DaftResult<Vec<Series>> {
    desc_iter
        .zip(nulls_first_iter)
        .zip(validity)
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
                $agg_helper(self, |s| s.sum(None), |dtype| Ok(dtype.clone()))
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
