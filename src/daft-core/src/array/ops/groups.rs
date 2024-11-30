use std::{
    collections::hash_map::Entry::{Occupied, Vacant},
    hash::{BuildHasherDefault, Hash},
};

use arrow2::array::Array;
use common_error::DaftResult;
use fnv::FnvHashMap;

use super::{as_arrow::AsArrow, IntoGroups};
use crate::{
    array::{DataArray, FixedSizeListArray, ListArray, StructArray},
    datatypes::{
        BinaryArray, BooleanArray, DaftIntegerType, DaftNumericType, FixedSizeBinaryArray,
        Float32Array, Float64Array, NullArray, Utf8Array,
    },
    prelude::Decimal128Array,
};

/// Given a list of values, return a `(Vec<u64>, Vec<Vec<u64>>)`.
/// The sub-vector in the first part of the tuple contains the indices of the unique values.
/// The sub-vector in the second part of the tuple contains the index of all the places of where that value was found.
///
/// The length of the first sub-vector will always be the same as the length of the second sub-vector.
///
/// # Example:
/// ```text
/// +---+
/// | a | (index: 0)
/// | b |
/// | a |
/// | c |
/// | a | (index: 4)
/// +---+
/// ```
///
/// Calling `make_groups` on the above list would return `(vec![0, 1, 3], vec![vec![0, 2, 4], vec![1], vec![3]])` since `a` appeared 3 times at indices `0`, `2`, and `4`, etc.
fn make_groups<T>(iter: impl Iterator<Item = T>) -> DaftResult<super::GroupIndicesPair>
where
    T: Hash,
    T: Eq,
{
    const DEFAULT_SIZE: usize = 256;
    let mut tbl = FnvHashMap::<T, (u64, Vec<u64>)>::with_capacity_and_hasher(
        DEFAULT_SIZE,
        BuildHasherDefault::default(),
    );
    for (idx, val) in iter.enumerate() {
        let idx = idx as u64;
        let e = tbl.entry(val);
        match e {
            Vacant(e) => {
                e.insert((idx, vec![idx]));
            }
            Occupied(mut e) => {
                e.get_mut().1.push(idx);
            }
        }
    }
    let mut sample_indices = Vec::with_capacity(tbl.len());
    let mut group_indices = Vec::with_capacity(tbl.len());

    for (sample_index, group_index) in tbl.into_values() {
        sample_indices.push(sample_index);
        group_indices.push(group_index);
    }

    Ok((sample_indices, group_indices))
}

impl<T> IntoGroups for DataArray<T>
where
    T: DaftIntegerType,
    <T as DaftNumericType>::Native: Ord,
    <T as DaftNumericType>::Native: Hash,
    <T as DaftNumericType>::Native: std::cmp::Eq,
{
    fn make_groups(&self) -> DaftResult<super::GroupIndicesPair> {
        let array: &arrow2::array::PrimitiveArray<<T as DaftNumericType>::Native> = self.as_arrow();
        if array.null_count() > 0 {
            make_groups(array.iter())
        } else {
            make_groups(array.values_iter())
        }
    }
}

impl IntoGroups for Decimal128Array {
    fn make_groups(&self) -> DaftResult<super::GroupIndicesPair> {
        let array = self.as_arrow();
        if array.null_count() > 0 {
            make_groups(array.iter())
        } else {
            make_groups(array.values_iter())
        }
    }
}

impl IntoGroups for Float32Array {
    fn make_groups(&self) -> DaftResult<super::GroupIndicesPair> {
        let array = self.as_arrow();
        if array.null_count() > 0 {
            make_groups(array.iter().map(move |f| {
                f.map(|v| {
                    match v.is_nan() {
                        true => f32::NAN,
                        false => *v,
                    }
                    .to_bits()
                })
            }))
        } else {
            make_groups(array.values_iter().map(move |f| {
                match f.is_nan() {
                    true => f32::NAN,
                    false => *f,
                }
                .to_bits()
            }))
        }
    }
}

impl IntoGroups for Float64Array {
    fn make_groups(&self) -> DaftResult<super::GroupIndicesPair> {
        let array = self.as_arrow();
        if array.null_count() > 0 {
            make_groups(array.iter().map(move |f| {
                f.map(|v| {
                    match v.is_nan() {
                        true => f64::NAN,
                        false => *v,
                    }
                    .to_bits()
                })
            }))
        } else {
            make_groups(array.values_iter().map(move |f| {
                match f.is_nan() {
                    true => f64::NAN,
                    false => *f,
                }
                .to_bits()
            }))
        }
    }
}

impl IntoGroups for Utf8Array {
    fn make_groups(&self) -> DaftResult<super::GroupIndicesPair> {
        let array = self.as_arrow();
        if array.null_count() > 0 {
            make_groups(array.iter())
        } else {
            make_groups(array.values_iter())
        }
    }
}

impl IntoGroups for BinaryArray {
    fn make_groups(&self) -> DaftResult<super::GroupIndicesPair> {
        let array = self.as_arrow();
        if array.null_count() > 0 {
            make_groups(array.iter())
        } else {
            make_groups(array.values_iter())
        }
    }
}

impl IntoGroups for FixedSizeBinaryArray {
    fn make_groups(&self) -> DaftResult<super::GroupIndicesPair> {
        let array = self.as_arrow();
        if array.null_count() > 0 {
            make_groups(array.iter())
        } else {
            make_groups(array.values_iter())
        }
    }
}

impl IntoGroups for BooleanArray {
    fn make_groups(&self) -> DaftResult<super::GroupIndicesPair> {
        let array = self.as_arrow();
        if array.null_count() > 0 {
            make_groups(array.iter())
        } else {
            make_groups(array.values_iter())
        }
    }
}

impl IntoGroups for NullArray {
    fn make_groups(&self) -> DaftResult<super::GroupIndicesPair> {
        let l = self.len() as u64;
        if l == 0 {
            return Ok((vec![], vec![]));
        }
        let v = (0u64..l).collect::<Vec<u64>>();
        Ok((vec![0], vec![v]))
    }
}

impl IntoGroups for ListArray {
    fn make_groups(&self) -> DaftResult<super::GroupIndicesPair> {
        self.hash(None)?.make_groups()
    }
}

impl IntoGroups for FixedSizeListArray {
    fn make_groups(&self) -> DaftResult<super::GroupIndicesPair> {
        self.hash(None)?.make_groups()
    }
}

impl IntoGroups for StructArray {
    fn make_groups(&self) -> DaftResult<super::GroupIndicesPair> {
        self.hash(None)?.make_groups()
    }
}
