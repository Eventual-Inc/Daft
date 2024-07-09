use std::collections::hash_map::Entry::{Occupied, Vacant};

use arrow2::array::Array;
use fnv::FnvHashMap;

use crate::{
    array::{DataArray, FixedSizeListArray, ListArray},
    datatypes::{
        BinaryArray, BooleanArray, DaftIntegerType, DaftNumericType, FixedSizeBinaryArray,
        Float32Array, Float64Array, NullArray, Utf8Array,
    },
};
use common_error::DaftResult;

use super::{as_arrow::AsArrow, IntoGroups};

use std::hash::Hash;

fn make_groups<T>(iter: impl Iterator<Item = T>) -> DaftResult<super::GroupIndicesPair>
where
    T: Hash,
    T: std::cmp::Eq,
{
    const DEFAULT_SIZE: usize = 256;
    let mut tbl = FnvHashMap::<T, (u64, Vec<u64>)>::with_capacity_and_hasher(
        DEFAULT_SIZE,
        Default::default(),
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
    let mut s_indices = Vec::with_capacity(tbl.len());
    let mut g_indices = Vec::with_capacity(tbl.len());

    for (s_idx, g_idx) in tbl.into_values() {
        s_indices.push(s_idx);
        g_indices.push(g_idx);
    }

    Ok((s_indices, g_indices))
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
