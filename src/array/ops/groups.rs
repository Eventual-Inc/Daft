use std::collections::{
    hash_map::Entry::{Occupied, Vacant},
    HashMap,
};

use crate::{
    array::DataArray,
    datatypes::{BooleanArray, DaftNumericType, NullArray, Utf8Array},
    error::DaftResult,
};

use super::{downcast::Downcastable, IntoGroups};

use std::hash::Hash;

fn make_groups<T>(iter: impl Iterator<Item = T>) -> DaftResult<super::GroupIndicesPair>
where
    T: Hash,
    T: std::cmp::Eq,
{
    let mut tbl = HashMap::<T, (u64, Vec<u64>)>::with_capacity(512);
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
    T: DaftNumericType,
    <T as DaftNumericType>::Native: Hash,
    <T as DaftNumericType>::Native: std::cmp::Eq,
{
    fn make_groups(&self) -> DaftResult<super::GroupIndicesPair> {
        make_groups(self.downcast().values_iter())
    }
}

impl IntoGroups for Utf8Array {
    fn make_groups(&self) -> DaftResult<super::GroupIndicesPair> {
        make_groups(self.downcast().values_iter())
    }
}

impl IntoGroups for BooleanArray {
    fn make_groups(&self) -> DaftResult<super::GroupIndicesPair> {
        make_groups(self.downcast().values_iter())
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
