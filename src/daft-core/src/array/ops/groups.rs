use std::{
    collections::hash_map::Entry::{Occupied, Vacant},
    hash::{BuildHasherDefault, Hash},
};

use arrow::{array::Array, datatypes::ArrowPrimitiveType};
use common_error::DaftResult;
use fnv::FnvHashMap;

use super::{IntoGroups, as_arrow::AsArrow};
use crate::{
    array::{DataArray, FixedSizeListArray, ListArray, StructArray, ops::IntoUniqueIdxs},
    datatypes::{
        BinaryArray, BooleanArray, DaftIntegerType, DaftNumericType, FixedSizeBinaryArray,
        Float32Array, Float64Array, NullArray, NumericNative, Utf8Array,
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

/// Given a list of values, return a `Vec<u64>` containing the index of the first
/// appearance of each unique value.
///
/// # Example:
/// ```text
/// +---+
/// | a | (index: 0)
/// | b | (index: 1)
/// | a |
/// | c | (index: 3)
/// | a |
/// +---+
/// ```
///
/// Calling `make_unique_idxs` on the above list would return `vec![0, 1, 3]`
/// since those are the first indices where each unique value (`a`, `b`, `c`) appears.
fn make_unique_idxs<T>(iter: impl Iterator<Item = T>) -> DaftResult<super::VecIndices>
where
    T: Hash,
    T: Eq,
{
    const DEFAULT_SIZE: usize = 256;
    let mut tbl =
        FnvHashMap::<T, u64>::with_capacity_and_hasher(DEFAULT_SIZE, BuildHasherDefault::default());
    for (idx, val) in iter.enumerate() {
        let idx = idx as u64;
        let e = tbl.entry(val);
        match e {
            Vacant(e) => {
                e.insert(idx);
            }
            Occupied(_) => {}
        }
    }
    let mut indices = Vec::with_capacity(tbl.len());
    for idx in tbl.into_values() {
        indices.push(idx);
    }
    Ok(indices)
}

impl<T> IntoGroups for DataArray<T>
where
    T: DaftIntegerType,
    <T as DaftNumericType>::Native: Ord + Hash + Eq,
    <<T as DaftNumericType>::Native as NumericNative>::ARROWTYPE:
        ArrowPrimitiveType<Native = <T as DaftNumericType>::Native>,
{
    fn make_groups(&self) -> DaftResult<super::GroupIndicesPair> {
        let array = self.as_arrow()?;
        if array.null_count() > 0 {
            make_groups(array.iter())
        } else {
            make_groups(array.values().iter())
        }
    }
}

impl<T> IntoUniqueIdxs for DataArray<T>
where
    T: DaftIntegerType,
    <T as DaftNumericType>::Native: Ord + Hash + Eq,
    <<T as DaftNumericType>::Native as NumericNative>::ARROWTYPE:
        ArrowPrimitiveType<Native = <T as DaftNumericType>::Native>,
{
    fn make_unique_idxs(&self) -> DaftResult<super::VecIndices> {
        let array = self.as_arrow()?;
        if array.null_count() > 0 {
            make_unique_idxs(array.iter())
        } else {
            make_unique_idxs(array.values().iter())
        }
    }
}

impl IntoGroups for Decimal128Array {
    fn make_groups(&self) -> DaftResult<super::GroupIndicesPair> {
        let array = self.as_arrow()?;
        if array.null_count() > 0 {
            make_groups(array.iter())
        } else {
            make_groups(array.values().iter())
        }
    }
}

impl IntoUniqueIdxs for Decimal128Array {
    fn make_unique_idxs(&self) -> DaftResult<super::VecIndices> {
        let array = self.as_arrow()?;
        if array.null_count() > 0 {
            make_unique_idxs(array.iter())
        } else {
            make_unique_idxs(array.values().iter())
        }
    }
}

impl IntoGroups for Float32Array {
    fn make_groups(&self) -> DaftResult<super::GroupIndicesPair> {
        let array = self.as_arrow()?;
        if array.null_count() > 0 {
            make_groups(array.iter().map(|f| {
                f.map(|v| {
                    match v.is_nan() {
                        true => f32::NAN,
                        false => v,
                    }
                    .to_bits()
                })
            }))
        } else {
            make_groups(array.values().iter().map(|f| {
                match f.is_nan() {
                    true => f32::NAN,
                    false => *f,
                }
                .to_bits()
            }))
        }
    }
}

impl IntoUniqueIdxs for Float32Array {
    fn make_unique_idxs(&self) -> DaftResult<super::VecIndices> {
        let array = self.as_arrow()?;
        if array.null_count() > 0 {
            make_unique_idxs(array.iter().map(|f| {
                f.map(|v| {
                    match v.is_nan() {
                        true => f32::NAN,
                        false => v,
                    }
                    .to_bits()
                })
            }))
        } else {
            make_unique_idxs(array.values().iter().map(|f| {
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
        let array = self.as_arrow()?;
        if array.null_count() > 0 {
            make_groups(array.iter().map(|f| {
                f.map(|v| {
                    match v.is_nan() {
                        true => f64::NAN,
                        false => v,
                    }
                    .to_bits()
                })
            }))
        } else {
            make_groups(array.values().iter().map(|f| {
                match f.is_nan() {
                    true => f64::NAN,
                    false => *f,
                }
                .to_bits()
            }))
        }
    }
}

impl IntoUniqueIdxs for Float64Array {
    fn make_unique_idxs(&self) -> DaftResult<super::VecIndices> {
        let array = self.as_arrow()?;
        if array.null_count() > 0 {
            make_unique_idxs(array.iter().map(|f| {
                f.map(|v| {
                    match v.is_nan() {
                        true => f64::NAN,
                        false => v,
                    }
                    .to_bits()
                })
            }))
        } else {
            make_unique_idxs(array.values().iter().map(|f| {
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
        let array = self.as_arrow()?;
        if array.null_count() > 0 {
            make_groups(array.iter())
        } else {
            make_groups(array.values().iter())
        }
    }
}

impl IntoUniqueIdxs for Utf8Array {
    fn make_unique_idxs(&self) -> DaftResult<super::VecIndices> {
        let array = self.as_arrow()?;
        if array.null_count() > 0 {
            make_unique_idxs(array.iter())
        } else {
            make_unique_idxs(array.values().iter())
        }
    }
}

impl IntoGroups for BinaryArray {
    fn make_groups(&self) -> DaftResult<super::GroupIndicesPair> {
        let array = self.as_arrow()?;
        if array.null_count() > 0 {
            make_groups(array.iter())
        } else {
            make_groups(array.values().iter())
        }
    }
}

impl IntoUniqueIdxs for BinaryArray {
    fn make_unique_idxs(&self) -> DaftResult<super::VecIndices> {
        let array = self.as_arrow()?;
        if array.null_count() > 0 {
            make_unique_idxs(array.iter())
        } else {
            make_unique_idxs(array.values().iter())
        }
    }
}

impl IntoGroups for FixedSizeBinaryArray {
    fn make_groups(&self) -> DaftResult<super::GroupIndicesPair> {
        let array = self.as_arrow()?;
        if array.null_count() > 0 {
            make_groups(array.iter())
        } else {
            make_groups(array.values().iter())
        }
    }
}

impl IntoUniqueIdxs for FixedSizeBinaryArray {
    fn make_unique_idxs(&self) -> DaftResult<super::VecIndices> {
        let array = self.as_arrow()?;
        if array.null_count() > 0 {
            make_unique_idxs(array.iter())
        } else {
            make_unique_idxs(array.values().iter())
        }
    }
}

impl IntoGroups for BooleanArray {
    fn make_groups(&self) -> DaftResult<super::GroupIndicesPair> {
        let array = self.as_arrow()?;
        if array.null_count() > 0 {
            make_groups(array.iter())
        } else {
            make_groups(array.values().iter())
        }
    }
}

impl IntoUniqueIdxs for BooleanArray {
    fn make_unique_idxs(&self) -> DaftResult<super::VecIndices> {
        let array = self.as_arrow()?;
        if array.null_count() > 0 {
            make_unique_idxs(array.iter())
        } else {
            make_unique_idxs(array.values().iter())
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

impl IntoUniqueIdxs for NullArray {
    fn make_unique_idxs(&self) -> DaftResult<super::VecIndices> {
        let l = self.len() as u64;
        if l == 0 {
            return Ok(vec![]);
        }
        Ok(vec![0])
    }
}

impl IntoGroups for ListArray {
    fn make_groups(&self) -> DaftResult<super::GroupIndicesPair> {
        self.hash(None)?.make_groups()
    }
}

impl IntoUniqueIdxs for ListArray {
    fn make_unique_idxs(&self) -> DaftResult<super::VecIndices> {
        self.hash(None)?.make_unique_idxs()
    }
}

impl IntoGroups for FixedSizeListArray {
    fn make_groups(&self) -> DaftResult<super::GroupIndicesPair> {
        self.hash(None)?.make_groups()
    }
}

impl IntoUniqueIdxs for FixedSizeListArray {
    fn make_unique_idxs(&self) -> DaftResult<super::VecIndices> {
        self.hash(None)?.make_unique_idxs()
    }
}

impl IntoGroups for StructArray {
    fn make_groups(&self) -> DaftResult<super::GroupIndicesPair> {
        self.hash(None)?.make_groups()
    }
}

impl IntoUniqueIdxs for StructArray {
    fn make_unique_idxs(&self) -> DaftResult<super::VecIndices> {
        self.hash(None)?.make_unique_idxs()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use common_error::DaftResult;

    use crate::{
        array::ops::{IntoGroups, IntoUniqueIdxs, full::FullNull},
        datatypes::{DataType, Field, Float64Array, Int64Array, NullArray},
    };

    fn grouped_by_sample(
        sample_indices: Vec<u64>,
        group_indices: Vec<Vec<u64>>,
    ) -> BTreeMap<u64, Vec<u64>> {
        sample_indices
            .into_iter()
            .zip(group_indices)
            .collect::<BTreeMap<_, _>>()
    }

    #[test]
    fn test_make_groups_int64_with_nulls() -> DaftResult<()> {
        let arr = Int64Array::from_iter(
            Field::new("a", DataType::Int64),
            [Some(1i64), None, Some(1), None, Some(2)].into_iter(),
        );

        let (sample_indices, group_indices) = arr.make_groups()?;
        let grouped = grouped_by_sample(sample_indices, group_indices);

        assert_eq!(
            grouped,
            BTreeMap::from([
                (0u64, vec![0u64, 2u64]),
                (1u64, vec![1u64, 3u64]),
                (4u64, vec![4u64]),
            ])
        );

        let mut unique_idxs = arr.make_unique_idxs()?;
        unique_idxs.sort_unstable();
        assert_eq!(unique_idxs, vec![0, 1, 4]);
        Ok(())
    }

    #[test]
    fn test_make_groups_float64_canonicalizes_nan() -> DaftResult<()> {
        let nan_1 = f64::from_bits(0x7ff8_0000_0000_0001);
        let nan_2 = f64::from_bits(0x7ff8_0000_0000_0002);
        let arr = Float64Array::from_values("a", [nan_1, 1.0, nan_2, 1.0]);

        let (sample_indices, group_indices) = arr.make_groups()?;
        let grouped = grouped_by_sample(sample_indices, group_indices);

        assert_eq!(
            grouped,
            BTreeMap::from([(0u64, vec![0u64, 2u64]), (1u64, vec![1u64, 3u64])])
        );

        let mut unique_idxs = arr.make_unique_idxs()?;
        unique_idxs.sort_unstable();
        assert_eq!(unique_idxs, vec![0, 1]);
        Ok(())
    }

    #[test]
    fn test_null_array_groups_and_unique_idxs() -> DaftResult<()> {
        let arr = NullArray::full_null("a", &DataType::Null, 4);
        assert_eq!(arr.make_groups()?, (vec![0], vec![vec![0, 1, 2, 3]]));
        assert_eq!(arr.make_unique_idxs()?, vec![0]);

        let empty = NullArray::full_null("a", &DataType::Null, 0);
        assert_eq!(empty.make_groups()?, (vec![], vec![]));
        assert_eq!(empty.make_unique_idxs()?, Vec::<u64>::new());
        Ok(())
    }
}
