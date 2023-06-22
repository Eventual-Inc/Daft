use crate::{
    array::DataArray,
    datatypes::{BooleanArray, DaftNumericType, NullArray, UInt64Type, Utf8Array},
};
use common_error::DaftResult;

use super::as_arrow::AsArrow;

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn pairwise_compare<F>(
        &self,
        other: &Self,
        func: F,
    ) -> DaftResult<(DataArray<UInt64Type>, DataArray<UInt64Type>)>
    where
        F: Fn(T::Native, T::Native) -> bool,
    {
        let mut left_idx = vec![];
        let mut right_idx = vec![];

        for (i, l) in self.as_arrow().iter().enumerate() {
            if l.is_none() {
                continue;
            }
            let l = *l.unwrap();
            for (j, r) in other.as_arrow().iter().enumerate() {
                match r {
                    None => continue,
                    Some(r) => {
                        if func(l, *r) {
                            left_idx.push(i as u64);
                            right_idx.push(j as u64);
                        }
                    }
                }
            }
        }
        let left_series = DataArray::from((self.name(), left_idx));
        let right_series = DataArray::from((other.name(), right_idx));
        Ok((left_series, right_series))
    }
}

impl Utf8Array {
    pub fn pairwise_compare<F>(
        &self,
        other: &Self,
        func: F,
    ) -> DaftResult<(DataArray<UInt64Type>, DataArray<UInt64Type>)>
    where
        F: Fn(&str, &str) -> bool,
    {
        let mut left_idx = vec![];
        let mut right_idx = vec![];

        for (i, l) in self.as_arrow().iter().enumerate() {
            if l.is_none() {
                continue;
            }
            let l = l.unwrap();
            for (j, r) in other.as_arrow().iter().enumerate() {
                match r {
                    None => continue,
                    Some(r) => {
                        if func(l, r) {
                            left_idx.push(i as u64);
                            right_idx.push(j as u64);
                        }
                    }
                }
            }
        }
        let left_series = DataArray::from((self.name(), left_idx));
        let right_series = DataArray::from((other.name(), right_idx));
        Ok((left_series, right_series))
    }
}

impl BooleanArray {
    pub fn pairwise_compare<F>(
        &self,
        other: &Self,
        func: F,
    ) -> DaftResult<(DataArray<UInt64Type>, DataArray<UInt64Type>)>
    where
        F: Fn(bool, bool) -> bool,
    {
        let mut left_idx = vec![];
        let mut right_idx = vec![];

        for (i, l) in self.as_arrow().iter().enumerate() {
            if l.is_none() {
                continue;
            }
            let l = l.unwrap();
            for (j, r) in other.as_arrow().iter().enumerate() {
                match r {
                    None => continue,
                    Some(r) => {
                        if func(l, r) {
                            left_idx.push(i as u64);
                            right_idx.push(j as u64);
                        }
                    }
                }
            }
        }
        let left_series = DataArray::from((self.name(), left_idx));
        let right_series = DataArray::from((other.name(), right_idx));
        Ok((left_series, right_series))
    }
}

impl NullArray {
    pub fn pairwise_compare<F>(
        &self,
        other: &Self,
        _func: F,
    ) -> DaftResult<(DataArray<UInt64Type>, DataArray<UInt64Type>)>
    where
        F: Fn(bool, bool) -> bool,
    {
        let left_idx = vec![];
        let right_idx = vec![];
        let left_series = DataArray::from((self.name(), left_idx));
        let right_series = DataArray::from((other.name(), right_idx));
        Ok((left_series, right_series))
    }
}
