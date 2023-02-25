use std::cmp::Ordering;

use crate::{
    array::DataArray,
    datatypes::{
        BooleanArray, DaftIntegerType, DaftNumericType, Float32Array, Float64Array, NullArray,
        Utf8Array,
    },
    error::DaftResult,
};

use crate::array::BaseArray;
use arrow2::array::ord;
use num_traits::Float;

impl<T> DataArray<T>
where
    T: DaftIntegerType,
    <T as DaftNumericType>::Native: arrow2::types::Index,
{
    pub fn argsort<I>(&self, descending: bool) -> DaftResult<DataArray<I>>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let options = arrow2::compute::sort::SortOptions {
            descending,
            nulls_first: descending,
        };
        let arrow_array = self.downcast();

        let result =
            crate::array::ops::arrow2::sort::primitive::indices::indices_sorted_unstable_by::<
                I::Native,
                T::Native,
                _,
            >(arrow_array, ord::total_cmp, &options, None);

        Ok(DataArray::<I>::from((self.name(), Box::new(result))))
    }

    pub fn sort(&self, descending: bool) -> DaftResult<Self> {
        let options = arrow2::compute::sort::SortOptions {
            descending,
            nulls_first: descending,
        };

        let arrow_array = self.downcast();

        let result = crate::array::ops::arrow2::sort::primitive::sort::sort_by::<T::Native, _>(
            arrow_array,
            ord::total_cmp,
            &options,
            None,
        );

        Ok(DataArray::<T>::from((self.name(), Box::new(result))))
    }
}

#[allow(clippy::eq_op)]
#[inline]
pub fn cmp_float<F: Float>(l: &F, r: &F) -> std::cmp::Ordering {
    match (l.is_nan(), r.is_nan()) {
        (false, false) => unsafe { l.partial_cmp(r).unwrap_unchecked() },
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
    }
}

impl Float32Array {
    pub fn argsort<I>(&self, descending: bool) -> DaftResult<DataArray<I>>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let options = arrow2::compute::sort::SortOptions {
            descending,
            nulls_first: descending,
        };
        let arrow_array = self.downcast();

        let result =
            crate::array::ops::arrow2::sort::primitive::indices::indices_sorted_unstable_by::<
                I::Native,
                f32,
                _,
            >(arrow_array, cmp_float::<f32>, &options, None);

        Ok(DataArray::<I>::from((self.name(), Box::new(result))))
    }

    pub fn sort(&self, descending: bool) -> DaftResult<Self> {
        let options = arrow2::compute::sort::SortOptions {
            descending,
            nulls_first: descending,
        };

        let arrow_array = self.downcast();

        let result = crate::array::ops::arrow2::sort::primitive::sort::sort_by::<f32, _>(
            arrow_array,
            cmp_float::<f32>,
            &options,
            None,
        );

        Ok(Float32Array::from((self.name(), Box::new(result))))
    }
}

impl Float64Array {
    pub fn argsort<I>(&self, descending: bool) -> DaftResult<DataArray<I>>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let options = arrow2::compute::sort::SortOptions {
            descending,
            nulls_first: descending,
        };
        let arrow_array = self.downcast();

        let result =
            crate::array::ops::arrow2::sort::primitive::indices::indices_sorted_unstable_by::<
                I::Native,
                f64,
                _,
            >(arrow_array, cmp_float::<f64>, &options, None);

        Ok(DataArray::<I>::from((self.name(), Box::new(result))))
    }

    pub fn sort(&self, descending: bool) -> DaftResult<Self> {
        let options = arrow2::compute::sort::SortOptions {
            descending,
            nulls_first: descending,
        };

        let arrow_array = self.downcast();

        let result = crate::array::ops::arrow2::sort::primitive::sort::sort_by::<f64, _>(
            arrow_array,
            cmp_float::<f64>,
            &options,
            None,
        );

        Ok(Float64Array::from((self.name(), Box::new(result))))
    }
}

impl NullArray {
    pub fn argsort<I>(&self, _descending: bool) -> DaftResult<DataArray<I>>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        DataArray::<I>::arange(self.name(), 0_i64, self.len() as i64, 1)
    }

    pub fn sort(&self, _descending: bool) -> DaftResult<Self> {
        Ok(self.clone())
    }
}

impl BooleanArray {
    pub fn argsort<I>(&self, descending: bool) -> DaftResult<DataArray<I>>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let options = arrow2::compute::sort::SortOptions {
            descending,
            nulls_first: descending,
        };

        let result =
            arrow2::compute::sort::sort_to_indices::<I::Native>(self.data(), &options, None)?;

        Ok(DataArray::<I>::from((self.name(), Box::new(result))))
    }

    pub fn sort(&self, descending: bool) -> DaftResult<Self> {
        let options = arrow2::compute::sort::SortOptions {
            descending,
            nulls_first: descending,
        };

        let result = arrow2::compute::sort::sort(self.data(), &options, None)?;

        BooleanArray::try_from((self.name(), result))
    }
}

impl Utf8Array {
    pub fn argsort<I>(&self, descending: bool) -> DaftResult<DataArray<I>>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let options = arrow2::compute::sort::SortOptions {
            descending,
            nulls_first: descending,
        };

        let result =
            arrow2::compute::sort::sort_to_indices::<I::Native>(self.data(), &options, None)?;

        Ok(DataArray::<I>::from((self.name(), Box::new(result))))
    }

    pub fn sort(&self, descending: bool) -> DaftResult<Self> {
        let options = arrow2::compute::sort::SortOptions {
            descending,
            nulls_first: descending,
        };

        let result = arrow2::compute::sort::sort(self.data(), &options, None)?;

        Utf8Array::try_from((self.name(), result))
    }
}
