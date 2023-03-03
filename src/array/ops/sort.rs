use crate::{
    array::DataArray,
    datatypes::{
        BooleanArray, DaftIntegerType, DaftNumericType, Float32Array, Float64Array, NullArray,
        Utf8Array,
    },
    error::DaftResult,
    kernels::search_sorted::cmp_float,
};

use crate::array::BaseArray;
use arrow2::array::ord::{self};

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

    // pub fn argsort_multikey<I>(&self, descending: bool, others: &[Series]) -> DaftResult<DataArray<I>>
    // where
    //     I: DaftIntegerType,
    //     <I as DaftNumericType>::Native: arrow2::types::Index,
    // {
    //     let options = arrow2::compute::sort::SortOptions {
    //         descending,
    //         nulls_first: descending,
    //     };
    //     let arrow_array = self.downcast();

    //     let options = SortOptions {
    //         descending: descending,
    //         nulls_first: descending
    //     };

    //     let mut compare: Vec<_> = Vec::with_capacity(others.len());
    //     for s in others.iter() {
    //         compare.push(build_compare_with_nulls(s.array().data(), s.array().data(), descending)?);
    //     }

    //     let result =
    //         crate::array::ops::arrow2::sort::primitive::indices::indices_sorted_unstable_by::<
    //             I::Native,
    //             T::Native,
    //             _,
    //         >(arrow_array, |l, r| {
    //             match ord::total_cmp(l, r) {
    //                 std::cmp::Ordering::Equal => {
    //                     for st
    //                 }
    //                 v => v
    //             }
    //         }, &options, None);

    //     Ok(DataArray::<I>::from((self.name(), Box::new(result))))
    // }

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
