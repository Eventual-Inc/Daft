use crate::{
    array::DataArray,
    datatypes::{BooleanArray, DaftIntegerType, DaftNumericType, NullArray, Utf8Array},
    error::DaftResult,
};

use crate::array::BaseArray;

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn argsort<I>(&self, descending: bool) -> DaftResult<DataArray<I>>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let options = arrow2::compute::sort::SortOptions {
            descending: descending,
            nulls_first: descending,
        };

        let result =
            arrow2::compute::sort::sort_to_indices::<I::Native>(self.data(), &options, None)?;

        Ok(DataArray::<I>::from((self.name(), Box::new(result))))
    }

    pub fn sort(&self, descending: bool) -> DaftResult<Self> {
        let options = arrow2::compute::sort::SortOptions {
            descending: descending,
            nulls_first: descending,
        };

        let result = arrow2::compute::sort::sort(self.data(), &options, None)?;

        DataArray::<T>::try_from((self.name(), result))
    }
}

impl NullArray {
    pub fn argsort<I>(&self, _descending: bool) -> DaftResult<DataArray<I>>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        DataArray::<I>::arange(self.name(), 0 as i64, self.len() as i64, 1)
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
            descending: descending,
            nulls_first: descending,
        };

        let result =
            arrow2::compute::sort::sort_to_indices::<I::Native>(self.data(), &options, None)?;

        Ok(DataArray::<I>::from((self.name(), Box::new(result))))
    }

    pub fn sort(&self, descending: bool) -> DaftResult<Self> {
        let options = arrow2::compute::sort::SortOptions {
            descending: descending,
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
            descending: descending,
            nulls_first: descending,
        };

        let result =
            arrow2::compute::sort::sort_to_indices::<I::Native>(self.data(), &options, None)?;

        Ok(DataArray::<I>::from((self.name(), Box::new(result))))
    }

    pub fn sort(&self, descending: bool) -> DaftResult<Self> {
        let options = arrow2::compute::sort::SortOptions {
            descending: descending,
            nulls_first: descending,
        };

        let result = arrow2::compute::sort::sort(self.data(), &options, None)?;

        Utf8Array::try_from((self.name(), result))
    }
}
