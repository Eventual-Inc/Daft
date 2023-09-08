use crate::{
    array::{DataArray, FixedSizeListArray, ListArray, StructArray},
    datatypes::{
        logical::{
            DateArray, Decimal128Array, DurationArray, EmbeddingArray, FixedShapeImageArray,
            FixedShapeTensorArray, ImageArray, TensorArray, TimestampArray,
        },
        BinaryArray, BooleanArray, DaftIntegerType, DaftNumericType, ExtensionArray, Float32Array,
        Float64Array, NullArray, Utf8Array,
    },
    kernels::search_sorted::{build_compare_with_nulls, cmp_float},
    series::Series,
};
use common_error::DaftResult;

#[cfg(feature = "python")]
use crate::datatypes::PythonArray;

use arrow2::{
    array::ord::{self, DynComparator},
    types::Index,
};

use super::arrow2::sort::primitive::common::multi_column_idx_sort;

use super::as_arrow::AsArrow;

pub fn build_multi_array_compare(
    arrays: &[Series],
    descending: &[bool],
) -> DaftResult<DynComparator> {
    build_multi_array_bicompare(arrays, arrays, descending)
}

pub fn build_multi_array_bicompare(
    left: &[Series],
    right: &[Series],
    descending: &[bool],
) -> DaftResult<DynComparator> {
    let mut cmp_list = Vec::with_capacity(left.len());

    for ((l, r), desc) in left.iter().zip(right.iter()).zip(descending.iter()) {
        cmp_list.push(build_compare_with_nulls(
            l.to_arrow().as_ref(),
            r.to_arrow().as_ref(),
            *desc,
        )?);
    }

    let combined_comparator = Box::new(move |a_idx: usize, b_idx: usize| -> std::cmp::Ordering {
        for comparator in cmp_list.iter() {
            match comparator(a_idx, b_idx) {
                std::cmp::Ordering::Equal => continue,
                other => return other,
            }
        }
        std::cmp::Ordering::Equal
    });
    Ok(combined_comparator)
}

impl<T> DataArray<T>
where
    T: DaftIntegerType,
    <T as DaftNumericType>::Native: Ord,
{
    pub fn argsort<I>(&self, descending: bool) -> DaftResult<DataArray<I>>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let arrow_array = self.as_arrow();

        let result =
            crate::array::ops::arrow2::sort::primitive::indices::indices_sorted_unstable_by::<
                I::Native,
                T::Native,
                _,
            >(arrow_array, ord::total_cmp, descending);

        Ok(DataArray::<I>::from((self.name(), Box::new(result))))
    }

    pub fn argsort_multikey<I>(
        &self,
        others: &[Series],
        descending: &[bool],
    ) -> DaftResult<DataArray<I>>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let arrow_array = self.as_arrow();
        let first_desc = *descending.first().unwrap();

        let others_cmp = build_multi_array_compare(others, &descending[1..])?;

        let values = arrow_array.values().as_slice();

        let result = if first_desc {
            multi_column_idx_sort(
                arrow_array.validity(),
                |a: &I::Native, b: &I::Native| {
                    let a = a.to_usize();
                    let b = b.to_usize();
                    let l = unsafe { values.get_unchecked(a) };
                    let r = unsafe { values.get_unchecked(b) };
                    match ord::total_cmp(r, l) {
                        std::cmp::Ordering::Equal => others_cmp(a, b),
                        v => v,
                    }
                },
                &others_cmp,
                arrow_array.len(),
                first_desc,
            )
        } else {
            multi_column_idx_sort(
                arrow_array.validity(),
                |a: &I::Native, b: &I::Native| {
                    let a = a.to_usize();
                    let b = b.to_usize();
                    let l = unsafe { values.get_unchecked(a) };
                    let r = unsafe { values.get_unchecked(b) };
                    match ord::total_cmp(l, r) {
                        std::cmp::Ordering::Equal => others_cmp(a, b),
                        v => v,
                    }
                },
                &others_cmp,
                arrow_array.len(),
                first_desc,
            )
        };

        Ok(DataArray::<I>::from((self.name(), Box::new(result))))
    }

    pub fn sort(&self, descending: bool) -> DaftResult<Self> {
        let options = arrow2::compute::sort::SortOptions {
            descending,
            nulls_first: descending,
        };

        let arrow_array = self.as_arrow();

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
        let arrow_array = self.as_arrow();

        let result =
            crate::array::ops::arrow2::sort::primitive::indices::indices_sorted_unstable_by::<
                I::Native,
                f32,
                _,
            >(arrow_array, cmp_float::<f32>, descending);

        Ok(DataArray::<I>::from((self.name(), Box::new(result))))
    }

    pub fn argsort_multikey<I>(
        &self,
        others: &[Series],
        descending: &[bool],
    ) -> DaftResult<DataArray<I>>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let arrow_array = self.as_arrow();
        let first_desc = *descending.first().unwrap();

        let others_cmp = build_multi_array_compare(others, &descending[1..])?;

        let values = arrow_array.values().as_slice();

        let result = if first_desc {
            multi_column_idx_sort(
                arrow_array.validity(),
                |a: &I::Native, b: &I::Native| {
                    let a = a.to_usize();
                    let b = b.to_usize();
                    let l = unsafe { values.get_unchecked(a) };
                    let r = unsafe { values.get_unchecked(b) };
                    match cmp_float::<f32>(r, l) {
                        std::cmp::Ordering::Equal => others_cmp(a, b),
                        v => v,
                    }
                },
                &others_cmp,
                arrow_array.len(),
                first_desc,
            )
        } else {
            multi_column_idx_sort(
                arrow_array.validity(),
                |a: &I::Native, b: &I::Native| {
                    let a = a.to_usize();
                    let b = b.to_usize();
                    let l = unsafe { values.get_unchecked(a) };
                    let r = unsafe { values.get_unchecked(b) };
                    match cmp_float::<f32>(l, r) {
                        std::cmp::Ordering::Equal => others_cmp(a, b),
                        v => v,
                    }
                },
                &others_cmp,
                arrow_array.len(),
                first_desc,
            )
        };

        Ok(DataArray::<I>::from((self.name(), Box::new(result))))
    }

    pub fn sort(&self, descending: bool) -> DaftResult<Self> {
        let options = arrow2::compute::sort::SortOptions {
            descending,
            nulls_first: descending,
        };

        let arrow_array = self.as_arrow();

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
        let arrow_array = self.as_arrow();

        let result =
            crate::array::ops::arrow2::sort::primitive::indices::indices_sorted_unstable_by::<
                I::Native,
                f64,
                _,
            >(arrow_array, cmp_float::<f64>, descending);

        Ok(DataArray::<I>::from((self.name(), Box::new(result))))
    }

    pub fn argsort_multikey<I>(
        &self,
        others: &[Series],
        descending: &[bool],
    ) -> DaftResult<DataArray<I>>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let arrow_array = self.as_arrow();
        let first_desc = *descending.first().unwrap();

        let others_cmp = build_multi_array_compare(others, &descending[1..])?;

        let values = arrow_array.values().as_slice();

        let result = if first_desc {
            multi_column_idx_sort(
                arrow_array.validity(),
                |a: &I::Native, b: &I::Native| {
                    let a = a.to_usize();
                    let b = b.to_usize();
                    let l = unsafe { values.get_unchecked(a) };
                    let r = unsafe { values.get_unchecked(b) };
                    match cmp_float::<f64>(r, l) {
                        std::cmp::Ordering::Equal => others_cmp(a, b),
                        v => v,
                    }
                },
                &others_cmp,
                arrow_array.len(),
                first_desc,
            )
        } else {
            multi_column_idx_sort(
                arrow_array.validity(),
                |a: &I::Native, b: &I::Native| {
                    let a = a.to_usize();
                    let b = b.to_usize();
                    let l = unsafe { values.get_unchecked(a) };
                    let r = unsafe { values.get_unchecked(b) };
                    match cmp_float::<f64>(l, r) {
                        std::cmp::Ordering::Equal => others_cmp(a, b),
                        v => v,
                    }
                },
                &others_cmp,
                arrow_array.len(),
                first_desc,
            )
        };

        Ok(DataArray::<I>::from((self.name(), Box::new(result))))
    }

    pub fn sort(&self, descending: bool) -> DaftResult<Self> {
        let options = arrow2::compute::sort::SortOptions {
            descending,
            nulls_first: descending,
        };

        let arrow_array = self.as_arrow();

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

    pub fn argsort_multikey<I>(
        &self,
        others: &[Series],
        descending: &[bool],
    ) -> DaftResult<DataArray<I>>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let first_desc = *descending.first().unwrap();

        let others_cmp = build_multi_array_compare(others, &descending[1..])?;

        let result = multi_column_idx_sort(
            self.data().validity(),
            |a: &I::Native, b: &I::Native| {
                let a = a.to_usize();
                let b = b.to_usize();
                others_cmp(a, b)
            },
            &others_cmp,
            self.len(),
            first_desc,
        );

        Ok(DataArray::<I>::from((self.name(), Box::new(result))))
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

    pub fn argsort_multikey<I>(
        &self,
        others: &[Series],
        descending: &[bool],
    ) -> DaftResult<DataArray<I>>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let first_desc = *descending.first().unwrap();

        let others_cmp = build_multi_array_compare(others, &descending[1..])?;

        let values = self
            .data()
            .as_any()
            .downcast_ref::<arrow2::array::BooleanArray>()
            .unwrap()
            .values();

        let result = if first_desc {
            multi_column_idx_sort(
                self.data().validity(),
                |a: &I::Native, b: &I::Native| {
                    let a = a.to_usize();
                    let b = b.to_usize();
                    let l = unsafe { values.get_bit_unchecked(a) };
                    let r = unsafe { values.get_bit_unchecked(b) };
                    match r.cmp(&l) {
                        std::cmp::Ordering::Equal => others_cmp(a, b),
                        v => v,
                    }
                },
                &others_cmp,
                self.len(),
                first_desc,
            )
        } else {
            multi_column_idx_sort(
                self.data().validity(),
                |a: &I::Native, b: &I::Native| {
                    let a = a.to_usize();
                    let b = b.to_usize();
                    let l = unsafe { values.get_bit_unchecked(a) };
                    let r = unsafe { values.get_bit_unchecked(b) };
                    match l.cmp(&r) {
                        std::cmp::Ordering::Equal => others_cmp(a, b),
                        v => v,
                    }
                },
                &others_cmp,
                self.len(),
                first_desc,
            )
        };

        Ok(DataArray::<I>::from((self.name(), Box::new(result))))
    }

    pub fn sort(&self, descending: bool) -> DaftResult<Self> {
        let options = arrow2::compute::sort::SortOptions {
            descending,
            nulls_first: descending,
        };

        let result = arrow2::compute::sort::sort(self.data(), &options, None)?;

        BooleanArray::try_from((self.field.clone(), result))
    }
}

macro_rules! impl_binary_like_sort {
    ($da:ident) => {
        impl $da {
            pub fn argsort<I>(&self, descending: bool) -> DaftResult<DataArray<I>>
            where
                I: DaftIntegerType,
                <I as DaftNumericType>::Native: arrow2::types::Index,
            {
                let options = arrow2::compute::sort::SortOptions {
                    descending,
                    nulls_first: descending,
                };

                let result = arrow2::compute::sort::sort_to_indices::<I::Native>(
                    self.data(),
                    &options,
                    None,
                )?;

                Ok(DataArray::<I>::from((self.name(), Box::new(result))))
            }

            pub fn argsort_multikey<I>(
                &self,
                others: &[Series],
                descending: &[bool],
            ) -> DaftResult<DataArray<I>>
            where
                I: DaftIntegerType,
                <I as DaftNumericType>::Native: arrow2::types::Index,
            {
                let first_desc = *descending.first().unwrap();

                let others_cmp = build_multi_array_compare(others, &descending[1..])?;

                let values = self.as_arrow();

                let result = if first_desc {
                    multi_column_idx_sort(
                        self.data().validity(),
                        |a: &I::Native, b: &I::Native| {
                            let a = a.to_usize();
                            let b = b.to_usize();
                            let l = unsafe { values.value_unchecked(a) };
                            let r = unsafe { values.value_unchecked(b) };
                            match r.cmp(&l) {
                                std::cmp::Ordering::Equal => others_cmp(a, b),
                                v => v,
                            }
                        },
                        &others_cmp,
                        self.len(),
                        first_desc,
                    )
                } else {
                    multi_column_idx_sort(
                        self.data().validity(),
                        |a: &I::Native, b: &I::Native| {
                            let a = a.to_usize();
                            let b = b.to_usize();
                            let l = unsafe { values.value_unchecked(a) };
                            let r = unsafe { values.value_unchecked(b) };
                            match l.cmp(&r) {
                                std::cmp::Ordering::Equal => others_cmp(a, b),
                                v => v,
                            }
                        },
                        &others_cmp,
                        self.len(),
                        first_desc,
                    )
                };

                Ok(DataArray::<I>::from((self.name(), Box::new(result))))
            }

            pub fn sort(&self, descending: bool) -> DaftResult<Self> {
                let options = arrow2::compute::sort::SortOptions {
                    descending,
                    nulls_first: descending,
                };

                let result = arrow2::compute::sort::sort(self.data(), &options, None)?;

                $da::try_from((self.field.clone(), result))
            }
        }
    };
}

impl_binary_like_sort!(BinaryArray);
impl_binary_like_sort!(Utf8Array);

impl FixedSizeListArray {
    pub fn sort(&self, _descending: bool) -> DaftResult<Self> {
        todo!("impl sort for FixedSizeListArray")
    }
}

impl ListArray {
    pub fn sort(&self, _descending: bool) -> DaftResult<Self> {
        todo!("impl sort for ListArray")
    }
}

impl StructArray {
    pub fn sort(&self, _descending: bool) -> DaftResult<Self> {
        todo!("impl sort for StructArray")
    }
}

impl ExtensionArray {
    pub fn sort(&self, _descending: bool) -> DaftResult<Self> {
        todo!("impl sort for ExtensionArray")
    }
}

#[cfg(feature = "python")]
impl PythonArray {
    pub fn sort(&self, _descending: bool) -> DaftResult<Self> {
        todo!("impl sort for python array")
    }
}

impl Decimal128Array {
    pub fn sort(&self, descending: bool) -> DaftResult<Self> {
        let new_array = self.physical.sort(descending)?;
        Ok(Self::new(self.field.clone(), new_array))
    }
}

impl DateArray {
    pub fn sort(&self, descending: bool) -> DaftResult<Self> {
        let new_array = self.physical.sort(descending)?;
        Ok(Self::new(self.field.clone(), new_array))
    }
}

impl DurationArray {
    pub fn sort(&self, descending: bool) -> DaftResult<Self> {
        let new_array = self.physical.sort(descending)?;
        Ok(Self::new(self.field.clone(), new_array))
    }
}

impl TimestampArray {
    pub fn sort(&self, descending: bool) -> DaftResult<Self> {
        let new_array = self.physical.sort(descending)?;
        Ok(Self::new(self.field.clone(), new_array))
    }
}

impl EmbeddingArray {
    pub fn sort(&self, _descending: bool) -> DaftResult<Self> {
        todo!("impl sort for EmbeddingArray")
    }
}

impl ImageArray {
    pub fn sort(&self, _descending: bool) -> DaftResult<Self> {
        todo!("impl sort for ImageArray")
    }
}

impl FixedShapeImageArray {
    pub fn sort(&self, _descending: bool) -> DaftResult<Self> {
        todo!("impl sort for FixedShapeImageArray")
    }
}

impl TensorArray {
    pub fn sort(&self, _descending: bool) -> DaftResult<Self> {
        todo!("impl sort for TensorArray")
    }
}

impl FixedShapeTensorArray {
    pub fn sort(&self, _descending: bool) -> DaftResult<Self> {
        todo!("impl sort for FixedShapeTensorArray")
    }
}
