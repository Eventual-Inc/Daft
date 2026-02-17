use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, ArrowPrimitiveType, Float32Array as ArrowFloat32Array,
        Float64Array as ArrowFloat64Array, make_comparator,
    },
    compute::SortOptions,
};
use common_error::{DaftError, DaftResult};
use daft_arrow::{
    array::ord::{self, DynComparator},
    // A real tragedy. Arrow-rs has all these functions but uses 32 bit indices instead of 64 bit indices.
    compute::sort::{sort, sort_to_indices},
    types::Index,
};

use super::as_arrow::AsArrow;
#[cfg(feature = "python")]
use crate::prelude::PythonArray;
use crate::{
    array::{
        DataArray, FixedSizeListArray, ListArray, StructArray,
        ops::arrow::sort::primitive::{
            common::multi_column_idx_sort, indices::indices_sorted_unstable_by, sort::sort_by,
        },
    },
    datatypes::{
        BinaryArray, BooleanArray, DaftIntegerType, DaftNumericType, DataType, Decimal128Array,
        ExtensionArray, Field, FileArray, FixedSizeBinaryArray, Float32Array, Float64Array,
        IntervalArray, NullArray, NumericNative, Utf8Array,
        logical::{
            DateArray, DurationArray, EmbeddingArray, FixedShapeImageArray,
            FixedShapeSparseTensorArray, FixedShapeTensorArray, ImageArray, MapArray,
            SparseTensorArray, TensorArray, TimeArray, TimestampArray,
        },
    },
    file::DaftMediaType,
    kernels::search_sorted::cmp_float,
    prelude::UInt64Array,
    series::Series,
};

pub fn build_multi_array_compare(
    arrays: &[Series],
    descending: &[bool],
    nulls_first: &[bool],
) -> DaftResult<DynComparator> {
    build_multi_array_bicompare(arrays, arrays, descending, nulls_first)
}

/// Canonicalize negative NaN to positive NaN so arrow-rs total ordering sorts them correctly.
///
/// arrow-rs `make_comparator` uses IEEE 754 total ordering where -NaN < -Inf < ... < +Inf < +NaN.
/// Daft's sort contract treats ALL NaN as greater than regular values, so we normalize
/// negative NaN to positive NaN before comparison.
///
/// TODO: Replace this with a custom `cmp_float`-based comparator to avoid the allocation.
fn canonicalize_nan(array: ArrayRef) -> ArrayRef {
    if let Some(f64_arr) = array.as_any().downcast_ref::<ArrowFloat64Array>() {
        let canonical: ArrowFloat64Array = f64_arr
            .iter()
            .map(|v| v.map(|x| if x.is_nan() { f64::NAN } else { x }))
            .collect();
        Arc::new(canonical)
    } else if let Some(f32_arr) = array.as_any().downcast_ref::<ArrowFloat32Array>() {
        let canonical: ArrowFloat32Array = f32_arr
            .iter()
            .map(|v| v.map(|x| if x.is_nan() { f32::NAN } else { x }))
            .collect();
        Arc::new(canonical)
    } else {
        array
    }
}

pub fn build_multi_array_bicompare(
    left: &[Series],
    right: &[Series],
    descending: &[bool],
    nulls_first: &[bool],
) -> DaftResult<DynComparator> {
    let mut cmp_list = Vec::with_capacity(left.len());

    for (((l, r), desc), nf) in left
        .iter()
        .zip(right.iter())
        .zip(descending.iter())
        .zip(nulls_first.iter())
    {
        let l_arrow = canonicalize_nan(l.to_arrow()?);
        let r_arrow = canonicalize_nan(r.to_arrow()?);
        cmp_list.push(
            make_comparator(
                l_arrow.as_ref(),
                r_arrow.as_ref(),
                SortOptions::new(*desc, *nf),
            )
            .map_err(DaftError::ArrowRsError)?,
        );
    }

    let combined_comparator = Box::new(move |a_idx: usize, b_idx: usize| -> std::cmp::Ordering {
        for comparator in &cmp_list {
            match comparator(a_idx, b_idx) {
                std::cmp::Ordering::Equal => {}
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
    <<<T as DaftNumericType>::Native as NumericNative>::ARROWTYPE as ArrowPrimitiveType>::Native:
        Ord,
{
    pub fn argsort(&self, descending: bool, nulls_first: bool) -> DaftResult<UInt64Array> {
        let arrow_array = self.as_arrow()?;

        let result =
            indices_sorted_unstable_by(&arrow_array, |l, r| l.cmp(r), descending, nulls_first);

        UInt64Array::from_arrow(
            Field::new(self.field().name.clone(), DataType::UInt64),
            Arc::new(result),
        )
    }

    pub fn argsort_multikey(
        &self,
        others: &[Series],
        descending: &[bool],
        nulls_first: &[bool],
    ) -> DaftResult<UInt64Array> {
        let arrow_array = self.as_arrow()?;
        let first_desc = *descending.first().unwrap();
        let first_nulls_first = *nulls_first.first().unwrap();

        let others_cmp = build_multi_array_compare(others, &descending[1..], &nulls_first[1..])?;

        let result = if first_desc {
            multi_column_idx_sort(
                arrow_array.nulls(),
                |a: &u64, b: &u64| {
                    let a = a.to_usize();
                    let b = b.to_usize();
                    let l = unsafe { &arrow_array.value_unchecked(a) };
                    let r = unsafe { &arrow_array.value_unchecked(b) };
                    match r.cmp(l) {
                        std::cmp::Ordering::Equal => others_cmp(a, b),
                        v => v,
                    }
                },
                &others_cmp,
                arrow_array.len(),
                first_nulls_first,
            )
        } else {
            multi_column_idx_sort(
                arrow_array.nulls(),
                |a: &u64, b: &u64| {
                    let a = a.to_usize();
                    let b = b.to_usize();
                    let l = unsafe { &arrow_array.value_unchecked(a) };
                    let r = unsafe { &arrow_array.value_unchecked(b) };
                    match l.cmp(r) {
                        std::cmp::Ordering::Equal => others_cmp(a, b),
                        v => v,
                    }
                },
                &others_cmp,
                arrow_array.len(),
                first_nulls_first,
            )
        };

        UInt64Array::from_arrow(
            Field::new(self.field().name.clone(), DataType::UInt64),
            Arc::new(result),
        )
    }

    pub fn sort(&self, descending: bool, nulls_first: bool) -> DaftResult<Self> {
        let options = SortOptions {
            descending,
            nulls_first,
        };

        let arrow_array = self.as_arrow()?;

        let result = sort_by(&arrow_array, |l, r| l.cmp(r), &options, None);

        Self::from_arrow(self.field().clone(), Arc::new(result))
    }
}

impl Float32Array {
    pub fn argsort(&self, descending: bool, nulls_first: bool) -> DaftResult<UInt64Array> {
        let arrow_array = self.as_arrow()?;

        let result =
            indices_sorted_unstable_by(&arrow_array, cmp_float::<f32>, descending, nulls_first);

        UInt64Array::from_arrow(
            Field::new(self.field().name.clone(), DataType::UInt64),
            Arc::new(result),
        )
    }

    pub fn argsort_multikey(
        &self,
        others: &[Series],
        descending: &[bool],
        nulls_first: &[bool],
    ) -> DaftResult<UInt64Array> {
        let arrow_array = self.as_arrow()?;
        let first_desc = *descending.first().unwrap();
        let first_nulls_first = *nulls_first.first().unwrap();

        let others_cmp = build_multi_array_compare(others, &descending[1..], &nulls_first[1..])?;

        let result = if first_desc {
            multi_column_idx_sort(
                arrow_array.nulls(),
                |a: &u64, b: &u64| {
                    let a = a.to_usize();
                    let b = b.to_usize();
                    let l = unsafe { &arrow_array.value_unchecked(a) };
                    let r = unsafe { &arrow_array.value_unchecked(b) };
                    match cmp_float::<f32>(r, l) {
                        std::cmp::Ordering::Equal => others_cmp(a, b),
                        v => v,
                    }
                },
                &others_cmp,
                arrow_array.len(),
                first_nulls_first,
            )
        } else {
            multi_column_idx_sort(
                arrow_array.nulls(),
                |a: &u64, b: &u64| {
                    let a = a.to_usize();
                    let b = b.to_usize();
                    let l = unsafe { &arrow_array.value_unchecked(a) };
                    let r = unsafe { &arrow_array.value_unchecked(b) };
                    match cmp_float::<f32>(l, r) {
                        std::cmp::Ordering::Equal => others_cmp(a, b),
                        v => v,
                    }
                },
                &others_cmp,
                arrow_array.len(),
                first_nulls_first,
            )
        };

        UInt64Array::from_arrow(
            Field::new(self.field().name.clone(), DataType::UInt64),
            Arc::new(result),
        )
    }

    pub fn sort(&self, descending: bool, nulls_first: bool) -> DaftResult<Self> {
        let options = SortOptions {
            descending,
            nulls_first,
        };

        let arrow_array = self.as_arrow()?;

        let result = sort_by(&arrow_array, cmp_float::<f32>, &options, None);

        Self::from_arrow(self.field().clone(), Arc::new(result))
    }
}

impl Float64Array {
    pub fn argsort(&self, descending: bool, nulls_first: bool) -> DaftResult<UInt64Array> {
        let arrow_array = self.as_arrow()?;

        let result =
            indices_sorted_unstable_by(&arrow_array, cmp_float::<f64>, descending, nulls_first);

        UInt64Array::from_arrow(
            Field::new(self.field().name.clone(), DataType::UInt64),
            Arc::new(result),
        )
    }

    pub fn argsort_multikey(
        &self,
        others: &[Series],
        descending: &[bool],
        nulls_first: &[bool],
    ) -> DaftResult<UInt64Array> {
        let arrow_array = self.as_arrow()?;
        let first_desc = *descending.first().unwrap();
        let first_nulls_first = *nulls_first.first().unwrap();

        let others_cmp = build_multi_array_compare(others, &descending[1..], &nulls_first[1..])?;

        let result = if first_desc {
            multi_column_idx_sort(
                arrow_array.nulls(),
                |a: &u64, b: &u64| {
                    let a = a.to_usize();
                    let b = b.to_usize();
                    let l = unsafe { &arrow_array.value_unchecked(a) };
                    let r = unsafe { &arrow_array.value_unchecked(b) };
                    match cmp_float::<f64>(r, l) {
                        std::cmp::Ordering::Equal => others_cmp(a, b),
                        v => v,
                    }
                },
                &others_cmp,
                arrow_array.len(),
                first_nulls_first,
            )
        } else {
            multi_column_idx_sort(
                arrow_array.nulls(),
                |a: &u64, b: &u64| {
                    let a = a.to_usize();
                    let b = b.to_usize();
                    let l = unsafe { &arrow_array.value_unchecked(a) };
                    let r = unsafe { &arrow_array.value_unchecked(b) };
                    match cmp_float::<f64>(l, r) {
                        std::cmp::Ordering::Equal => others_cmp(a, b),
                        v => v,
                    }
                },
                &others_cmp,
                arrow_array.len(),
                first_nulls_first,
            )
        };

        UInt64Array::from_arrow(
            Field::new(self.field().name.clone(), DataType::UInt64),
            Arc::new(result),
        )
    }

    pub fn sort(&self, descending: bool, nulls_first: bool) -> DaftResult<Self> {
        let options = SortOptions {
            descending,
            nulls_first,
        };

        let arrow_array = self.as_arrow()?;

        let result = sort_by(&arrow_array, cmp_float::<f64>, &options, None);

        Self::from_arrow(self.field().clone(), Arc::new(result))
    }
}

impl Decimal128Array {
    pub fn argsort(&self, descending: bool, nulls_first: bool) -> DaftResult<UInt64Array> {
        let arrow_array = self.as_arrow()?;

        let result =
            indices_sorted_unstable_by(&arrow_array, |l, r| l.cmp(r), descending, nulls_first);

        UInt64Array::from_arrow(
            Field::new(self.field().name.clone(), DataType::UInt64),
            Arc::new(result),
        )
    }

    pub fn argsort_multikey(
        &self,
        others: &[Series],
        descending: &[bool],
        nulls_first: &[bool],
    ) -> DaftResult<UInt64Array> {
        let arrow_array = self.as_arrow()?;
        let first_desc = *descending.first().unwrap();
        let first_nulls_first = *nulls_first.first().unwrap();

        let others_cmp = build_multi_array_compare(others, &descending[1..], &nulls_first[1..])?;

        let result = if first_desc {
            multi_column_idx_sort(
                arrow_array.nulls(),
                |a: &u64, b: &u64| {
                    let a = a.to_usize();
                    let b = b.to_usize();
                    let l = unsafe { &arrow_array.value_unchecked(a) };
                    let r = unsafe { &arrow_array.value_unchecked(b) };
                    match ord::total_cmp(r, l) {
                        std::cmp::Ordering::Equal => others_cmp(a, b),
                        v => v,
                    }
                },
                &others_cmp,
                arrow_array.len(),
                first_nulls_first,
            )
        } else {
            multi_column_idx_sort(
                arrow_array.nulls(),
                |a: &u64, b: &u64| {
                    let a = a.to_usize();
                    let b = b.to_usize();
                    let l = unsafe { &arrow_array.value_unchecked(a) };
                    let r = unsafe { &arrow_array.value_unchecked(b) };
                    match ord::total_cmp(l, r) {
                        std::cmp::Ordering::Equal => others_cmp(a, b),
                        v => v,
                    }
                },
                &others_cmp,
                arrow_array.len(),
                first_nulls_first,
            )
        };

        UInt64Array::from_arrow(
            Field::new(self.field().name.clone(), DataType::UInt64),
            Arc::new(result),
        )
    }

    pub fn sort(&self, descending: bool, nulls_first: bool) -> DaftResult<Self> {
        let options = SortOptions {
            descending,
            nulls_first,
        };

        let arrow_array = self.as_arrow()?;

        let result = sort_by(&arrow_array, |l, r| l.cmp(r), &options, None);

        Self::from_arrow(self.field().clone(), Arc::new(result))
    }
}

impl NullArray {
    pub fn argsort(&self, _descending: bool, _nulls_first: bool) -> DaftResult<UInt64Array> {
        UInt64Array::arange(self.name(), 0_i64, self.len() as i64, 1)
    }

    pub fn argsort_multikey(
        &self,
        others: &[Series],
        descending: &[bool],
        nulls_first: &[bool],
    ) -> DaftResult<UInt64Array> {
        let first_nulls_first = *nulls_first.first().unwrap();

        let others_cmp = build_multi_array_compare(others, &descending[1..], &nulls_first[1..])?;

        let result = multi_column_idx_sort(
            self.to_data().nulls(),
            |a: &u64, b: &u64| {
                let a = a.to_usize();
                let b = b.to_usize();
                others_cmp(a, b)
            },
            &others_cmp,
            self.len(),
            first_nulls_first,
        );

        UInt64Array::from_arrow(
            Field::new(self.field().name.clone(), DataType::UInt64),
            Arc::new(result),
        )
    }

    pub fn sort(&self, _descending: bool, _nulls_first: bool) -> DaftResult<Self> {
        Ok(self.clone())
    }
}

impl BooleanArray {
    pub fn argsort(&self, descending: bool, nulls_first: bool) -> DaftResult<UInt64Array> {
        let options = daft_arrow::compute::sort::SortOptions {
            descending,
            nulls_first,
        };

        let result = sort_to_indices::<u64>(self.data(), &options, None)?;

        Ok(UInt64Array::new(
            Field::new(self.name(), DataType::UInt64).into(),
            Box::new(result),
        )
        .unwrap())
    }

    pub fn argsort_multikey(
        &self,
        others: &[Series],
        descending: &[bool],
        nulls_first: &[bool],
    ) -> DaftResult<UInt64Array> {
        let arrow_array = self.as_arrow()?;
        let first_desc = *descending.first().unwrap();
        let first_nulls_first = *nulls_first.first().unwrap();

        let others_cmp = build_multi_array_compare(others, &descending[1..], &nulls_first[1..])?;

        let result = if first_desc {
            multi_column_idx_sort(
                arrow_array.nulls(),
                |a: &u64, b: &u64| {
                    let a = a.to_usize();
                    let b = b.to_usize();
                    let l = unsafe { arrow_array.value_unchecked(a) };
                    let r = unsafe { arrow_array.value_unchecked(b) };
                    match r.cmp(&l) {
                        std::cmp::Ordering::Equal => others_cmp(a, b),
                        v => v,
                    }
                },
                &others_cmp,
                arrow_array.len(),
                first_nulls_first,
            )
        } else {
            multi_column_idx_sort(
                arrow_array.nulls(),
                |a: &u64, b: &u64| {
                    let a = a.to_usize();
                    let b = b.to_usize();
                    let l = unsafe { arrow_array.value_unchecked(a) };
                    let r = unsafe { arrow_array.value_unchecked(b) };
                    match l.cmp(&r) {
                        std::cmp::Ordering::Equal => others_cmp(a, b),
                        v => v,
                    }
                },
                &others_cmp,
                arrow_array.len(),
                first_nulls_first,
            )
        };

        UInt64Array::from_arrow(
            Field::new(self.field().name.clone(), DataType::UInt64),
            Arc::new(result),
        )
    }

    pub fn sort(&self, descending: bool, nulls_first: bool) -> DaftResult<Self> {
        let options = daft_arrow::compute::sort::SortOptions {
            descending,
            nulls_first,
        };

        let result = daft_arrow::compute::sort::sort(self.data(), &options, None)?;

        Self::new(self.field.clone(), result)
    }
}

macro_rules! impl_binary_like_sort {
    ($da:ident) => {
        impl $da {
            pub fn argsort(&self, descending: bool, nulls_first: bool) -> DaftResult<UInt64Array> {
                let options = daft_arrow::compute::sort::SortOptions {
                    descending,
                    nulls_first,
                };

                let result = sort_to_indices::<u64>(self.data(), &options, None)?;

                Ok(UInt64Array::new(
                    Field::new(self.name(), DataType::UInt64).into(),
                    Box::new(result),
                )
                .unwrap())
            }

            pub fn argsort_multikey(
                &self,
                others: &[Series],
                descending: &[bool],
                nulls_first: &[bool],
            ) -> DaftResult<UInt64Array> {
                let first_desc = *descending.first().unwrap();
                let first_nulls_first = *nulls_first.first().unwrap();

                let others_cmp =
                    build_multi_array_compare(others, &descending[1..], &nulls_first[1..])?;

                let arrow_array = self.as_arrow()?;

                let result = if first_desc {
                    multi_column_idx_sort(
                        self.to_data().nulls(),
                        |a: &u64, b: &u64| {
                            let a = a.to_usize();
                            let b = b.to_usize();
                            let l = unsafe { &arrow_array.value_unchecked(a) };
                            let r = unsafe { &arrow_array.value_unchecked(b) };
                            match r.cmp(&l) {
                                std::cmp::Ordering::Equal => others_cmp(a, b),
                                v => v,
                            }
                        },
                        &others_cmp,
                        self.len(),
                        first_nulls_first,
                    )
                } else {
                    multi_column_idx_sort(
                        self.to_data().nulls(),
                        |a: &u64, b: &u64| {
                            let a = a.to_usize();
                            let b = b.to_usize();
                            let l = unsafe { &arrow_array.value_unchecked(a) };
                            let r = unsafe { &arrow_array.value_unchecked(b) };
                            match l.cmp(&r) {
                                std::cmp::Ordering::Equal => others_cmp(a, b),
                                v => v,
                            }
                        },
                        &others_cmp,
                        self.len(),
                        first_nulls_first,
                    )
                };

                UInt64Array::from_arrow(
                    Field::new(self.field().name.clone(), DataType::UInt64),
                    Arc::new(result),
                )
            }

            pub fn sort(&self, descending: bool, nulls_first: bool) -> DaftResult<Self> {
                let options = daft_arrow::compute::sort::SortOptions {
                    descending,
                    nulls_first,
                };

                let result = sort(self.data(), &options, None)?;

                $da::new(self.field.clone(), result)
            }
        }
    };
}

impl_binary_like_sort!(BinaryArray);
impl_binary_like_sort!(Utf8Array);

impl FixedSizeBinaryArray {
    pub fn argsort(&self, _descending: bool, _nulls_first: bool) -> DaftResult<UInt64Array> {
        todo!("impl argsort for FixedSizeBinaryArray")
    }
    pub fn argsort_multikey(
        &self,
        _others: &[Series],
        _descending: &[bool],
        _nulls_first: &[bool],
    ) -> DaftResult<UInt64Array> {
        todo!("impl argsort_multikey for FixedSizeBinaryArray")
    }
    pub fn sort(&self, _descending: bool, _nulls_first: bool) -> DaftResult<Self> {
        todo!("impl sort for FixedSizeBinaryArray")
    }
}

impl FixedSizeListArray {
    pub fn sort(&self, _descending: bool, _nulls_first: bool) -> DaftResult<Self> {
        todo!("impl sort for FixedSizeListArray")
    }
}

impl ListArray {
    pub fn sort(&self, _descending: bool, _nulls_first: bool) -> DaftResult<Self> {
        todo!("impl sort for ListArray")
    }
}

impl MapArray {
    pub fn sort(&self, _descending: bool, _nulls_first: bool) -> DaftResult<Self> {
        todo!("impl sort for MapArray")
    }
}

impl StructArray {
    pub fn sort(&self, _descending: bool, _nulls_first: bool) -> DaftResult<Self> {
        todo!("impl sort for StructArray")
    }
}

impl ExtensionArray {
    pub fn sort(&self, _descending: bool, _nulls_first: bool) -> DaftResult<Self> {
        todo!("impl sort for ExtensionArray")
    }
}

impl IntervalArray {
    pub fn sort(&self, _descending: bool, _nulls_first: bool) -> DaftResult<Self> {
        todo!("impl sort for IntervalArray")
    }
}

#[cfg(feature = "python")]
impl PythonArray {
    pub fn sort(&self, _descending: bool, _nulls_first: bool) -> DaftResult<Self> {
        todo!("impl sort for python array")
    }
}

impl DateArray {
    pub fn sort(&self, descending: bool, nulls_first: bool) -> DaftResult<Self> {
        let new_array = self.physical.sort(descending, nulls_first)?;
        Ok(Self::new(self.field.clone(), new_array))
    }
}

impl TimeArray {
    pub fn sort(&self, descending: bool, nulls_first: bool) -> DaftResult<Self> {
        let new_array = self.physical.sort(descending, nulls_first)?;
        Ok(Self::new(self.field.clone(), new_array))
    }
}

impl DurationArray {
    pub fn sort(&self, descending: bool, nulls_first: bool) -> DaftResult<Self> {
        let new_array = self.physical.sort(descending, nulls_first)?;
        Ok(Self::new(self.field.clone(), new_array))
    }
}

impl TimestampArray {
    pub fn sort(&self, descending: bool, nulls_first: bool) -> DaftResult<Self> {
        let new_array = self.physical.sort(descending, nulls_first)?;
        Ok(Self::new(self.field.clone(), new_array))
    }
}

impl EmbeddingArray {
    pub fn sort(&self, _descending: bool, _nulls_first: bool) -> DaftResult<Self> {
        todo!("impl sort for EmbeddingArray")
    }
}

impl ImageArray {
    pub fn sort(&self, _descending: bool, _nulls_first: bool) -> DaftResult<Self> {
        todo!("impl sort for ImageArray")
    }
}

impl FixedShapeImageArray {
    pub fn sort(&self, _descending: bool, _nulls_first: bool) -> DaftResult<Self> {
        todo!("impl sort for FixedShapeImageArray")
    }
}

impl TensorArray {
    pub fn sort(&self, _descending: bool, _nulls_first: bool) -> DaftResult<Self> {
        todo!("impl sort for TensorArray")
    }
}

impl SparseTensorArray {
    pub fn sort(&self, _descending: bool, _nulls_first: bool) -> DaftResult<Self> {
        todo!("impl sort for SparseTensorArray")
    }
}

impl FixedShapeSparseTensorArray {
    pub fn sort(&self, _descending: bool, _nulls_first: bool) -> DaftResult<Self> {
        todo!("impl sort for FixedShapeSparseTensorArray")
    }
}

impl FixedShapeTensorArray {
    pub fn sort(&self, _descending: bool, _nulls_first: bool) -> DaftResult<Self> {
        todo!("impl sort for FixedShapeTensorArray")
    }
}

impl<T> FileArray<T>
where
    T: DaftMediaType,
{
    pub fn sort(&self, _descending: bool, _nulls_first: bool) -> DaftResult<Self> {
        todo!("impl sort for FileArray")
    }
}
