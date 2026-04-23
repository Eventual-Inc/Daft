use std::{cmp::Ordering, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, ArrowPrimitiveType},
    compute::{SortOptions, sort_to_indices},
    datatypes::ArrowNativeType,
};
use common_error::{DaftError, DaftResult};

use super::as_arrow::AsArrow;
#[cfg(feature = "python")]
use crate::prelude::PythonArray;
use crate::{
    array::{
        DataArray, FixedSizeListArray, ListArray, StructArray, UnionArray, UuidArray,
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
    kernels::search_sorted::{cmp_float, make_daft_comparator},
    prelude::UInt64Array,
    series::Series,
};
/// Compare the values at two arbitrary indices in two arrays.
pub type DynComparator = Box<dyn Fn(usize, usize) -> Ordering + Send + Sync>;

#[inline(never)]
fn argsort_indices_array(name: &str, result: ArrayRef) -> DaftResult<UInt64Array> {
    UInt64Array::from_arrow(Field::new(name, DataType::UInt64), result)
}

#[inline(never)]
fn arrow_argsort(
    array: &dyn Array,
    name: &str,
    descending: bool,
    nulls_first: bool,
) -> DaftResult<UInt64Array> {
    if array.len() > u32::MAX as usize {
        return Err(DaftError::ComputeError(format!(
            "Cannot argsort array with {} elements (max {})",
            array.len(),
            u32::MAX
        )));
    }

    let options = SortOptions {
        descending,
        nulls_first,
    };

    let result: ArrayRef = Arc::new(sort_to_indices(array, Some(options), None)?);
    let result = arrow::compute::cast(&result, &arrow::datatypes::DataType::UInt64)?;
    argsort_indices_array(name, result)
}

#[inline(never)]
fn argsort_multikey_impl(
    nulls: Option<&arrow::buffer::NullBuffer>,
    len: usize,
    name: &str,
    others: &[Series],
    descending: &[bool],
    nulls_first: &[bool],
    cmp_at: DynComparator,
) -> DaftResult<UInt64Array> {
    let first_desc = *descending.first().unwrap();
    let first_nulls_first = *nulls_first.first().unwrap();
    let others_cmp = build_multi_array_compare(others, &descending[1..], &nulls_first[1..])?;

    let result = multi_column_idx_sort(
        nulls,
        |a: &u64, b: &u64| {
            let a = a.to_usize().unwrap();
            let b = b.to_usize().unwrap();
            let ordering = cmp_at(a, b);
            let ordering = if first_desc {
                ordering.reverse()
            } else {
                ordering
            };
            match ordering {
                Ordering::Equal => others_cmp(a, b),
                other => other,
            }
        },
        &others_cmp,
        len,
        first_nulls_first,
    );

    argsort_indices_array(name, Arc::new(result))
}

#[inline(never)]
fn primitive_argsort_multikey<T, F>(
    arrow_array: &arrow::array::PrimitiveArray<T>,
    name: &str,
    others: &[Series],
    descending: &[bool],
    nulls_first: &[bool],
    cmp: F,
) -> DaftResult<UInt64Array>
where
    T: ArrowPrimitiveType,
    F: Fn(&T::Native, &T::Native) -> Ordering + Send + Sync + 'static,
{
    let cmp_at: DynComparator = Box::new({
        let arrow_array = arrow_array.clone();
        move |a: usize, b: usize| {
            // SAFETY: `a` and `b` come from the sort index domain, which is bounded by
            // `arrow_array.len()` in `argsort_multikey_impl` / `multi_column_idx_sort`.
            let left = unsafe { &arrow_array.value_unchecked(a) };
            let right = unsafe { &arrow_array.value_unchecked(b) };
            cmp(left, right)
        }
    });

    argsort_multikey_impl(
        arrow_array.nulls(),
        arrow_array.len(),
        name,
        others,
        descending,
        nulls_first,
        cmp_at,
    )
}

#[inline(never)]
fn bool_argsort_multikey(
    array: &BooleanArray,
    others: &[Series],
    descending: &[bool],
    nulls_first: &[bool],
) -> DaftResult<UInt64Array> {
    let arrow_array = array.as_arrow()?;
    let cmp_at: DynComparator = Box::new({
        let arrow_array = arrow_array.clone();
        move |a: usize, b: usize| {
            // SAFETY: `a` and `b` come from the sort index domain, which is bounded by
            // `arrow_array.len()` in `argsort_multikey_impl` / `multi_column_idx_sort`.
            let left = unsafe { arrow_array.value_unchecked(a) };
            let right = unsafe { arrow_array.value_unchecked(b) };
            left.cmp(&right)
        }
    });

    argsort_multikey_impl(
        arrow_array.nulls(),
        arrow_array.len(),
        array.name(),
        others,
        descending,
        nulls_first,
        cmp_at,
    )
}

pub fn build_multi_array_compare(
    arrays: &[Series],
    descending: &[bool],
    nulls_first: &[bool],
) -> DaftResult<DynComparator> {
    build_multi_array_bicompare(arrays, arrays, descending, nulls_first)
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
        let l_arrow = l.to_arrow()?;
        let r_arrow = r.to_arrow()?;
        cmp_list.push(make_daft_comparator(
            l_arrow.as_ref(),
            r_arrow.as_ref(),
            SortOptions::new(*desc, *nf),
        )?);
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
            indices_sorted_unstable_by(arrow_array, |l, r| l.cmp(r), descending, nulls_first);

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
        primitive_argsort_multikey(
            arrow_array,
            self.name(),
            others,
            descending,
            nulls_first,
            |l, r| l.cmp(r),
        )
    }

    pub fn sort(&self, descending: bool, nulls_first: bool) -> DaftResult<Self> {
        let options = SortOptions {
            descending,
            nulls_first,
        };

        let arrow_array = self.as_arrow()?;

        let result = sort_by(arrow_array, |l, r| l.cmp(r), &options, None);

        Self::from_arrow(self.field().clone(), Arc::new(result))
    }
}

impl Float32Array {
    pub fn argsort(&self, descending: bool, nulls_first: bool) -> DaftResult<UInt64Array> {
        let arrow_array = self.as_arrow()?;

        let result =
            indices_sorted_unstable_by(arrow_array, cmp_float::<f32>, descending, nulls_first);

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
        primitive_argsort_multikey(
            arrow_array,
            self.name(),
            others,
            descending,
            nulls_first,
            cmp_float::<f32>,
        )
    }

    pub fn sort(&self, descending: bool, nulls_first: bool) -> DaftResult<Self> {
        let options = SortOptions {
            descending,
            nulls_first,
        };

        let arrow_array = self.as_arrow()?;

        let result = sort_by(arrow_array, cmp_float::<f32>, &options, None);

        Self::from_arrow(self.field().clone(), Arc::new(result))
    }
}

impl Float64Array {
    pub fn argsort(&self, descending: bool, nulls_first: bool) -> DaftResult<UInt64Array> {
        let arrow_array = self.as_arrow()?;

        let result =
            indices_sorted_unstable_by(arrow_array, cmp_float::<f64>, descending, nulls_first);

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
        primitive_argsort_multikey(
            arrow_array,
            self.name(),
            others,
            descending,
            nulls_first,
            cmp_float::<f64>,
        )
    }

    pub fn sort(&self, descending: bool, nulls_first: bool) -> DaftResult<Self> {
        let options = SortOptions {
            descending,
            nulls_first,
        };

        let arrow_array = self.as_arrow()?;

        let result = sort_by(arrow_array, cmp_float::<f64>, &options, None);

        Self::from_arrow(self.field().clone(), Arc::new(result))
    }
}

impl Decimal128Array {
    pub fn argsort(&self, descending: bool, nulls_first: bool) -> DaftResult<UInt64Array> {
        let arrow_array = self.as_arrow()?;

        let result =
            indices_sorted_unstable_by(arrow_array, |l, r| l.cmp(r), descending, nulls_first);

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
        primitive_argsort_multikey(
            arrow_array,
            self.name(),
            others,
            descending,
            nulls_first,
            |l, r| l.cmp(r),
        )
    }

    pub fn sort(&self, descending: bool, nulls_first: bool) -> DaftResult<Self> {
        let options = SortOptions {
            descending,
            nulls_first,
        };

        let arrow_array = self.as_arrow()?;

        let result = sort_by(arrow_array, |l, r| l.cmp(r), &options, None);

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
            self.nulls(),
            |a: &u64, b: &u64| {
                let a = a.to_usize().unwrap();
                let b = b.to_usize().unwrap();
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
        arrow_argsort(
            self.to_arrow().as_ref(),
            self.name(),
            descending,
            nulls_first,
        )
    }

    pub fn argsort_multikey(
        &self,
        others: &[Series],
        descending: &[bool],
        nulls_first: &[bool],
    ) -> DaftResult<UInt64Array> {
        bool_argsort_multikey(self, others, descending, nulls_first)
    }

    pub fn sort(&self, descending: bool, nulls_first: bool) -> DaftResult<Self> {
        let options = arrow::compute::SortOptions {
            descending,
            nulls_first,
        };

        let result = arrow::compute::sort(self.to_arrow().as_ref(), Some(options))?;

        Self::from_arrow(self.field.clone(), result)
    }
}

macro_rules! impl_binary_like_sort {
    ($da:ident) => {
        impl $da {
            pub fn argsort(&self, descending: bool, nulls_first: bool) -> DaftResult<UInt64Array> {
                arrow_argsort(
                    self.to_arrow().as_ref(),
                    self.name(),
                    descending,
                    nulls_first,
                )
            }

            pub fn argsort_multikey(
                &self,
                others: &[Series],
                descending: &[bool],
                nulls_first: &[bool],
            ) -> DaftResult<UInt64Array> {
                let arrow_array = self.as_arrow()?;
                let cmp_at: DynComparator = Box::new({
                    let arrow_array = arrow_array.clone();
                    move |a: usize, b: usize| {
                        // SAFETY: `a` and `b` come from the sort index domain, which is bounded by
                        // `arrow_array.len()` in `argsort_multikey_impl` / `multi_column_idx_sort`.
                        let left = unsafe { &arrow_array.value_unchecked(a) };
                        let right = unsafe { &arrow_array.value_unchecked(b) };
                        left.cmp(right)
                    }
                });

                argsort_multikey_impl(
                    self.to_data().nulls(),
                    self.len(),
                    self.name(),
                    others,
                    descending,
                    nulls_first,
                    cmp_at,
                )
            }

            pub fn sort(&self, descending: bool, nulls_first: bool) -> DaftResult<Self> {
                let options = arrow::compute::SortOptions {
                    descending,
                    nulls_first,
                };

                let result = arrow::compute::sort(self.to_arrow().as_ref(), Some(options))?;

                $da::from_arrow(self.field.clone(), result)
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

impl UnionArray {
    pub fn sort(&self, _descending: bool, _nulls_first: bool) -> DaftResult<Self> {
        todo!("impl sort for UnionArray")
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

impl UuidArray {
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
