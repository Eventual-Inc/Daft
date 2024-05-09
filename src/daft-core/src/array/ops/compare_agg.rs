use super::full::FullNull;
use super::{DaftCompareAggable, GroupIndices};
use crate::{
    array::{ListArray, StructArray},
    datatypes::*,
};
use arrow2::array::Array;
use arrow2::array::PrimitiveArray;

use common_error::DaftResult;

fn grouped_cmp_native<T, F>(
    data_array: &DataArray<T>,
    mut op: F,
    groups: &GroupIndices,
) -> DaftResult<DataArray<T>>
where
    T: DaftNumericType,
    F: Fn(T::Native, T::Native) -> T::Native,
{
    let arrow_array = data_array.as_arrow();
    let cmp_per_group = if arrow_array.null_count() > 0 {
        let cmp_values_iter = groups.iter().map(|g| {
            let reduced_val = g
                .iter()
                .map(|i| {
                    let idx = *i as usize;
                    match arrow_array.is_null(idx) {
                        false => Some(unsafe { arrow_array.value_unchecked(idx) }),
                        true => None,
                    }
                })
                .reduce(|l, r| match (l, r) {
                    (None, None) => None,
                    (None, Some(r)) => Some(r),
                    (Some(l), None) => Some(l),
                    (Some(l), Some(r)) => Some(op(l, r)),
                });
            reduced_val.unwrap_or_default()
        });
        Box::new(PrimitiveArray::from_trusted_len_iter(cmp_values_iter))
    } else {
        Box::new(PrimitiveArray::from_trusted_len_values_iter(
            groups.iter().map(|g| {
                g.iter()
                    .map(|i| {
                        let idx = *i as usize;
                        unsafe { arrow_array.value_unchecked(idx) }
                    })
                    .reduce(&mut op)
                    .unwrap()
            }),
        ))
    };
    Ok(DataArray::from((
        data_array.field.name.as_ref(),
        cmp_per_group,
    )))
}

use super::as_arrow::AsArrow;

impl<T> DaftCompareAggable for DataArray<T>
where
    T: DaftNumericType,
    T::Native: PartialOrd,
    <T::Native as arrow2::types::simd::Simd>::Simd: arrow2::compute::aggregate::SimdOrd<T::Native>,
{
    type Output = DaftResult<DataArray<T>>;

    fn min(&self) -> Self::Output {
        let primitive_arr = self.as_arrow();

        let result = arrow2::compute::aggregate::min_primitive(primitive_arr);
        let arrow_array = Box::new(arrow2::array::PrimitiveArray::from([result]));

        DataArray::new(self.field.clone(), arrow_array)
    }

    fn max(&self) -> Self::Output {
        let primitive_arr = self.as_arrow();

        let result = arrow2::compute::aggregate::max_primitive(primitive_arr);
        let arrow_array = Box::new(arrow2::array::PrimitiveArray::from([result]));

        DataArray::new(self.field.clone(), arrow_array)
    }
    fn grouped_min(&self, groups: &GroupIndices) -> Self::Output {
        grouped_cmp_native(
            self,
            |l, r| match l.lt(&r) {
                true => l,
                false => r,
            },
            groups,
        )
    }

    fn grouped_max(&self, groups: &GroupIndices) -> Self::Output {
        grouped_cmp_native(
            self,
            |l, r| match l.gt(&r) {
                true => l,
                false => r,
            },
            groups,
        )
    }
}

fn grouped_cmp_utf8<'a, F>(
    data_array: &'a Utf8Array,
    op: F,
    groups: &GroupIndices,
) -> DaftResult<Utf8Array>
where
    F: Fn(&'a str, &'a str) -> &'a str,
{
    let arrow_array = data_array.as_arrow();
    let cmp_per_group = if arrow_array.null_count() > 0 {
        let cmp_values_iter = groups.iter().map(|g| {
            let reduced_val = g
                .iter()
                .map(|i| {
                    let idx = *i as usize;
                    match arrow_array.is_null(idx) {
                        false => Some(unsafe { arrow_array.value_unchecked(idx) }),
                        true => None,
                    }
                })
                .reduce(|l, r| match (l, r) {
                    (None, None) => None,
                    (None, Some(r)) => Some(r),
                    (Some(l), None) => Some(l),
                    (Some(l), Some(r)) => Some(op(l, r)),
                });
            reduced_val.unwrap_or_default()
        });
        Box::new(arrow2::array::Utf8Array::<i64>::from_trusted_len_iter(
            cmp_values_iter,
        ))
    } else {
        Box::new(
            arrow2::array::Utf8Array::<i64>::from_trusted_len_values_iter(groups.iter().map(|g| {
                g.iter()
                    .map(|i| {
                        let idx = *i as usize;
                        unsafe { arrow_array.value_unchecked(idx) }
                    })
                    .reduce(|l, r| op(l, r))
                    .unwrap()
            })),
        )
    };
    Ok(DataArray::from((
        data_array.field.name.as_ref(),
        cmp_per_group,
    )))
}

impl DaftCompareAggable for DataArray<Utf8Type> {
    type Output = DaftResult<DataArray<Utf8Type>>;
    fn min(&self) -> Self::Output {
        let arrow_array: &arrow2::array::Utf8Array<i64> = self.as_arrow();

        let result = arrow2::compute::aggregate::min_string(arrow_array);
        let res_arrow_array = arrow2::array::Utf8Array::<i64>::from([result]);

        DataArray::new(self.field.clone(), Box::new(res_arrow_array))
    }
    fn max(&self) -> Self::Output {
        let arrow_array: &arrow2::array::Utf8Array<i64> = self.as_arrow();

        let result = arrow2::compute::aggregate::max_string(arrow_array);
        let res_arrow_array = arrow2::array::Utf8Array::<i64>::from([result]);

        DataArray::new(self.field.clone(), Box::new(res_arrow_array))
    }

    fn grouped_min(&self, groups: &GroupIndices) -> Self::Output {
        grouped_cmp_utf8(self, |l, r| l.min(r), groups)
    }

    fn grouped_max(&self, groups: &GroupIndices) -> Self::Output {
        grouped_cmp_utf8(self, |l, r| l.max(r), groups)
    }
}

fn grouped_cmp_binary<'a, F>(
    data_array: &'a BinaryArray,
    op: F,
    groups: &GroupIndices,
) -> DaftResult<BinaryArray>
where
    F: Fn(&'a [u8], &'a [u8]) -> &'a [u8],
{
    let arrow_array = data_array.as_arrow();
    let cmp_per_group = if arrow_array.null_count() > 0 {
        let cmp_values_iter = groups.iter().map(|g| {
            let reduced_val = g
                .iter()
                .map(|i| {
                    let idx = *i as usize;
                    match arrow_array.is_null(idx) {
                        false => Some(unsafe { arrow_array.value_unchecked(idx) }),
                        true => None,
                    }
                })
                .reduce(|l, r| match (l, r) {
                    (None, None) => None,
                    (None, Some(r)) => Some(r),
                    (Some(l), None) => Some(l),
                    (Some(l), Some(r)) => Some(op(l, r)),
                });
            reduced_val.unwrap_or_default()
        });
        Box::new(arrow2::array::BinaryArray::<i64>::from_trusted_len_iter(
            cmp_values_iter,
        ))
    } else {
        Box::new(
            arrow2::array::BinaryArray::<i64>::from_trusted_len_values_iter(groups.iter().map(
                |g| {
                    g.iter()
                        .map(|i| {
                            let idx = *i as usize;
                            unsafe { arrow_array.value_unchecked(idx) }
                        })
                        .reduce(|l, r| op(l, r))
                        .unwrap()
                },
            )),
        )
    };
    Ok(DataArray::from((
        data_array.field.name.as_ref(),
        cmp_per_group,
    )))
}

impl DaftCompareAggable for DataArray<BinaryType> {
    type Output = DaftResult<DataArray<BinaryType>>;
    fn min(&self) -> Self::Output {
        let arrow_array: &arrow2::array::BinaryArray<i64> = self.as_arrow();

        let result = arrow2::compute::aggregate::min_binary(arrow_array);
        let res_arrow_array = arrow2::array::BinaryArray::<i64>::from([result]);

        DataArray::new(self.field.clone(), Box::new(res_arrow_array))
    }
    fn max(&self) -> Self::Output {
        let arrow_array: &arrow2::array::BinaryArray<i64> = self.as_arrow();

        let result = arrow2::compute::aggregate::max_binary(arrow_array);
        let res_arrow_array = arrow2::array::BinaryArray::<i64>::from([result]);

        DataArray::new(self.field.clone(), Box::new(res_arrow_array))
    }

    fn grouped_min(&self, groups: &GroupIndices) -> Self::Output {
        grouped_cmp_binary(self, |l, r| l.min(r), groups)
    }

    fn grouped_max(&self, groups: &GroupIndices) -> Self::Output {
        grouped_cmp_binary(self, |l, r| l.max(r), groups)
    }
}

fn cmp_fixed_size_binary<'a, F>(
    data_array: &'a FixedSizeBinaryArray,
    op: F,
) -> DaftResult<FixedSizeBinaryArray>
where
    F: Fn(&'a [u8], &'a [u8]) -> &'a [u8],
{
    let arrow_array = data_array.as_arrow();
    if arrow_array.null_count() == arrow_array.len() {
        Ok(FixedSizeBinaryArray::full_null(
            data_array.name(),
            &DataType::FixedSizeBinary(arrow_array.size()),
            1,
        ))
    } else if arrow_array.validity().is_some() {
        let res = arrow_array
            .iter()
            .reduce(|v1, v2| match (v1, v2) {
                (None, v2) => v2,
                (v1, None) => v1,
                (Some(v1), Some(v2)) => Some(op(v1, v2)),
            })
            .unwrap_or(None);
        Ok(FixedSizeBinaryArray::from_iter(
            data_array.name(),
            std::iter::once(res),
            arrow_array.size(),
        ))
    } else {
        let res = arrow_array.values_iter().reduce(|v1, v2| op(v1, v2));
        Ok(FixedSizeBinaryArray::from_iter(
            data_array.name(),
            std::iter::once(res),
            arrow_array.size(),
        ))
    }
}

fn grouped_cmp_fixed_size_binary<'a, F>(
    data_array: &'a FixedSizeBinaryArray,
    op: F,
    groups: &GroupIndices,
) -> DaftResult<FixedSizeBinaryArray>
where
    F: Fn(&'a [u8], &'a [u8]) -> &'a [u8],
{
    let arrow_array = data_array.as_arrow();
    let cmp_per_group = if arrow_array.null_count() > 0 {
        let cmp_values_iter = groups.iter().map(|g| {
            let reduced_val = g
                .iter()
                .map(|i| {
                    let idx = *i as usize;
                    match arrow_array.is_null(idx) {
                        false => Some(unsafe { arrow_array.value_unchecked(idx) }),
                        true => None,
                    }
                })
                .reduce(|l, r| match (l, r) {
                    (None, None) => None,
                    (None, Some(r)) => Some(r),
                    (Some(l), None) => Some(l),
                    (Some(l), Some(r)) => Some(op(l, r)),
                });
            reduced_val.unwrap_or_default()
        });
        Box::new(arrow2::array::FixedSizeBinaryArray::from_iter(
            cmp_values_iter,
            arrow_array.size(),
        ))
    } else {
        Box::new(arrow2::array::FixedSizeBinaryArray::from_iter(
            groups.iter().map(|g| {
                g.iter()
                    .map(|i| {
                        let idx = *i as usize;
                        unsafe { arrow_array.value_unchecked(idx) }
                    })
                    .reduce(|l, r| op(l, r))
            }),
            arrow_array.size(),
        ))
    };
    Ok(DataArray::from((
        data_array.field.name.as_ref(),
        cmp_per_group,
    )))
}

impl DaftCompareAggable for DataArray<FixedSizeBinaryType> {
    type Output = DaftResult<DataArray<FixedSizeBinaryType>>;
    fn min(&self) -> Self::Output {
        cmp_fixed_size_binary(self, |l, r| l.min(r))
    }
    fn max(&self) -> Self::Output {
        cmp_fixed_size_binary(self, |l, r| l.max(r))
    }

    fn grouped_min(&self, groups: &GroupIndices) -> Self::Output {
        grouped_cmp_fixed_size_binary(self, |l, r| l.min(r), groups)
    }

    fn grouped_max(&self, groups: &GroupIndices) -> Self::Output {
        grouped_cmp_fixed_size_binary(self, |l, r| l.max(r), groups)
    }
}

fn grouped_cmp_bool(
    data_array: &BooleanArray,
    val_to_find: bool,
    groups: &GroupIndices,
) -> DaftResult<BooleanArray> {
    let arrow_array = data_array.as_arrow();
    let cmp_per_group = if arrow_array.null_count() > 0 {
        let cmp_values_iter = groups.iter().map(|g| {
            let reduced_val = g
                .iter()
                .map(|i| {
                    let idx = *i as usize;
                    match arrow_array.is_null(idx) {
                        false => Some(unsafe { arrow_array.value_unchecked(idx) }),
                        true => None,
                    }
                })
                .reduce(|l, r| match (l, r) {
                    (None, None) => None,
                    (None, Some(r)) => Some(r),
                    (Some(l), None) => Some(l),
                    (Some(l), Some(r)) => Some((l | r) ^ val_to_find),
                });
            reduced_val.unwrap_or_default()
        });
        Box::new(arrow2::array::BooleanArray::from_trusted_len_iter(
            cmp_values_iter,
        ))
    } else {
        Box::new(arrow2::array::BooleanArray::from_trusted_len_values_iter(
            groups.iter().map(|g| {
                let reduced_val = g
                    .iter()
                    .map(|i| {
                        let idx = *i as usize;
                        unsafe { arrow_array.value_unchecked(idx) }
                    })
                    .find(|v| *v == val_to_find);
                match reduced_val {
                    None => !val_to_find,
                    Some(v) => v,
                }
            }),
        ))
    };
    Ok(DataArray::from((
        data_array.field.name.as_ref(),
        cmp_per_group,
    )))
}

impl DaftCompareAggable for DataArray<BooleanType> {
    type Output = DaftResult<DataArray<BooleanType>>;
    fn min(&self) -> Self::Output {
        let arrow_array: &arrow2::array::BooleanArray = self.as_arrow();

        let result = arrow2::compute::aggregate::min_boolean(arrow_array);
        let res_arrow_array = arrow2::array::BooleanArray::from([result]);

        DataArray::new(self.field.clone(), Box::new(res_arrow_array))
    }
    fn max(&self) -> Self::Output {
        let arrow_array: &arrow2::array::BooleanArray = self.as_arrow();

        let result = arrow2::compute::aggregate::max_boolean(arrow_array);
        let res_arrow_array = arrow2::array::BooleanArray::from([result]);

        DataArray::new(self.field.clone(), Box::new(res_arrow_array))
    }

    fn grouped_min(&self, groups: &GroupIndices) -> Self::Output {
        grouped_cmp_bool(self, false, groups)
    }

    fn grouped_max(&self, groups: &GroupIndices) -> Self::Output {
        grouped_cmp_bool(self, true, groups)
    }
}

impl DaftCompareAggable for DataArray<NullType> {
    type Output = DaftResult<DataArray<NullType>>;

    fn min(&self) -> Self::Output {
        let res_arrow_array = arrow2::array::NullArray::new(arrow2::datatypes::DataType::Null, 1);
        DataArray::new(self.field.clone(), Box::new(res_arrow_array))
    }

    fn max(&self) -> Self::Output {
        // Min and max are the same for NullArray.
        Self::min(self)
    }

    fn grouped_min(&self, groups: &super::GroupIndices) -> Self::Output {
        Ok(DataArray::full_null(
            self.name(),
            self.data_type(),
            groups.len(),
        ))
    }

    fn grouped_max(&self, groups: &super::GroupIndices) -> Self::Output {
        Ok(DataArray::full_null(
            self.name(),
            self.data_type(),
            groups.len(),
        ))
    }
}

macro_rules! impl_todo_daft_comparable {
    ($da:ident) => {
        impl DaftCompareAggable for $da {
            type Output = DaftResult<$da>;
            fn min(&self) -> Self::Output {
                todo!(
                    "TODO need to impl DaftCompareAggable for {}",
                    self.data_type()
                )
            }

            fn max(&self) -> Self::Output {
                todo!(
                    "TODO need to impl DaftCompareAggable for {}",
                    self.data_type()
                )
            }

            fn grouped_min(&self, _groups: &super::GroupIndices) -> Self::Output {
                todo!(
                    "TODO need to impl DaftCompareAggable for {}",
                    self.data_type()
                )
            }

            fn grouped_max(&self, _groups: &super::GroupIndices) -> Self::Output {
                todo!(
                    "TODO need to impl DaftCompareAggable for {}",
                    self.data_type()
                )
            }
        }
    };
}

impl_todo_daft_comparable!(StructArray);
impl_todo_daft_comparable!(FixedSizeListArray);
impl_todo_daft_comparable!(ListArray);
impl_todo_daft_comparable!(ExtensionArray);

#[cfg(feature = "python")]
impl_todo_daft_comparable!(PythonArray);
