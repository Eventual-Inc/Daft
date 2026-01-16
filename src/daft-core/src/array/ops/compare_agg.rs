use std::sync::Arc;

use arrow::array::{ArrowPrimitiveType, LargeStringArray};
use common_error::DaftResult;

use super::{DaftCompareAggable, GroupIndices, full::FullNull};
#[cfg(feature = "python")]
use crate::prelude::PythonArray;
use crate::{
    array::{ListArray, StructArray},
    datatypes::*,
};

fn grouped_cmp_native<T, F>(
    array: &DataArray<T>,
    mut op: F,
    groups: &GroupIndices,
) -> DaftResult<DataArray<T>>
where
    T: DaftPrimitiveType,
    F: Fn(T::Native, T::Native) -> T::Native,
{
    let cmp_per_group = if array.null_count() > 0 {
        let cmp_values_iter = groups.iter().map(|g| {
            let reduced_val =
                g.iter()
                    .map(|i| array.get(*i as usize))
                    .reduce(|l, r| match (l, r) {
                        (None, None) => None,
                        (None, Some(r)) => Some(r),
                        (Some(l), None) => Some(l),
                        (Some(l), Some(r)) => Some(op(l, r)),
                    });
            reduced_val.unwrap_or_default()
        });
        DataArray::<T>::from_iter(array.field.clone(), cmp_values_iter)
    } else {
        DataArray::<T>::from_values_iter(
            array.field.clone(),
            groups.iter().map(|g| {
                g.iter()
                    .map(|i| array.get(*i as usize).unwrap())
                    .reduce(&mut op)
                    .unwrap()
            }),
        )
    };
    Ok(cmp_per_group)
}

use super::as_arrow::AsArrow;

impl<T> DaftCompareAggable for DataArray<T>
where
    T: DaftPrimitiveType,
    T::Native: PartialOrd,
    <<T::Native as NumericNative>::ARROWTYPE as ArrowPrimitiveType>::Native: Into<T::Native>,
{
    type Output = DaftResult<Self>;

    fn min(&self) -> Self::Output {
        let primitive_arr = self.as_arrow()?;

        let result = arrow::compute::min(&primitive_arr);
        Ok(Self::from_iter(
            self.field.clone(),
            std::iter::once(result.map(Into::into)),
        ))
    }

    fn max(&self) -> Self::Output {
        let primitive_arr = self.as_arrow()?;

        let result = arrow::compute::max(&primitive_arr);
        Ok(Self::from_iter(
            self.field.clone(),
            std::iter::once(result.map(Into::into)),
        ))
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
    if data_array.null_count() > 0 {
        let cmp_values_iter = groups.iter().map(|g| {
            let reduced_val = g
                .iter()
                .map(|i| data_array.get(*i as usize))
                .reduce(|l, r| match (l, r) {
                    (None, None) => None,
                    (None, Some(r)) => Some(r),
                    (Some(l), None) => Some(l),
                    (Some(l), Some(r)) => Some(op(l, r)),
                });
            reduced_val.unwrap_or_default()
        });
        Ok(Utf8Array::from_iter(data_array.name(), cmp_values_iter))
    } else {
        let arrow_result = LargeStringArray::from_iter_values(groups.iter().map(|g| {
            g.iter()
                .map(|i| data_array.get(*i as usize).unwrap())
                .reduce(|l, r| op(l, r))
                .unwrap()
        }));
        Utf8Array::from_arrow(
            Field::new(data_array.name(), DataType::Utf8),
            Arc::new(arrow_result),
        )
    }
}

impl DaftCompareAggable for DataArray<Utf8Type> {
    type Output = DaftResult<Self>;
    fn min(&self) -> Self::Output {
        let arrow_array = self.as_arrow()?;

        let result = arrow::compute::min_string(&arrow_array);
        Ok(Self::from_iter(self.name(), std::iter::once(result)))
    }
    fn max(&self) -> Self::Output {
        let arrow_array = self.as_arrow()?;

        let result = arrow::compute::max_string(&arrow_array);
        Ok(Self::from_iter(self.name(), std::iter::once(result)))
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
    if data_array.null_count() > 0 {
        let cmp_values_iter = groups.iter().map(|g| {
            let reduced_val = g
                .iter()
                .map(|i| data_array.get(*i as usize))
                .reduce(|l, r| match (l, r) {
                    (None, None) => None,
                    (None, Some(r)) => Some(r),
                    (Some(l), None) => Some(l),
                    (Some(l), Some(r)) => Some(op(l, r)),
                });
            reduced_val.unwrap_or_default()
        });
        Ok(BinaryArray::from_iter(data_array.name(), cmp_values_iter))
    } else {
        Ok(BinaryArray::from_values(
            data_array.name(),
            groups.iter().map(|g| {
                g.iter()
                    .map(|i| data_array.get(*i as usize).unwrap())
                    .reduce(|l, r| op(l, r))
                    .unwrap()
            }),
        ))
    }
}

impl DaftCompareAggable for DataArray<BinaryType> {
    type Output = DaftResult<Self>;
    fn min(&self) -> Self::Output {
        let arrow_array = self.as_arrow()?;

        let result = arrow::compute::min_binary(&arrow_array);
        Ok(Self::from_iter(self.name(), std::iter::once(result)))
    }
    fn max(&self) -> Self::Output {
        let arrow_array = self.as_arrow()?;

        let result = arrow::compute::max_binary(&arrow_array);
        Ok(Self::from_iter(self.name(), std::iter::once(result)))
    }

    fn grouped_min(&self, groups: &GroupIndices) -> Self::Output {
        grouped_cmp_binary(self, |l, r| l.min(r), groups)
    }

    fn grouped_max(&self, groups: &GroupIndices) -> Self::Output {
        grouped_cmp_binary(self, |l, r| l.max(r), groups)
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
    let DataType::FixedSizeBinary(size) = data_array.data_type() else {
        unreachable!("FixedSizeBinaryArray must have DataType::FixedSizeBinary(..)");
    };

    if data_array.null_count() > 0 {
        let cmp_values_iter = groups.iter().map(|g| {
            let reduced_val = g
                .iter()
                .map(|i| data_array.get(*i as usize))
                .reduce(|l, r| match (l, r) {
                    (None, None) => None,
                    (None, Some(r)) => Some(r),
                    (Some(l), None) => Some(l),
                    (Some(l), Some(r)) => Some(op(l, r)),
                });
            reduced_val.unwrap_or_default()
        });
        Ok(FixedSizeBinaryArray::from_iter(
            data_array.name(),
            cmp_values_iter,
            *size,
        ))
    } else {
        let arrow_result =
            arrow::array::FixedSizeBinaryArray::try_from_iter(groups.iter().map(|g| {
                g.iter()
                    .map(|i| data_array.get(*i as usize).unwrap())
                    .reduce(|l, r| op(l, r))
                    .unwrap()
            }))?;
        FixedSizeBinaryArray::from_arrow(data_array.field.clone(), Arc::new(arrow_result))
    }
}

impl DaftCompareAggable for DataArray<FixedSizeBinaryType> {
    type Output = DaftResult<Self>;
    fn min(&self) -> Self::Output {
        let arrow_array = self.as_arrow()?;

        let DataType::FixedSizeBinary(size) = self.data_type() else {
            unreachable!("FixedSizeBinaryArray must have DataType::FixedSizeBinary(..)");
        };

        let result = arrow::compute::min_fixed_size_binary(&arrow_array);
        Ok(Self::from_iter(self.name(), std::iter::once(result), *size))
    }
    fn max(&self) -> Self::Output {
        let arrow_array = self.as_arrow()?;

        let DataType::FixedSizeBinary(size) = self.data_type() else {
            unreachable!("FixedSizeBinaryArray must have DataType::FixedSizeBinary(..)");
        };

        let result = arrow::compute::max_fixed_size_binary(&arrow_array);
        Ok(Self::from_iter(self.name(), std::iter::once(result), *size))
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
    if data_array.null_count() > 0 {
        let cmp_values_iter = groups.iter().map(|g| {
            let reduced_val = g
                .iter()
                .map(|i| data_array.get(*i as usize))
                .reduce(|l, r| match (l, r) {
                    (None, None) => None,
                    (None, Some(r)) => Some(r),
                    (Some(l), None) => Some(l),
                    (Some(l), Some(r)) => Some((l | r) ^ val_to_find),
                });
            reduced_val.unwrap_or_default()
        });
        Ok(BooleanArray::from_iter(data_array.name(), cmp_values_iter))
    } else {
        Ok(BooleanArray::from_values(
            data_array.name(),
            groups.iter().map(|g| {
                g.iter()
                    .map(|i| data_array.get(*i as usize).unwrap())
                    .reduce(|l, r| (l | r) ^ val_to_find)
                    .unwrap()
            }),
        ))
    }
}

impl DaftCompareAggable for DataArray<BooleanType> {
    type Output = DaftResult<Self>;
    fn min(&self) -> Self::Output {
        let arrow_array = self.as_arrow()?;

        let result = arrow::compute::min_boolean(&arrow_array);
        Ok(Self::from_iter(self.name(), std::iter::once(result)))
    }
    fn max(&self) -> Self::Output {
        let arrow_array = self.as_arrow()?;

        let result = arrow::compute::max_boolean(&arrow_array);
        Ok(Self::from_iter(self.name(), std::iter::once(result)))
    }

    fn grouped_min(&self, groups: &GroupIndices) -> Self::Output {
        grouped_cmp_bool(self, false, groups)
    }

    fn grouped_max(&self, groups: &GroupIndices) -> Self::Output {
        grouped_cmp_bool(self, true, groups)
    }
}

impl DaftCompareAggable for DataArray<NullType> {
    type Output = DaftResult<Self>;

    fn min(&self) -> Self::Output {
        Ok(Self::full_null(self.name(), self.data_type(), 1))
    }

    fn max(&self) -> Self::Output {
        Ok(Self::full_null(self.name(), self.data_type(), 1))
    }

    fn grouped_min(&self, groups: &super::GroupIndices) -> Self::Output {
        Ok(Self::full_null(self.name(), self.data_type(), groups.len()))
    }

    fn grouped_max(&self, groups: &super::GroupIndices) -> Self::Output {
        Ok(Self::full_null(self.name(), self.data_type(), groups.len()))
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
impl_todo_daft_comparable!(IntervalArray);

#[cfg(feature = "python")]
impl_todo_daft_comparable!(PythonArray);
