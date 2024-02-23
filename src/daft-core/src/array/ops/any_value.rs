use arrow2::{array::PrimitiveArray, bitmap::Bitmap};
use common_error::DaftResult;

use crate::{
    array::{DataArray, FixedSizeListArray, ListArray, StructArray},
    datatypes::*,
};

use super::{full::FullNull, DaftAnyValueAggable, GroupIndices};

fn get_any_grouped_idx(
    groups: &GroupIndices,
    ignore_nulls: bool,
    validity: Option<&Bitmap>,
) -> UInt64Array {
    let group_indices = if ignore_nulls && let Some(validity) = validity {
        Box::new(PrimitiveArray::from_trusted_len_iter(groups.iter().map(|g| {
            for i in g {
                if validity.get_bit(*i as usize) {
                    return Some(*i)
                }
            }
            None
        })))
    } else {
        Box::new(PrimitiveArray::from_trusted_len_iter(groups.iter().map(|g| g.first().cloned())))
    };

    DataArray::from(("", group_indices))
}

impl<T> DaftAnyValueAggable for DataArray<T>
where
    T: DaftNumericType,
{
    type Output = DaftResult<DataArray<T>>;

    fn any_value(&self, ignore_nulls: bool) -> Self::Output {
        if ignore_nulls && let Some(validity) = self.validity() {
            for i in 0..self.len() {
                if validity.get_bit(i) {
                    return self.slice(i, i+1);
                }
            }

            Ok(DataArray::full_null(
                self.name(),
                self.data_type(),
                1
            ))
        } else {
            self.slice(0, 1)
        }
    }

    fn grouped_any_value(&self, groups: &GroupIndices, ignore_nulls: bool) -> Self::Output {
        self.take(&get_any_grouped_idx(groups, ignore_nulls, self.validity()))
    }
}

macro_rules! impl_daft_any_value {
    ($arr:ident) => {

        // there is a strange bug in the formatter which adds extra indentations to any_value
        #[rustfmt::skip]
        impl DaftAnyValueAggable for $arr {
            type Output = DaftResult<$arr>;

            fn any_value(&self, ignore_nulls: bool) -> Self::Output {
                if ignore_nulls && let Some(validity) = self.validity() {
                    for i in 0..self.len() {
                        if validity.get_bit(i) {
                            return self.slice(i, i+1);
                        }
                    }

                    Ok($arr::full_null(
                        self.name(),
                        self.data_type(),
                        1
                    ))
                } else {
                    self.slice(0, 1)
                }
            }

            fn grouped_any_value(&self, groups: &GroupIndices, ignore_nulls: bool) -> Self::Output {
                self.take(&get_any_grouped_idx(groups, ignore_nulls, self.validity()))
            }
        }
    };
}

impl_daft_any_value!(Utf8Array);
impl_daft_any_value!(BooleanArray);
impl_daft_any_value!(BinaryArray);
impl_daft_any_value!(NullArray);
impl_daft_any_value!(ExtensionArray);
impl_daft_any_value!(FixedSizeListArray);
impl_daft_any_value!(ListArray);
impl_daft_any_value!(StructArray);

#[cfg(feature = "python")]
impl_daft_any_value!(PythonArray);
