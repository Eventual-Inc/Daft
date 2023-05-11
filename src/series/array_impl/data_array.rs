use super::{ArrayWrapper, IntoSeries, Series};
use std::sync::Arc;

use crate::{
    datatypes::{
        BinaryArray, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int16Array,
        Int32Array, Int64Array, Int8Array, ListArray, NullArray, PythonArray, StructArray,
        UInt16Array, UInt32Array, UInt64Array, UInt8Array, Utf8Array,
    },
    error::DaftResult,
    series::series_like::SeriesLike,
    with_match_integer_daft_types,
};

macro_rules! impl_series_like_for_data_array {
    ($da:ident) => {
        impl IntoSeries for $da {
            fn into_series(self) -> Series {
                Series {
                    inner: Arc::new(ArrayWrapper(self)),
                }
            }
        }

        impl SeriesLike for ArrayWrapper<$da> {
            fn array(&self) -> &dyn arrow2::array::Array {
                self.0.data()
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn broadcast(&self, num: usize) -> DaftResult<Series> {
                use crate::array::ops::broadcast::Broadcastable;
                Ok(self.0.broadcast(num)?.into_series())
            }

            fn cast(&self, datatype: &crate::datatypes::DataType) -> DaftResult<Series> {
                self.0.cast(datatype)
            }

            fn data_type(&self) -> &crate::datatypes::DataType {
                self.0.data_type()
            }
            fn field(&self) -> &crate::datatypes::Field {
                self.0.field()
            }
            fn filter(&self, mask: &crate::datatypes::BooleanArray) -> DaftResult<Series> {
                Ok(self.0.filter(mask)?.into_series())
            }
            fn head(&self, num: usize) -> DaftResult<Series> {
                Ok(self.0.head(num)?.into_series())
            }

            fn if_else(&self, other: &Series, predicate: &Series) -> DaftResult<Series> {
                Ok(self
                    .0
                    .if_else(other.downcast()?, predicate.downcast()?)?
                    .into_series())
            }

            fn is_null(&self) -> DaftResult<Series> {
                use crate::array::ops::DaftIsNull;

                Ok(DaftIsNull::is_null(&self.0)?.into_series())
            }
            fn len(&self) -> usize {
                self.0.len()
            }

            fn size_bytes(&self) -> DaftResult<usize> {
                self.0.size_bytes()
            }

            fn name(&self) -> &str {
                self.0.name()
            }
            fn rename(&self, name: &str) -> Series {
                self.0.rename(name).into_series()
            }
            fn slice(&self, start: usize, end: usize) -> DaftResult<Series> {
                Ok(self.0.slice(start, end)?.into_series())
            }
            fn sort(&self, descending: bool) -> DaftResult<Series> {
                Ok(self.0.sort(descending)?.into_series())
            }
            fn str_value(&self, idx: usize) -> DaftResult<String> {
                self.0.str_value(idx)
            }
            fn take(&self, idx: &Series) -> DaftResult<Series> {
                with_match_integer_daft_types!(idx.data_type(), |$S| {
                    Ok(self.0.take(idx.downcast::<$S>()?)?.into_series())
                })
            }
        }
    };
}

impl_series_like_for_data_array!(NullArray);
impl_series_like_for_data_array!(BooleanArray);
impl_series_like_for_data_array!(BinaryArray);
impl_series_like_for_data_array!(Int8Array);
impl_series_like_for_data_array!(Int16Array);
impl_series_like_for_data_array!(Int32Array);
impl_series_like_for_data_array!(Int64Array);
impl_series_like_for_data_array!(UInt8Array);
impl_series_like_for_data_array!(UInt16Array);
impl_series_like_for_data_array!(UInt32Array);
impl_series_like_for_data_array!(UInt64Array);
impl_series_like_for_data_array!(Float32Array);
impl_series_like_for_data_array!(Float64Array);
impl_series_like_for_data_array!(Utf8Array);
impl_series_like_for_data_array!(FixedSizeListArray);
impl_series_like_for_data_array!(ListArray);
impl_series_like_for_data_array!(StructArray);
impl_series_like_for_data_array!(PythonArray);
