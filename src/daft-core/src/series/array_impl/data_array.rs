use super::{ArrayWrapper, IntoSeries, Series};
use std::sync::Arc;

use crate::array::ops::broadcast::Broadcastable;
use crate::array::ops::DaftListAggable;
use crate::array::ops::GroupIndices;
use crate::array::DataArray;
use crate::datatypes::DaftArrowBackedType;

use crate::datatypes::FixedSizeBinaryArray;
#[cfg(feature = "python")]
use crate::datatypes::PythonArray;
use crate::series::array_impl::binary_ops::SeriesBinaryOps;
use crate::{
    datatypes::{
        BinaryArray, BooleanArray, ExtensionArray, Float32Array, Float64Array, Int128Array,
        Int16Array, Int32Array, Int64Array, Int8Array, NullArray, UInt16Array, UInt32Array,
        UInt64Array, UInt8Array, Utf8Array,
    },
    series::series_like::SeriesLike,
    with_match_integer_daft_types,
};
use common_error::DaftResult;

use crate::datatypes::DataType;

impl<T: DaftArrowBackedType> IntoSeries for DataArray<T>
where
    ArrayWrapper<DataArray<T>>: SeriesLike,
{
    fn into_series(self) -> Series {
        Series {
            inner: Arc::new(ArrayWrapper(self)),
        }
    }
}

#[cfg(feature = "python")]
impl IntoSeries for PythonArray {
    fn into_series(self) -> Series {
        Series {
            inner: Arc::new(ArrayWrapper(self)),
        }
    }
}

macro_rules! impl_series_like_for_data_array {
    ($da:ident) => {
        impl SeriesLike for ArrayWrapper<$da> {
            fn into_series(&self) -> Series {
                self.0.clone().into_series()
            }
            fn to_arrow(&self) -> Box<dyn arrow2::array::Array> {
                self.0.data().to_boxed()
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn with_validity(
                &self,
                validity: Option<arrow2::bitmap::Bitmap>,
            ) -> DaftResult<Series> {
                Ok(self.0.with_validity(validity)?.into_series())
            }

            fn validity(&self) -> Option<&arrow2::bitmap::Bitmap> {
                self.0.validity()
            }

            fn broadcast(&self, num: usize) -> DaftResult<Series> {
                Ok(self.0.broadcast(num)?.into_series())
            }

            fn cast(&self, datatype: &crate::datatypes::DataType) -> DaftResult<Series> {
                self.0.cast(datatype)
            }

            fn data_type(&self) -> &DataType {
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

            fn not_null(&self) -> DaftResult<Series> {
                use crate::array::ops::DaftNotNull;

                Ok(DaftNotNull::not_null(&self.0)?.into_series())
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
            fn html_value(&self, idx: usize) -> String {
                self.0.html_value(idx)
            }
            fn take(&self, idx: &Series) -> DaftResult<Series> {
                with_match_integer_daft_types!(idx.data_type(), |$S| {
                    Ok(self
                        .0
                        .take(idx.downcast::<<$S as DaftDataType>::ArrayType>()?)?
                        .into_series())
                })
            }

            fn min(&self, groups: Option<&GroupIndices>) -> DaftResult<Series> {
                use crate::array::ops::DaftCompareAggable;
                match groups {
                    Some(groups) => {
                        Ok(DaftCompareAggable::grouped_min(&self.0, groups)?.into_series())
                    }
                    None => Ok(DaftCompareAggable::min(&self.0)?.into_series()),
                }
            }
            fn max(&self, groups: Option<&GroupIndices>) -> DaftResult<Series> {
                use crate::array::ops::DaftCompareAggable;
                match groups {
                    Some(groups) => {
                        Ok(DaftCompareAggable::grouped_max(&self.0, groups)?.into_series())
                    }
                    None => Ok(DaftCompareAggable::max(&self.0)?.into_series()),
                }
            }

            fn agg_list(&self, groups: Option<&GroupIndices>) -> DaftResult<Series> {
                match groups {
                    Some(groups) => Ok(self.0.grouped_list(groups)?.into_series()),
                    None => Ok(self.0.list()?.into_series()),
                }
            }

            fn add(&self, rhs: &Series) -> DaftResult<Series> {
                SeriesBinaryOps::add(self, rhs)
            }
            fn sub(&self, rhs: &Series) -> DaftResult<Series> {
                SeriesBinaryOps::sub(self, rhs)
            }
            fn mul(&self, rhs: &Series) -> DaftResult<Series> {
                SeriesBinaryOps::mul(self, rhs)
            }
            fn div(&self, rhs: &Series) -> DaftResult<Series> {
                SeriesBinaryOps::div(self, rhs)
            }
            fn rem(&self, rhs: &Series) -> DaftResult<Series> {
                SeriesBinaryOps::rem(self, rhs)
            }

            fn and(&self, rhs: &Series) -> DaftResult<Series> {
                SeriesBinaryOps::and(self, rhs)
            }
            fn or(&self, rhs: &Series) -> DaftResult<Series> {
                SeriesBinaryOps::or(self, rhs)
            }
            fn xor(&self, rhs: &Series) -> DaftResult<Series> {
                SeriesBinaryOps::xor(self, rhs)
            }

            fn equal(&self, rhs: &Series) -> DaftResult<BooleanArray> {
                SeriesBinaryOps::equal(self, rhs)
            }
            fn not_equal(&self, rhs: &Series) -> DaftResult<BooleanArray> {
                SeriesBinaryOps::not_equal(self, rhs)
            }
            fn lt(&self, rhs: &Series) -> DaftResult<BooleanArray> {
                SeriesBinaryOps::lt(self, rhs)
            }
            fn lte(&self, rhs: &Series) -> DaftResult<BooleanArray> {
                SeriesBinaryOps::lte(self, rhs)
            }
            fn gt(&self, rhs: &Series) -> DaftResult<BooleanArray> {
                SeriesBinaryOps::gt(self, rhs)
            }
            fn gte(&self, rhs: &Series) -> DaftResult<BooleanArray> {
                SeriesBinaryOps::gte(self, rhs)
            }
        }
    };
}

impl_series_like_for_data_array!(NullArray);
impl_series_like_for_data_array!(BooleanArray);
impl_series_like_for_data_array!(BinaryArray);
impl_series_like_for_data_array!(FixedSizeBinaryArray);
impl_series_like_for_data_array!(Int8Array);
impl_series_like_for_data_array!(Int16Array);
impl_series_like_for_data_array!(Int32Array);
impl_series_like_for_data_array!(Int64Array);
impl_series_like_for_data_array!(Int128Array);
impl_series_like_for_data_array!(UInt8Array);
impl_series_like_for_data_array!(UInt16Array);
impl_series_like_for_data_array!(UInt32Array);
impl_series_like_for_data_array!(UInt64Array);
impl_series_like_for_data_array!(Float32Array);
impl_series_like_for_data_array!(Float64Array);
impl_series_like_for_data_array!(Utf8Array);
impl_series_like_for_data_array!(ExtensionArray);
#[cfg(feature = "python")]
impl_series_like_for_data_array!(PythonArray);
