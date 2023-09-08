use std::sync::Arc;

use common_error::{DaftError, DaftResult};

use crate::array::ops::broadcast::Broadcastable;
use crate::array::ops::{DaftIsNull, GroupIndices};
use crate::array::{FixedSizeListArray, ListArray, StructArray};
use crate::datatypes::BooleanArray;
use crate::datatypes::Field;
use crate::series::{array_impl::binary_ops::SeriesBinaryOps, IntoSeries, Series, SeriesLike};
use crate::{with_match_integer_daft_types, DataType};

use super::ArrayWrapper;

macro_rules! impl_series_like_for_nested_arrays {
    ($da:ident) => {
        impl IntoSeries for $da {
            fn into_series(self) -> Series {
                Series {
                    inner: Arc::new(ArrayWrapper(self)),
                }
            }
        }

        impl SeriesLike for ArrayWrapper<$da> {
            fn into_series(&self) -> Series {
                self.0.clone().into_series()
            }

            fn to_arrow(&self) -> Box<dyn arrow2::array::Array> {
                self.0.to_arrow()
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn min(&self, _groups: Option<&GroupIndices>) -> DaftResult<Series> {
                Err(DaftError::ValueError(format!(
                    "{} does not support min",
                    stringify!($da)
                )))
            }

            fn max(&self, _groups: Option<&GroupIndices>) -> DaftResult<Series> {
                Err(DaftError::ValueError(format!(
                    "{} does not support max",
                    stringify!($da)
                )))
            }

            fn agg_list(&self, groups: Option<&GroupIndices>) -> DaftResult<Series> {
                use crate::array::ops::DaftListAggable;

                match groups {
                    Some(groups) => Ok(self.0.grouped_list(groups)?.into_series()),
                    None => Ok(self.0.list()?.into_series()),
                }
            }

            fn broadcast(&self, num: usize) -> DaftResult<Series> {
                Ok(self.0.broadcast(num)?.into_series())
            }

            fn cast(&self, datatype: &DataType) -> DaftResult<Series> {
                self.0.cast(datatype)
            }

            fn filter(&self, mask: &BooleanArray) -> DaftResult<Series> {
                Ok(self.0.filter(mask)?.into_series())
            }

            fn if_else(&self, other: &Series, predicate: &Series) -> DaftResult<Series> {
                Ok(self
                    .0
                    .if_else(other.downcast()?, predicate.bool()?)?
                    .into_series())
            }

            fn data_type(&self) -> &DataType {
                self.0.data_type()
            }

            fn field(&self) -> &Field {
                &self.0.field
            }

            fn len(&self) -> usize {
                self.0.len()
            }

            fn name(&self) -> &str {
                self.0.name()
            }

            fn rename(&self, name: &str) -> Series {
                self.0.rename(name).into_series()
            }

            fn size_bytes(&self) -> DaftResult<usize> {
                self.0.size_bytes()
            }

            fn is_null(&self) -> DaftResult<Series> {
                Ok(self.0.is_null()?.into_series())
            }

            fn sort(&self, _descending: bool) -> DaftResult<Series> {
                Err(DaftError::ValueError(format!(
                    "Cannot sort a {}",
                    stringify!($da)
                )))
            }

            fn head(&self, num: usize) -> DaftResult<Series> {
                self.slice(0, num)
            }

            fn slice(&self, start: usize, end: usize) -> DaftResult<Series> {
                Ok(self.0.slice(start, end)?.into_series())
            }

            fn take(&self, idx: &Series) -> DaftResult<Series> {
                with_match_integer_daft_types!(idx.data_type(), |$S| {
                    Ok(self
                        .0
                        .take(idx.downcast::<<$S as DaftDataType>::ArrayType>()?)?
                        .into_series())
                })
            }

            fn str_value(&self, idx: usize) -> DaftResult<String> {
                self.0.str_value(idx)
            }

            fn html_value(&self, idx: usize) -> String {
                self.0.html_value(idx)
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

            fn and(&self, rhs: &Series) -> DaftResult<BooleanArray> {
                SeriesBinaryOps::and(self, rhs)
            }
            fn or(&self, rhs: &Series) -> DaftResult<BooleanArray> {
                SeriesBinaryOps::or(self, rhs)
            }
            fn xor(&self, rhs: &Series) -> DaftResult<BooleanArray> {
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

impl_series_like_for_nested_arrays!(FixedSizeListArray);
impl_series_like_for_nested_arrays!(StructArray);
impl_series_like_for_nested_arrays!(ListArray);
