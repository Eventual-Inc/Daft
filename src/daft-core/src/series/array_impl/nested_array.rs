use std::sync::Arc;

use common_error::{DaftError, DaftResult};

use super::ArrayWrapper;
use crate::{
    array::{
        ops::{broadcast::Broadcastable, DaftIsNull, DaftNotNull, DaftSetAggable, GroupIndices},
        FixedSizeListArray, ListArray, StructArray,
    },
    datatypes::{BooleanArray, DataType, Field},
    series::{IntoSeries, Series, SeriesLike},
    with_match_integer_daft_types,
};

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

            fn with_validity(
                &self,
                validity: Option<arrow2::bitmap::Bitmap>,
            ) -> DaftResult<Series> {
                Ok(self.0.with_validity(validity)?.into_series())
            }

            fn validity(&self) -> Option<&arrow2::bitmap::Bitmap> {
                self.0.validity()
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

            fn agg_set(&self, groups: Option<&GroupIndices>) -> DaftResult<Series> {
                match groups {
                    Some(groups) => self
                        .0
                        .clone()
                        .into_series()
                        .grouped_set(groups)
                        .map(|x| x.into_series()),
                    None => self.0.clone().into_series().set().map(|x| x.into_series()),
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

            fn not_null(&self) -> DaftResult<Series> {
                Ok(self.0.not_null()?.into_series())
            }

            fn sort(&self, _descending: bool, _nulls_first: bool) -> DaftResult<Series> {
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
        }
    };
}

impl_series_like_for_nested_arrays!(FixedSizeListArray);
impl_series_like_for_nested_arrays!(StructArray);
impl_series_like_for_nested_arrays!(ListArray);
