use crate::datatypes::logical::DateArray;

use super::{ArrayWrapper, IntoSeries, Series};
use crate::series::DaftResult;
use crate::series::SeriesLike;
use crate::with_match_integer_daft_types;
use std::sync::Arc;

macro_rules! impl_series_like_for_logical_array {
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
                self.0.physical.data()
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn broadcast(&self, num: usize) -> DaftResult<Series> {
                use crate::array::ops::broadcast::Broadcastable;
                let data_array = self.0.physical.broadcast(num)?;
                Ok($da::new(self.0.field.clone(), data_array).into_series())
            }

            fn cast(&self, datatype: &crate::datatypes::DataType) -> DaftResult<Series> {
                self.0.cast(datatype)
            }

            fn data_type(&self) -> &crate::datatypes::DataType {
                self.0.logical_type()
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
                    .if_else(other.downcast_logical()?, predicate.downcast()?)?
                    .into_series())
            }

            fn is_null(&self) -> DaftResult<Series> {
                use crate::array::ops::DaftIsNull;

                Ok(DaftIsNull::is_null(&self.0.physical)?.into_series())
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
                self.0.physical.str_value(idx)
            }

            fn take(&self, idx: &Series) -> DaftResult<Series> {
                with_match_integer_daft_types!(idx.data_type(), |$S| {
                    Ok(self.0.take(idx.downcast::<$S>()?)?.into_series())
                })
            }
        }
    };
}

impl_series_like_for_logical_array!(DateArray);
