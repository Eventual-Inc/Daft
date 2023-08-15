use crate::datatypes::logical::{
    DateArray, Decimal128Array, DurationArray, EmbeddingArray, FixedShapeImageArray,
    FixedShapeTensorArray, ImageArray, TensorArray, TimestampArray,
};
use crate::datatypes::{BooleanArray, DaftArrayType, Field};

use super::{ArrayWrapper, IntoSeries, Series};
use crate::array::ops::GroupIndices;
use crate::series::array_impl::binary_ops::SeriesBinaryOps;
use crate::series::DaftResult;
use crate::series::SeriesLike;
use crate::with_match_integer_daft_types;
use crate::{with_match_daft_logical_primitive_types, DataType};
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
            fn into_series(&self) -> Series {
                self.0.clone().into_series()
            }
            fn to_arrow(&self) -> Box<dyn arrow2::array::Array> {
                let daft_type = self.0.data_type();
                let arrow_logical_type = daft_type.to_arrow().unwrap();
                let physical_arrow_array = self.0.physical.data();
                use crate::datatypes::DataType::*;
                match daft_type {
                    // For wrapped primitive types, switch the datatype label on the arrow2 Array.
                    Decimal128(..) | Date | Timestamp(..) | Duration(..) => {
                        with_match_daft_logical_primitive_types!(daft_type, |$P| {
                            use arrow2::array::Array;
                            physical_arrow_array
                                .as_any()
                                .downcast_ref::<arrow2::array::PrimitiveArray<$P>>()
                                .unwrap()
                                .clone()
                                .to(arrow_logical_type)
                                .to_boxed()
                        })
                    }
                    // Otherwise, use arrow cast to make sure the result arrow2 array is of the correct type.
                    _ => arrow2::compute::cast::cast(
                        physical_arrow_array,
                        &arrow_logical_type,
                        arrow2::compute::cast::CastOptions {
                            wrapped: true,
                            partial: false,
                        },
                    )
                    .unwrap(),
                }
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn broadcast(&self, num: usize) -> DaftResult<Series> {
                use crate::array::ops::broadcast::Broadcastable;
                let data_array = self.0.physical.broadcast(num)?;
                Ok($da::new(self.0.field.clone(), data_array).into_series())
            }

            fn cast(&self, datatype: &DataType) -> DaftResult<Series> {
                self.0.cast(datatype)
            }

            fn data_type(&self) -> &DataType {
                self.0.data_type()
            }

            fn field(&self) -> &Field {
                self.0.field()
            }

            fn filter(&self, mask: &crate::datatypes::BooleanArray) -> DaftResult<Series> {
                let new_array = self.0.physical.filter(mask)?;
                Ok($da::new(self.0.field.clone(), new_array).into_series())
            }

            fn head(&self, num: usize) -> DaftResult<Series> {
                let new_array = self.0.physical.head(num)?;
                Ok($da::new(self.0.field.clone(), new_array).into_series())
            }

            fn if_else(&self, other: &Series, predicate: &Series) -> DaftResult<Series> {
                Ok(self
                    .0
                    .if_else(other.downcast()?, predicate.downcast()?)?
                    .into_series())
            }

            fn is_null(&self) -> DaftResult<Series> {
                use crate::array::ops::DaftIsNull;

                Ok(DaftIsNull::is_null(&self.0.physical)?.into_series())
            }

            fn len(&self) -> usize {
                self.0.physical.len()
            }

            fn size_bytes(&self) -> DaftResult<usize> {
                self.0.physical.size_bytes()
            }

            fn name(&self) -> &str {
                self.0.field.name.as_str()
            }

            fn rename(&self, name: &str) -> Series {
                let new_array = self.0.physical.rename(name);
                $da::new(self.0.field.clone(), new_array).into_series()
            }

            fn slice(&self, start: usize, end: usize) -> DaftResult<Series> {
                let new_array = self.0.physical.slice(start, end)?;
                Ok($da::new(self.0.field.clone(), new_array).into_series())
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
                let data_array = match groups {
                    Some(groups) => DaftCompareAggable::grouped_min(&self.0.physical, groups)?,
                    None => DaftCompareAggable::min(&self.0.physical)?,
                };
                Ok($da::new(self.0.field.clone(), data_array).into_series())
            }
            fn max(&self, groups: Option<&GroupIndices>) -> DaftResult<Series> {
                use crate::array::ops::DaftCompareAggable;
                let data_array = match groups {
                    Some(groups) => DaftCompareAggable::grouped_max(&self.0.physical, groups)?,
                    None => DaftCompareAggable::max(&self.0.physical)?,
                };
                Ok($da::new(self.0.field.clone(), data_array).into_series())
            }
            fn agg_list(&self, groups: Option<&GroupIndices>) -> DaftResult<Series> {
                use crate::array::ops::DaftListAggable;
                use crate::datatypes::ListArray;
                let data_array = match groups {
                    Some(groups) => self.0.physical.grouped_list(groups)?,
                    None => self.0.physical.list()?,
                };
                let new_field = self.field().to_list_field()?;
                Ok(ListArray::new(Arc::new(new_field), data_array.data)?.into_series())
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

impl_series_like_for_logical_array!(Decimal128Array);
impl_series_like_for_logical_array!(DateArray);
impl_series_like_for_logical_array!(DurationArray);
impl_series_like_for_logical_array!(ImageArray);
impl_series_like_for_logical_array!(TimestampArray);
impl_series_like_for_logical_array!(TensorArray);

macro_rules! impl_series_like_stub {
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
                todo!()
            }
            fn to_arrow(&self) -> Box<dyn arrow2::array::Array> {
                todo!()
            }

            fn as_any(&self) -> &dyn std::any::Any {
                todo!()
            }

            fn broadcast(&self, _num: usize) -> DaftResult<Series> {
                todo!()
            }

            fn cast(&self, _datatype: &DataType) -> DaftResult<Series> {
                todo!()
            }

            fn data_type(&self) -> &DataType {
                todo!()
            }

            fn field(&self) -> &Field {
                todo!()
            }

            fn filter(&self, _mask: &crate::datatypes::BooleanArray) -> DaftResult<Series> {
                todo!()
            }

            fn head(&self, _num: usize) -> DaftResult<Series> {
                todo!()
            }

            fn if_else(&self, _other: &Series, _predicate: &Series) -> DaftResult<Series> {
                todo!()
            }

            fn is_null(&self) -> DaftResult<Series> {
                todo!()
            }

            fn len(&self) -> usize {
                todo!()
            }

            fn size_bytes(&self) -> DaftResult<usize> {
                todo!()
            }

            fn name(&self) -> &str {
                todo!()
            }

            fn rename(&self, _name: &str) -> Series {
                todo!()
            }

            fn slice(&self, _start: usize, _end: usize) -> DaftResult<Series> {
                todo!()
            }

            fn sort(&self, _descending: bool) -> DaftResult<Series> {
                todo!()
            }

            fn str_value(&self, _idx: usize) -> DaftResult<String> {
                todo!()
            }

            fn html_value(&self, _idx: usize) -> String {
                todo!()
            }

            fn take(&self, _idx: &Series) -> DaftResult<Series> {
                todo!()
            }

            fn min(&self, _groups: Option<&GroupIndices>) -> DaftResult<Series> {
                todo!()
            }
            fn max(&self, _groups: Option<&GroupIndices>) -> DaftResult<Series> {
                todo!()
            }
            fn agg_list(&self, _groups: Option<&GroupIndices>) -> DaftResult<Series> {
                todo!()
            }

            fn add(&self, _rhs: &Series) -> DaftResult<Series> {
                todo!()
            }

            fn sub(&self, _rhs: &Series) -> DaftResult<Series> {
                todo!()
            }

            fn mul(&self, _rhs: &Series) -> DaftResult<Series> {
                todo!()
            }

            fn div(&self, _rhs: &Series) -> DaftResult<Series> {
                todo!()
            }

            fn rem(&self, _rhs: &Series) -> DaftResult<Series> {
                todo!()
            }
            fn and(&self, _rhs: &Series) -> DaftResult<BooleanArray> {
                todo!()
            }
            fn or(&self, _rhs: &Series) -> DaftResult<BooleanArray> {
                todo!()
            }
            fn xor(&self, _rhs: &Series) -> DaftResult<BooleanArray> {
                todo!()
            }
            fn equal(&self, _rhs: &Series) -> DaftResult<BooleanArray> {
                todo!()
            }
            fn not_equal(&self, _rhs: &Series) -> DaftResult<BooleanArray> {
                todo!()
            }
            fn lt(&self, _rhs: &Series) -> DaftResult<BooleanArray> {
                todo!()
            }
            fn lte(&self, _rhs: &Series) -> DaftResult<BooleanArray> {
                todo!()
            }
            fn gt(&self, _rhs: &Series) -> DaftResult<BooleanArray> {
                todo!()
            }
            fn gte(&self, _rhs: &Series) -> DaftResult<BooleanArray> {
                todo!()
            }
        }
    };
}

impl_series_like_stub!(EmbeddingArray);
impl_series_like_stub!(FixedShapeImageArray);
impl_series_like_stub!(FixedShapeTensorArray);
