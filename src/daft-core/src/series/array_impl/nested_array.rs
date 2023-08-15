use common_error::DaftResult;

use crate::array::ops::GroupIndices;
use crate::datatypes::Field;
use crate::datatypes::{nested_arrays::FixedSizeListArray, BooleanArray};
use crate::series::{IntoSeries, Series, SeriesLike};
use crate::DataType;

use super::ArrayWrapper;

impl IntoSeries for FixedSizeListArray {
    fn into_series(self) -> Series {
        // TODO(FixedSizeList)
        todo!()
    }
}

impl SeriesLike for ArrayWrapper<FixedSizeListArray> {
    // TODO(FixedSizeList)
    fn into_series(&self) -> Series {
        todo!()
    }

    fn to_arrow(&self) -> Box<dyn arrow2::array::Array> {
        todo!()
    }

    fn as_any(&self) -> &dyn std::any::Any {
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

    fn broadcast(&self, _num: usize) -> DaftResult<Series> {
        todo!()
    }

    fn cast(&self, _datatype: &DataType) -> DaftResult<Series> {
        todo!()
    }

    fn filter(&self, _mask: &BooleanArray) -> DaftResult<Series> {
        todo!()
    }

    fn if_else(&self, _other: &Series, _predicate: &Series) -> DaftResult<Series> {
        todo!()
    }

    fn data_type(&self) -> &DataType {
        todo!()
    }

    fn field(&self) -> &Field {
        todo!()
    }

    fn len(&self) -> usize {
        todo!()
    }

    fn name(&self) -> &str {
        todo!()
    }

    fn rename(&self, _name: &str) -> Series {
        todo!()
    }

    fn size_bytes(&self) -> DaftResult<usize> {
        todo!()
    }

    fn is_null(&self) -> DaftResult<Series> {
        todo!()
    }

    fn sort(&self, _descending: bool) -> DaftResult<Series> {
        todo!()
    }

    fn head(&self, _num: usize) -> DaftResult<Series> {
        todo!()
    }

    fn slice(&self, _start: usize, _end: usize) -> DaftResult<Series> {
        todo!()
    }

    fn take(&self, _idx: &Series) -> DaftResult<Series> {
        todo!()
    }

    fn str_value(&self, _idx: usize) -> DaftResult<String> {
        todo!()
    }

    fn html_value(&self, _idx: usize) -> String {
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
