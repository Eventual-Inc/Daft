use std::any::Any;

use crate::{
    array::ops::GroupIndices,
    datatypes::{BooleanArray, DataType, Field},
};
use common_error::DaftResult;

use super::Series;
pub trait SeriesLike: Send + Sync + Any {
    #[allow(clippy::wrong_self_convention)]
    fn into_series(&self) -> Series;
    fn to_arrow(&self) -> Box<dyn arrow2::array::Array>;
    fn as_any(&self) -> &dyn std::any::Any;
    fn min(&self, groups: Option<&GroupIndices>) -> DaftResult<Series>;
    fn max(&self, groups: Option<&GroupIndices>) -> DaftResult<Series>;
    fn agg_list(&self, groups: Option<&GroupIndices>) -> DaftResult<Series>;
    fn broadcast(&self, num: usize) -> DaftResult<Series>;
    fn cast(&self, datatype: &DataType) -> DaftResult<Series>;
    fn filter(&self, mask: &BooleanArray) -> DaftResult<Series>;
    fn if_else(&self, other: &Series, predicate: &Series) -> DaftResult<Series>;
    fn data_type(&self) -> &DataType;
    fn field(&self) -> &Field;
    fn len(&self) -> usize;
    fn name(&self) -> &str;
    fn rename(&self, name: &str) -> Series;
    fn size_bytes(&self) -> DaftResult<usize>;
    fn is_null(&self) -> DaftResult<Series>;
    fn sort(&self, descending: bool) -> DaftResult<Series>;
    fn head(&self, num: usize) -> DaftResult<Series>;
    fn slice(&self, start: usize, end: usize) -> DaftResult<Series>;
    fn take(&self, idx: &Series) -> DaftResult<Series>;
    fn str_value(&self, idx: usize) -> DaftResult<String>;
    fn html_value(&self, idx: usize) -> String;
    fn add(&self, rhs: &Series) -> DaftResult<Series>;
    fn sub(&self, rhs: &Series) -> DaftResult<Series>;
    fn mul(&self, rhs: &Series) -> DaftResult<Series>;
    fn div(&self, rhs: &Series) -> DaftResult<Series>;
    fn rem(&self, rhs: &Series) -> DaftResult<Series>;
    fn and(&self, rhs: &Series) -> DaftResult<BooleanArray>;
    fn or(&self, rhs: &Series) -> DaftResult<BooleanArray>;
    fn xor(&self, rhs: &Series) -> DaftResult<BooleanArray>;
    fn equal(&self, rhs: &Series) -> DaftResult<BooleanArray>;
    fn not_equal(&self, rhs: &Series) -> DaftResult<BooleanArray>;
    fn lt(&self, rhs: &Series) -> DaftResult<BooleanArray>;
    fn lte(&self, rhs: &Series) -> DaftResult<BooleanArray>;
    fn gt(&self, rhs: &Series) -> DaftResult<BooleanArray>;
    fn gte(&self, rhs: &Series) -> DaftResult<BooleanArray>;
}
