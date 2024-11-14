use std::any::Any;

use common_error::DaftResult;

use super::Series;
use crate::{
    array::ops::GroupIndices,
    datatypes::{BooleanArray, DataType, Field},
};
pub trait SeriesLike: Send + Sync + Any + std::fmt::Debug {
    #[allow(clippy::wrong_self_convention)]
    fn into_series(&self) -> Series;
    fn to_arrow(&self) -> Box<dyn arrow2::array::Array>;
    fn as_any(&self) -> &dyn std::any::Any;
    fn with_validity(&self, validity: Option<arrow2::bitmap::Bitmap>) -> DaftResult<Series>;
    fn validity(&self) -> Option<&arrow2::bitmap::Bitmap>;
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
    fn not_null(&self) -> DaftResult<Series>;
    fn sort(&self, descending: bool, nulls_first: bool) -> DaftResult<Series>;
    fn head(&self, num: usize) -> DaftResult<Series>;
    fn slice(&self, start: usize, end: usize) -> DaftResult<Series>;
    fn take(&self, idx: &Series) -> DaftResult<Series>;
    fn str_value(&self, idx: usize) -> DaftResult<String>;
}
