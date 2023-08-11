use std::any::Any;

use crate::{
    array::ops::GroupIndices,
    datatypes::{BooleanArray, DataType, Field},
};
use common_error::DaftResult;
use dyn_clone::DynClone;

use super::Series;

/// Trait for a concrete Array to be used as the backing implementation of a Series
///
/// Functions on this trait should be generally applicable to *ALL* Arrays. These are
/// generally either "black-box" functions (functionality that is independent of the
/// values contained in the array) such as [`SeriesLike::name`] and [`SeriesLike::len`],
/// or common functionality such as [`SeriesLike::add`] and [`SeriesLike::equal`].
pub trait SeriesLike: Send + Sync + Any + DynClone {
    // Metadata
    fn data_type(&self) -> &DataType;
    fn field(&self) -> &Field;
    fn len(&self) -> usize;
    fn name(&self) -> &str;
    fn rename(&self, name: &str) -> Series;
    fn size_bytes(&self) -> DaftResult<usize>;

    // Conversion
    #[allow(clippy::wrong_self_convention)]
    fn into_series(&self) -> Series;
    fn to_arrow(&self) -> Box<dyn arrow2::array::Array>;
    fn as_arrow(&self) -> Box<dyn arrow2::array::Array>;
    fn as_any(&self) -> &dyn std::any::Any;

    // Element selection
    fn head(&self, num: usize) -> DaftResult<Series>;
    fn slice(&self, start: usize, end: usize) -> DaftResult<Series>;
    fn take(&self, idx: &Series) -> DaftResult<Series>;
    fn broadcast(&self, num: usize) -> DaftResult<Series>;
    fn filter(&self, mask: &BooleanArray) -> DaftResult<Series>;
    fn if_else(&self, other: &Series, predicate: &Series) -> DaftResult<Series>;

    // Visualization
    fn str_value(&self, idx: usize) -> DaftResult<String>;
    fn html_value(&self, idx: usize) -> String;

    // Common binary operations
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

    // Other common operations
    fn min(&self, groups: Option<&GroupIndices>) -> DaftResult<Series>;
    fn max(&self, groups: Option<&GroupIndices>) -> DaftResult<Series>;
    fn agg_list(&self, groups: Option<&GroupIndices>) -> DaftResult<Series>;
    fn cast(&self, datatype: &DataType) -> DaftResult<Series>;
    fn is_null(&self) -> DaftResult<Series>;
    fn sort(&self, descending: bool) -> DaftResult<Series>;
}
