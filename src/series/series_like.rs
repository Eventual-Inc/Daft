use std::{
    any::Any,
    fmt::Debug,
    ops::{Add, Div, Not, Rem, Sub},
    sync::Arc,
};

use crate::{
    array::ops::{DaftCompare, DaftLogical, GroupIndices, GroupIndicesPair},
    datatypes::{BooleanArray, DataType, Field, UInt64Array},
    error::DaftResult,
};

use super::Series;

pub trait SeriesLike: Send + Sync + Any {
    fn array(&self) -> &dyn arrow2::array::Array;
    fn as_any(&self) -> &dyn std::any::Any;

    // fn abs(&self) -> DaftResult<Series>;
    // fn count(&self, groups: Option<&GroupIndices>) -> DaftResult<Series>;
    // fn sum(&self, groups: Option<&GroupIndices>) -> DaftResult<Series>;
    // fn mean(&self, groups: Option<&GroupIndices>) -> DaftResult<Series>;
    // fn min(&self, groups: Option<&GroupIndices>) -> DaftResult<Series>;
    // fn max(&self, groups: Option<&GroupIndices>) -> DaftResult<Series>;
    // fn agg_list(&self, groups: Option<&GroupIndices>) -> DaftResult<Series>;
    // fn agg_concat(&self, groups: Option<&GroupIndices>) -> DaftResult<Series>;
    fn broadcast(&self, num: usize) -> DaftResult<Series>;
    fn cast(&self, datatype: &DataType) -> DaftResult<Series>;

    // fn concat(series: &[&Self]) -> DaftResult<Series>;
    // fn dt_day(&self) -> DaftResult<Series>;
    // fn dt_month(&self) -> DaftResult<Series>;
    // fn dt_year(&self) -> DaftResult<Series>;
    // fn dt_day_of_week(&self) -> DaftResult<Series>;
    fn filter(&self, mask: &BooleanArray) -> DaftResult<Series>;

    // fn is_nan(&self) -> DaftResult<Series>;
    // fn make_groups(&self) -> DaftResult<GroupIndicesPair>;
    // fn hash(&self, seed: Option<&UInt64Array>) -> DaftResult<UInt64Array>;
    fn if_else(&self, other: &Series, predicate: &Series) -> DaftResult<Series>;
    fn data_type(&self) -> &DataType;
    fn field(&self) -> &Field;
    fn len(&self) -> usize;
    fn name(&self) -> &str;
    fn rename(&self, name: &str) -> Series;
    fn size_bytes(&self) -> DaftResult<usize>;
    // fn explode(&self) -> DaftResult<Series>;
    // fn lengths(&self) -> DaftResult<UInt64Array>;
    fn is_null(&self) -> DaftResult<Series>;
    // fn search_sorted(&self, keys: &Self, descending: bool) -> DaftResult<UInt64Array>;
    // fn argsort(&self, descending: bool) -> DaftResult<UInt64Array>;
    // fn argsort_multikey(sort_keys: &[&Self], descending: &[bool]) -> DaftResult<UInt64Array>;
    fn sort(&self, descending: bool) -> DaftResult<Series>;
    fn head(&self, num: usize) -> DaftResult<Series>;
    fn slice(&self, start: usize, end: usize) -> DaftResult<Series>;
    fn take(&self, idx: &Series) -> DaftResult<Series>;
    fn str_value(&self, idx: usize) -> DaftResult<String>;
    // fn utf8_endswith(&self, pattern: &Self) -> DaftResult<Series>;
    // fn utf8_startswith(&self, pattern: &Self) -> DaftResult<Series>;
    // fn utf8_contains(&self, pattern: &Self) -> DaftResult<Series>;
    // fn utf8_length(&self) -> DaftResult<Series>;
}
