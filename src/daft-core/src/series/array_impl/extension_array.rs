use std::sync::Arc;

use common_error::DaftResult;
use daft_schema::field::Field;

use super::{ArrayWrapper, IntoSeries};
use crate::{
    array::{extension_array::ExtensionArray, ops::GroupIndices},
    datatypes::{BooleanArray, DataType},
    lit::Literal,
    prelude::UInt64Array,
    series::{Series, series_like::SeriesLike},
};

impl IntoSeries for ExtensionArray {
    fn into_series(self) -> Series {
        Series {
            inner: Arc::new(ArrayWrapper(self)),
        }
    }
}

impl SeriesLike for ArrayWrapper<ExtensionArray> {
    fn into_series(&self) -> Series {
        self.0.clone().into_series()
    }

    fn to_arrow(&self) -> DaftResult<arrow::array::ArrayRef> {
        self.0.to_arrow()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn with_nulls(&self, nulls: Option<arrow::buffer::NullBuffer>) -> DaftResult<Series> {
        Ok(self
            .0
            .with_physical(self.0.physical.with_nulls(nulls)?)
            .into_series())
    }

    fn nulls(&self) -> Option<&arrow::buffer::NullBuffer> {
        self.0.nulls()
    }

    fn min(&self, groups: Option<&GroupIndices>) -> DaftResult<Series> {
        self.0.physical.min(groups)
    }

    fn max(&self, groups: Option<&GroupIndices>) -> DaftResult<Series> {
        self.0.physical.max(groups)
    }

    fn agg_list(&self, groups: Option<&GroupIndices>) -> DaftResult<Series> {
        self.0.physical.agg_list(groups)
    }

    fn agg_set(&self, groups: Option<&GroupIndices>) -> DaftResult<Series> {
        self.0.physical.agg_set(groups)
    }

    fn broadcast(&self, num: usize) -> DaftResult<Series> {
        Ok(self
            .0
            .with_physical(self.0.physical.broadcast(num)?)
            .into_series())
    }

    fn cast(&self, datatype: &DataType) -> DaftResult<Series> {
        match datatype {
            DataType::Extension(_, storage_type, _) => {
                let casted_storage = self.0.physical.cast(storage_type)?;
                let field = Arc::new(Field::new(self.0.name(), datatype.clone()));
                Ok(ExtensionArray::new(field, casted_storage).into_series())
            }
            _ => self.0.physical.cast(datatype),
        }
    }

    fn filter(&self, mask: &BooleanArray) -> DaftResult<Series> {
        Ok(self
            .0
            .with_physical(self.0.physical.filter(mask)?)
            .into_series())
    }

    fn if_else(&self, other: &Series, predicate: &Series) -> DaftResult<Series> {
        let other_physical = match other.downcast::<ExtensionArray>() {
            Ok(other_ext) => &other_ext.physical,
            Err(_) => other,
        };
        Ok(self
            .0
            .with_physical(self.0.physical.if_else(other_physical, predicate)?)
            .into_series())
    }

    fn data_type(&self) -> &DataType {
        self.0.data_type()
    }

    fn field(&self) -> &Field {
        self.0.field()
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

    fn size_bytes(&self) -> usize {
        self.0.physical.size_bytes()
    }

    fn is_null(&self) -> DaftResult<Series> {
        self.0.physical.is_null()
    }

    fn not_null(&self) -> DaftResult<Series> {
        self.0.physical.not_null()
    }

    fn sort(&self, descending: bool, nulls_first: bool) -> DaftResult<Series> {
        Ok(self
            .0
            .with_physical(self.0.physical.sort(descending, nulls_first)?)
            .into_series())
    }

    fn head(&self, num: usize) -> DaftResult<Series> {
        Ok(self
            .0
            .with_physical(self.0.physical.head(num)?)
            .into_series())
    }

    fn slice(&self, start: usize, end: usize) -> DaftResult<Series> {
        Ok(self.0.slice(start, end)?.into_series())
    }

    fn take(&self, idx: &UInt64Array) -> DaftResult<Series> {
        Ok(self
            .0
            .with_physical(self.0.physical.take(idx)?)
            .into_series())
    }

    fn str_value(&self, idx: usize) -> DaftResult<String> {
        self.0.physical.inner.str_value(idx)
    }

    fn get_lit(&self, idx: usize) -> Literal {
        self.0.get_lit(idx)
    }
}
