use common_error::DaftResult;
use daft_schema::dtype::DataType;

use crate::{
    array::ops::{DaftListAggable, DaftSetAggable, GroupIndices, broadcast::Broadcastable},
    lit::Literal,
    prelude::{PythonArray, UInt64Array},
    series::{ArrayWrapper, IntoSeries, Series, SeriesLike},
};

impl SeriesLike for ArrayWrapper<PythonArray> {
    fn into_series(&self) -> Series {
        self.0.clone().into_series()
    }
    fn to_arrow(&self) -> Box<dyn daft_arrow::array::Array> {
        self.0.to_arrow().unwrap()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn with_validity(
        &self,
        validity: Option<daft_arrow::buffer::NullBuffer>,
    ) -> DaftResult<Series> {
        Ok(self.0.with_validity(validity)?.into_series())
    }

    fn validity(&self) -> Option<&daft_arrow::buffer::NullBuffer> {
        self.0.validity().map(|v| v.into())
    }

    fn broadcast(&self, num: usize) -> DaftResult<Series> {
        Ok(self.0.broadcast(num)?.into_series())
    }

    fn cast(&self, datatype: &crate::datatypes::DataType) -> DaftResult<Series> {
        self.0.cast(datatype)
    }

    fn data_type(&self) -> &DataType {
        self.0.data_type()
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
            .if_else(other.downcast()?, predicate.downcast()?)?
            .into_series())
    }

    fn is_null(&self) -> DaftResult<Series> {
        use crate::array::ops::DaftIsNull;

        Ok(DaftIsNull::is_null(&self.0)?.into_series())
    }

    fn not_null(&self) -> DaftResult<Series> {
        use crate::array::ops::DaftNotNull;

        Ok(DaftNotNull::not_null(&self.0)?.into_series())
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn size_bytes(&self) -> usize {
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
    fn sort(&self, descending: bool, nulls_first: bool) -> DaftResult<Series> {
        Ok(self.0.sort(descending, nulls_first)?.into_series())
    }
    fn str_value(&self, idx: usize) -> DaftResult<String> {
        self.0.str_value(idx)
    }

    fn take(&self, idx: &UInt64Array) -> DaftResult<Series> {
        Ok(self.0.take(idx)?.into_series())
    }

    fn min(&self, groups: Option<&GroupIndices>) -> DaftResult<Series> {
        use crate::array::ops::DaftCompareAggable;
        match groups {
            Some(groups) => Ok(DaftCompareAggable::grouped_min(&self.0, groups)?.into_series()),
            None => Ok(DaftCompareAggable::min(&self.0)?.into_series()),
        }
    }
    fn max(&self, groups: Option<&GroupIndices>) -> DaftResult<Series> {
        use crate::array::ops::DaftCompareAggable;
        match groups {
            Some(groups) => Ok(DaftCompareAggable::grouped_max(&self.0, groups)?.into_series()),
            None => Ok(DaftCompareAggable::max(&self.0)?.into_series()),
        }
    }

    fn agg_list(&self, groups: Option<&GroupIndices>) -> DaftResult<Series> {
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

    fn get_lit(&self, idx: usize) -> Literal {
        self.0.get_lit(idx)
    }
}
