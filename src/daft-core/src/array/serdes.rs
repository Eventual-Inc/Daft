use std::cell::RefCell;

use serde::ser::SerializeMap;

use super::{DataArray, FixedSizeListArray, ListArray, StructArray, ops::as_arrow::AsArrow};
#[cfg(feature = "python")]
use crate::prelude::PythonArray;
use crate::{
    datatypes::{
        BinaryArray, BooleanArray, DaftLogicalType, DaftPrimitiveType, DataType, ExtensionArray,
        FixedSizeBinaryArray, Int64Array, IntervalArray, NullArray, Utf8Array,
        logical::LogicalArray,
    },
    series::{IntoSeries, Series},
};

pub struct IterSer<I>
where
    I: IntoIterator,
    <I as IntoIterator>::Item: serde::Serialize,
{
    iter: RefCell<Option<I>>,
}

impl<I> IterSer<I>
where
    I: IntoIterator,
    <I as IntoIterator>::Item: serde::Serialize,
{
    fn new(iter: I) -> Self {
        Self {
            iter: RefCell::new(Some(iter)),
        }
    }
}

impl<I> serde::Serialize for IterSer<I>
where
    I: IntoIterator,
    <I as IntoIterator>::Item: serde::Serialize,
{
    fn serialize<S>(
        &self,
        serializer: S,
    ) -> std::result::Result<<S as serde::Serializer>::Ok, <S as serde::Serializer>::Error>
    where
        S: serde::Serializer,
    {
        let iter: I = self.iter.borrow_mut().take().unwrap();
        serializer.collect_seq(iter)
    }
}

impl<T: DaftPrimitiveType> serde::Serialize for DataArray<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_map(Some(2))?;
        s.serialize_entry("field", self.field())?;
        s.serialize_entry("values", &IterSer::new(self.as_arrow2().iter()))?;
        s.end()
    }
}

impl serde::Serialize for Utf8Array {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_map(Some(2))?;
        s.serialize_entry("field", self.field())?;
        s.serialize_entry("values", &IterSer::new(self.as_arrow2().iter()))?;
        s.end()
    }
}

impl serde::Serialize for BooleanArray {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_map(Some(2))?;
        s.serialize_entry("field", self.field())?;
        s.serialize_entry("values", &IterSer::new(self.as_arrow2().iter()))?;
        s.end()
    }
}

impl serde::Serialize for BinaryArray {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_map(Some(2))?;
        s.serialize_entry("field", self.field())?;
        s.serialize_entry("values", &IterSer::new(self.as_arrow2().iter()))?;
        s.end()
    }
}

impl serde::Serialize for FixedSizeBinaryArray {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_map(Some(2))?;
        s.serialize_entry("field", self.field())?;
        s.serialize_entry("values", &IterSer::new(self.as_arrow2().iter()))?;
        s.end()
    }
}

impl serde::Serialize for NullArray {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_map(Some(2))?;
        s.serialize_entry("field", self.field())?;
        s.serialize_entry("values", &self.len())?;
        s.end()
    }
}

impl serde::Serialize for ExtensionArray {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_map(Some(2))?;
        s.serialize_entry("field", self.field())?;
        let values = if let DataType::Extension(_, inner, _) = self.data_type() {
            Series::try_from((
                "physical",
                self.data.convert_logical_type(inner.to_arrow().unwrap()),
            ))
            .unwrap()
        } else {
            panic!("Expected Extension Type!")
        };
        s.serialize_entry("values", &values)?;
        s.end()
    }
}

#[cfg(feature = "python")]
impl serde::Serialize for PythonArray {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_map(Some(2))?;
        s.serialize_entry("field", self.field())?;
        s.serialize_entry(
            "values",
            &IterSer::new(self.to_pickled_arrow().unwrap().iter()),
        )?;
        s.end()
    }
}

impl serde::Serialize for StructArray {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_map(Some(2))?;

        let mut values = Vec::with_capacity(self.children.len() + 1);
        values.extend(self.children.iter().map(Some));

        let validity = self.validity().map(|b| {
            let bitmap = daft_arrow::buffer::from_null_buffer(b.clone());
            BooleanArray::from(("validity", bitmap)).into_series()
        });
        values.push(validity.as_ref());

        s.serialize_entry("field", self.field.as_ref())?;
        s.serialize_entry("values", &values)?;
        s.end()
    }
}

impl serde::Serialize for ListArray {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_map(Some(2))?;
        let mut values = Vec::with_capacity(3);

        values.push(Some(&self.flat_child));

        let arrow2_offsets = daft_arrow::array::Int64Array::new(
            daft_arrow::datatypes::DataType::Int64,
            self.offsets().buffer().clone(),
            None,
        );
        let offsets = Int64Array::from(("offsets", Box::new(arrow2_offsets))).into_series();
        values.push(Some(&offsets));

        let validity = self.validity().map(|b| {
            let bitmap = daft_arrow::buffer::from_null_buffer(b.clone());
            BooleanArray::from(("validity", bitmap)).into_series()
        });
        values.push(validity.as_ref());

        s.serialize_entry("field", self.field.as_ref())?;
        s.serialize_entry("values", &values)?;
        s.end()
    }
}

impl serde::Serialize for FixedSizeListArray {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_map(Some(2))?;

        let validity = self.validity().map(|b| {
            let bitmap = daft_arrow::buffer::from_null_buffer(b.clone());
            BooleanArray::from(("validity", bitmap)).into_series()
        });
        let values = vec![Some(&self.flat_child), validity.as_ref()];
        s.serialize_entry("field", self.field.as_ref())?;
        s.serialize_entry("values", &values)?;
        s.end()
    }
}

impl<L: DaftLogicalType> serde::Serialize for LogicalArray<L>
where
    <<L as DaftLogicalType>::PhysicalType as crate::datatypes::DaftDataType>::ArrayType:
        serde::Serialize + IntoSeries,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_map(Some(2))?;
        s.serialize_entry("field", self.field.as_ref())?;
        s.serialize_entry("values", &self.physical.clone().into_series())?;
        s.end()
    }
}

impl serde::Serialize for IntervalArray {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_map(Some(2))?;
        s.serialize_entry("field", self.field())?;
        s.serialize_entry("values", &IterSer::new(self.as_arrow2().iter()))?;
        s.end()
    }
}
