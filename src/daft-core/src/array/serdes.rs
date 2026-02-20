use std::cell::RefCell;

use serde::ser::SerializeMap;

use super::{DataArray, FixedSizeListArray, ListArray, StructArray};
#[cfg(feature = "python")]
use crate::prelude::PythonArray;
use crate::{
    datatypes::{
        BinaryArray, BooleanArray, DaftLogicalType, DaftPrimitiveType, DataType, ExtensionArray,
        Field, FixedSizeBinaryArray, Int64Array, IntervalArray, NullArray, Utf8Array,
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
        s.serialize_entry("values", &IterSer::new(self.iter()))?;
        s.end()
    }
}

macro_rules! impl_serialize_with_iter {
    ($($arr:ty),+ $(,)?) => {
        $(
            impl serde::Serialize for $arr {
                fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
                where
                    S: serde::Serializer,
                {
                    let mut s = serializer.serialize_map(Some(2))?;
                    s.serialize_entry("field", self.field())?;
                    s.serialize_entry("values", &IterSer::new(self.iter()))?;
                    s.end()
                }
            }
        )+
    };
}

impl_serialize_with_iter!(Utf8Array, BooleanArray, BinaryArray, FixedSizeBinaryArray);

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
        let DataType::Extension(_, inner, _) = self.data_type() else {
            panic!("Expected Extension Type!")
        };
        let values = Series::from_arrow(
            Field::new("physical", inner.as_ref().clone()),
            self.to_arrow(),
        )
        .unwrap();
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

        let nulls = self.nulls().map(|b| {
            BooleanArray::from_null_buffer("validity", b)
                .unwrap()
                .into_series()
        });
        values.push(nulls.as_ref());

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
        let offsets = Int64Array::from_slice("offsets", self.offsets().inner()).into_series();
        values.push(Some(&offsets));

        let nulls = self.nulls().map(|b| {
            BooleanArray::from_null_buffer("validity", b)
                .unwrap()
                .into_series()
        });
        values.push(nulls.as_ref());

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

        let nulls = self.nulls().map(|b| {
            BooleanArray::from_null_buffer("validity", b)
                .unwrap()
                .into_series()
        });
        let values = vec![Some(&self.flat_child), nulls.as_ref()];
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
        s.serialize_entry(
            "values",
            &IterSer::new(
                (0..self.len()).map(|i| self.get(i).map(|i| (i.months, i.days, i.nanoseconds))),
            ),
        )?;
        s.end()
    }
}
