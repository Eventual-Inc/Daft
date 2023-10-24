use serde::ser::{SerializeMap, SerializeStruct};

use crate::{
    datatypes::{
        logical::LogicalArray, BinaryArray, BooleanArray, DaftLogicalType, DaftNumericType,
        ExtensionArray, Int64Array, NullArray, PythonArray, Utf8Array,
    },
    IntoSeries, Series,
};

use super::{ops::as_arrow::AsArrow, DataArray, FixedSizeListArray, ListArray, StructArray};

impl<T: DaftNumericType> serde::Serialize for DataArray<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_map(Some(2))?;
        s.serialize_entry("field", self.field())?;
        s.serialize_entry("values", &self.as_arrow().iter().collect::<Vec<_>>())?;
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
        s.serialize_entry("values", &self.as_arrow().iter().collect::<Vec<_>>())?;
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
        s.serialize_entry("values", &self.as_arrow().iter().collect::<Vec<_>>())?;
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
        s.serialize_entry("values", &self.as_arrow().iter().collect::<Vec<_>>())?;
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

        let values = Series::try_from(("values", self.data.clone()))
            .unwrap()
            .as_physical()
            .unwrap();
        s.serialize_entry("values", &values)?;
        s.end()
    }
}

#[cfg(feature = "python")]
impl serde::Serialize for PythonArray {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        panic!("Rust Serde is not implemented for Python Arrays")
    }
}

impl serde::Serialize for StructArray {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_map(Some(2))?;

        let mut values = Vec::with_capacity(self.children.len() + 1);
        let validity = self
            .validity()
            .map(|b| BooleanArray::from(("validity", b.clone())).into_series());
        values.push(validity.as_ref());
        values.extend(self.children.iter().map(|c| Some(c)));
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

        let arrow2_offsets = arrow2::array::Int64Array::new(
            arrow2::datatypes::DataType::Int64,
            self.offsets().buffer().clone(),
            self.validity().cloned(),
        );
        let offsets = &Int64Array::from(("validity", Box::new(arrow2_offsets))).into_series();
        let values = vec![offsets, &self.flat_child];
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

        let validity = self
            .validity()
            .map(|b| BooleanArray::from(("validity", b.clone())).into_series());
        let values = vec![validity.as_ref(), Some(&self.flat_child)];
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
