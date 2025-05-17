use std::fmt;

use common_error::DaftError;
use daft_core::prelude::{DataType, Field};
use indexmap::IndexMap;
use serde::{
    de::{self},
    ser::{self, Error},
    Serialize,
};

use super::{literals_to_series, LiteralValue};

#[derive(Debug)]
pub struct LitError {
    message: String,
}

impl From<LitError> for DaftError {
    fn from(err: LitError) -> Self {
        Self::ValueError(err.message)
    }
}

impl ser::Error for LitError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self {
            message: msg.to_string(),
        }
    }
}
impl de::Error for LitError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self {
            message: msg.to_string(),
        }
    }
}

impl fmt::Display for LitError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for LitError {}

pub struct SerializeVec {
    values: Vec<LiteralValue>,
}

impl ser::SerializeSeq for SerializeVec {
    type Ok = LiteralValue;
    type Error = LitError;

    fn serialize_element<T>(&mut self, value: &T) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.values.push(value.serialize(LiteralValueSerializer)?);
        Ok(())
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(LiteralValue::Series(
            literals_to_series(self.values.as_slice()).unwrap(),
        ))
    }
}

impl ser::SerializeTuple for SerializeVec {
    type Ok = LiteralValue;
    type Error = LitError;

    fn serialize_element<T>(&mut self, value: &T) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        ser::SerializeSeq::end(self)
    }
}
impl ser::SerializeTupleStruct for SerializeVec {
    type Ok = LiteralValue;
    type Error = LitError;

    fn serialize_field<T>(&mut self, value: &T) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        ser::SerializeSeq::end(self)
    }
}

pub struct SerializeTupleVariant {
    name: String,
    values: Vec<LiteralValue>,
}

impl ser::SerializeTupleVariant for SerializeTupleVariant {
    type Ok = LiteralValue;
    type Error = LitError;

    fn serialize_field<T>(&mut self, value: &T) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.values.push(value.serialize(LiteralValueSerializer)?);
        Ok(())
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        let series = literals_to_series(&self.values).unwrap();

        let mut map = IndexMap::new();
        map.insert(
            Field::new(self.name, series.data_type().clone()),
            LiteralValue::Series(series),
        );
        Ok(LiteralValue::Struct(map))
    }
}

pub struct SerializeMap {
    map: IndexMap<Field, LiteralValue>,
    next_key: Option<Field>,
}

impl ser::SerializeMap for SerializeMap {
    type Ok = LiteralValue;
    type Error = LitError;

    fn serialize_key<T>(&mut self, key: &T) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        let key_value = key.serialize(LiteralValueSerializer)?;
        let dtype = key_value.get_type();
        if let LiteralValue::Utf8(s) = key_value {
            self.next_key = Some(Field::new(s, dtype));
            Ok(())
        } else {
            Err(LitError::custom("Map keys must be strings"))
        }
    }

    fn serialize_value<T>(&mut self, value: &T) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        let key = self.next_key.take().unwrap();
        let value = value.serialize(LiteralValueSerializer)?;
        self.map.insert(key, value);
        Ok(())
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(LiteralValue::Struct(self.map))
    }
}
pub struct SerializeStruct {
    map: IndexMap<Field, LiteralValue>,
}

impl ser::SerializeStruct for SerializeStruct {
    type Ok = LiteralValue;
    type Error = LitError;

    fn serialize_field<T>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        let value_lit = value.serialize(LiteralValueSerializer)?;
        let field = Field::new(key.to_string(), value_lit.get_type());
        self.map.insert(field, value_lit);
        Ok(())
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(LiteralValue::Struct(self.map))
    }
}
pub struct SerializeStructVariant {
    name: String,
    map: IndexMap<Field, LiteralValue>,
}

impl ser::SerializeStructVariant for SerializeStructVariant {
    type Ok = LiteralValue;
    type Error = LitError;

    fn serialize_field<T>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        let value_lit = value.serialize(LiteralValueSerializer)?;
        let dtype = value_lit.get_type();
        let field = Field::new(key.to_string(), dtype);
        self.map.insert(field, value_lit);
        Ok(())
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        let variant_struct = LiteralValue::Struct(self.map);
        let mut outer_map = IndexMap::new();
        let variant_field = Field::new(self.name, DataType::Struct(vec![]));
        outer_map.insert(variant_field, variant_struct);
        Ok(LiteralValue::Struct(outer_map))
    }
}
pub struct LiteralValueSerializer;

impl ser::Serializer for LiteralValueSerializer {
    type Ok = LiteralValue;
    type Error = LitError;

    type SerializeSeq = SerializeVec;
    type SerializeTuple = SerializeVec;
    type SerializeTupleStruct = SerializeVec;
    type SerializeTupleVariant = SerializeTupleVariant;
    type SerializeMap = SerializeMap;
    type SerializeStruct = SerializeStruct;
    type SerializeStructVariant = SerializeStructVariant;

    fn serialize_bool(self, v: bool) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(LiteralValue::Boolean(v))
    }

    fn serialize_i8(self, v: i8) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(LiteralValue::Int8(v))
    }

    fn serialize_i16(self, v: i16) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(LiteralValue::Int16(v))
    }

    fn serialize_i32(self, v: i32) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(LiteralValue::Int32(v))
    }

    fn serialize_i64(self, v: i64) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(LiteralValue::Int64(v))
    }

    fn serialize_u8(self, v: u8) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(LiteralValue::UInt8(v))
    }

    fn serialize_u16(self, v: u16) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(LiteralValue::UInt16(v))
    }

    fn serialize_u32(self, v: u32) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(LiteralValue::UInt32(v))
    }

    fn serialize_u64(self, v: u64) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(LiteralValue::UInt64(v))
    }

    fn serialize_f32(self, v: f32) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(LiteralValue::Float64(v as f64))
    }

    fn serialize_f64(self, v: f64) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(LiteralValue::Float64(v))
    }

    fn serialize_char(self, v: char) -> std::result::Result<Self::Ok, Self::Error> {
        let mut s = String::new();
        s.push(v);
        Ok(LiteralValue::Utf8(s))
    }

    fn serialize_str(self, v: &str) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(LiteralValue::Utf8(v.to_string()))
    }

    fn serialize_bytes(self, v: &[u8]) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(LiteralValue::Binary(v.to_vec()))
    }

    fn serialize_none(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(LiteralValue::Null)
    }

    fn serialize_some<T>(self, value: &T) -> std::result::Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(LiteralValue::Null)
    }

    fn serialize_unit_struct(
        self,
        _name: &'static str,
    ) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(LiteralValue::Null)
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> std::result::Result<Self::Ok, Self::Error> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        value: &T,
    ) -> std::result::Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> std::result::Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        let value_lit = value.serialize(Self)?;
        let dtype = value_lit.get_type();
        let field = Field::new(variant.to_string(), dtype);
        let mut map = IndexMap::new();
        map.insert(field, value_lit);
        Ok(LiteralValue::Struct(map))
    }

    fn serialize_seq(
        self,
        _len: Option<usize>,
    ) -> std::result::Result<Self::SerializeSeq, Self::Error> {
        Ok(SerializeVec { values: Vec::new() })
    }

    fn serialize_tuple(self, len: usize) -> std::result::Result<Self::SerializeTuple, Self::Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> std::result::Result<Self::SerializeTupleStruct, Self::Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> std::result::Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(SerializeTupleVariant {
            name: variant.to_string(),
            values: Vec::new(),
        })
    }

    fn serialize_map(
        self,
        _len: Option<usize>,
    ) -> std::result::Result<Self::SerializeMap, Self::Error> {
        Ok(SerializeMap {
            map: IndexMap::new(),
            next_key: None,
        })
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> std::result::Result<Self::SerializeStruct, Self::Error> {
        Ok(SerializeStruct {
            map: IndexMap::new(),
        })
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> std::result::Result<Self::SerializeStructVariant, Self::Error> {
        Ok(SerializeStructVariant {
            name: variant.to_string(),
            map: IndexMap::new(),
        })
    }
}
