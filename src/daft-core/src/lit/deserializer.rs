use std::fmt::{Display, Formatter};

use common_error::DaftError;
use indexmap::IndexMap;
use serde::{
    Deserializer,
    de::{self, Error},
    forward_to_deserialize_any,
};

use super::Literal;
use crate::series::Series;

#[derive(Debug)]
pub struct LitError {
    message: String,
}

impl From<LitError> for DaftError {
    fn from(err: LitError) -> Self {
        Self::ValueError(err.message)
    }
}

impl de::Error for LitError {
    fn custom<T: Display>(msg: T) -> Self {
        Self {
            message: msg.to_string(),
        }
    }
}

impl Display for LitError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for LitError {}

struct SeriesDeserializer<'de> {
    values: &'de Series,
    index: usize,
}

impl<'de> SeriesDeserializer<'de> {
    fn new(values: &'de Series) -> Self {
        SeriesDeserializer { values, index: 0 }
    }
}

impl<'de> serde::de::SeqAccess<'de> for SeriesDeserializer<'de> {
    type Error = LitError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: serde::de::DeserializeSeed<'de>,
    {
        if self.index >= self.values.len() {
            return Ok(None);
        }

        let value = self.values.get_lit(self.index);
        self.index += 1;

        let deserializer = OwnedLiteralDeserializer { lit: value };
        seed.deserialize(deserializer).map(Some)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.values.len() - self.index)
    }
}

struct StructDeserializer<'de> {
    fields: &'de IndexMap<String, Literal>,
    iter: indexmap::map::Iter<'de, String, Literal>,
    value: Option<&'de Literal>,
}

impl<'de> StructDeserializer<'de> {
    fn new(fields: &'de IndexMap<String, Literal>) -> Self {
        StructDeserializer {
            fields,
            iter: fields.iter(),
            value: None,
        }
    }
}

impl<'de> serde::de::MapAccess<'de> for StructDeserializer<'de> {
    type Error = LitError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: serde::de::DeserializeSeed<'de>,
    {
        match self.iter.next() {
            Some((key, value)) => {
                // Save the value for the next call to next_value_seed
                self.value = Some(value);

                // Deserialize key name as a string
                let key_deserializer = StringDeserializer(key);
                seed.deserialize(key_deserializer).map(Some)
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        match self.value.take() {
            Some(value) => {
                // Deserialize the stored value
                seed.deserialize(LiteralDeserializer { lit: value })
            }
            None => Err(LitError::custom("Value is missing for struct field")),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.fields.len())
    }
}

// String deserializer for field names
struct StringDeserializer<'de>(&'de str);

impl<'de> Deserializer<'de> for StringDeserializer<'de> {
    type Error = LitError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_str(self.0)
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

pub struct LiteralDeserializer<'de> {
    pub(super) lit: &'de Literal,
}

// Owned deserializer
pub struct OwnedLiteralDeserializer {
    lit: Literal,
}

// Simple variant access that only supports unit variants
pub struct UnitOnlyVariantAccess;

impl<'de> serde::de::VariantAccess<'de> for UnitOnlyVariantAccess {
    type Error = LitError;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, _seed: T) -> Result<T::Value, Self::Error>
    where
        T: serde::de::DeserializeSeed<'de>,
    {
        Err(LitError::custom("Unexpected newtype variant"))
    }

    fn tuple_variant<V>(self, _len: usize, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(LitError::custom("Unexpected tuple variant"))
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        Err(LitError::custom("Unexpected struct variant"))
    }
}

pub struct StringEnumAccess<'de> {
    variant: &'de str,
}

impl<'de> serde::de::EnumAccess<'de> for StringEnumAccess<'de> {
    type Error = LitError;
    type Variant = UnitOnlyVariantAccess;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        let variant = StringDeserializer(self.variant);
        let val = seed.deserialize(variant)?;
        Ok((val, UnitOnlyVariantAccess))
    }
}

pub struct VariantAccess<'de> {
    value: &'de Literal,
}

impl<'de> serde::de::VariantAccess<'de> for VariantAccess<'de> {
    type Error = LitError;

    fn unit_variant(self) -> Result<(), Self::Error> {
        match self.value {
            Literal::Null => Ok(()),
            _ => Err(LitError::custom("Expected null for unit variant")),
        }
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: serde::de::DeserializeSeed<'de>,
    {
        seed.deserialize(LiteralDeserializer { lit: self.value })
    }

    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        match self.value {
            Literal::List(series) => visitor.visit_seq(SeriesDeserializer::new(series)),
            _ => LiteralDeserializer { lit: self.value }.deserialize_seq(visitor),
        }
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        match self.value {
            Literal::Struct(fields) => visitor.visit_map(StructDeserializer::new(fields)),
            _ => Err(LitError::custom("Expected struct for struct variant")),
        }
    }
}

// For struct enum variants (handles all variant types)
pub struct StructEnumAccess<'de> {
    variant: &'de str,
    value: &'de Literal,
}

impl<'de> serde::de::EnumAccess<'de> for StructEnumAccess<'de> {
    type Error = LitError;
    type Variant = VariantAccess<'de>;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        let variant = StringDeserializer(self.variant);
        let val = seed.deserialize(variant)?;
        Ok((val, VariantAccess { value: self.value }))
    }
}

impl<'de> Deserializer<'de> for OwnedLiteralDeserializer {
    type Error = LitError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, LitError>
    where
        V: serde::de::Visitor<'de>,
    {
        match self.lit {
            Literal::Null => visitor.visit_none(),
            Literal::Boolean(b) => visitor.visit_bool(b),
            Literal::Utf8(s) => visitor.visit_string(s),
            Literal::Binary(items) => visitor.visit_bytes(&items),
            Literal::Int8(i8) => visitor.visit_i8(i8),
            Literal::UInt8(u8) => visitor.visit_u8(u8),
            Literal::Int16(i16) => visitor.visit_i16(i16),
            Literal::UInt16(u16) => visitor.visit_u16(u16),
            Literal::Int32(i32) => visitor.visit_i32(i32),
            Literal::UInt32(u32) => visitor.visit_u32(u32),
            Literal::Int64(i64) => visitor.visit_i64(i64),
            Literal::UInt64(u64) => visitor.visit_u64(u64),
            Literal::Float32(f32) => visitor.visit_f32(f32),
            Literal::Float64(f64) => visitor.visit_f64(f64),
            Literal::Timestamp(..) => Err(LitError::custom("Not implemented: Timestamp")),
            Literal::Date(_) => Err(LitError::custom("Not implemented: Date")),
            Literal::Time(..) => Err(LitError::custom("Not implemented: Time")),
            Literal::Duration(..) => Err(LitError::custom("Not implemented: Duration")),
            Literal::Interval(..) => Err(LitError::custom("Not implemented: Interval")),
            Literal::Decimal(_, _, _) => Err(LitError::custom("Not implemented: Decimal")),
            Literal::List(_) => Err(LitError::custom("Not implemented: List")),
            #[cfg(feature = "python")]
            Literal::Python(_) => Err(LitError::custom("Not implemented: Python")),
            Literal::Struct(_) => Err(LitError::custom("Not implemented: Struct")),
            Literal::File(_) => Err(LitError::custom("Not implemented: File")),
            Literal::Tensor { .. } => Err(LitError::custom("Not implemented: Tensor")),
            Literal::SparseTensor { .. } => Err(LitError::custom("Not implemented: SparseTensor")),
            Literal::Embedding { .. } => Err(LitError::custom("Not implemented: Embedding")),
            Literal::Map { .. } => Err(LitError::custom("Not implemented: Map")),
            Literal::Image(_) => Err(LitError::custom("Not implemented: Image")),
            Literal::Extension(_) => Err(LitError::custom("Not implemented: Extension")),
        }
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

impl<'de> Deserializer<'de> for LiteralDeserializer<'de> {
    type Error = LitError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, LitError>
    where
        V: serde::de::Visitor<'de>,
    {
        match self.lit {
            Literal::Null => visitor.visit_none(),
            Literal::Boolean(b) => visitor.visit_bool(*b),
            Literal::Utf8(s) => visitor.visit_str(s),
            Literal::Binary(items) => visitor.visit_bytes(items),
            Literal::Int8(i8) => visitor.visit_i8(*i8),
            Literal::UInt8(u8) => visitor.visit_u8(*u8),
            Literal::Int16(i16) => visitor.visit_i16(*i16),
            Literal::UInt16(u16) => visitor.visit_u16(*u16),
            Literal::Int32(i32) => visitor.visit_i32(*i32),
            Literal::UInt32(u32) => visitor.visit_u32(*u32),
            Literal::Int64(i64) => visitor.visit_i64(*i64),
            Literal::UInt64(u64) => visitor.visit_u64(*u64),
            Literal::Float32(f32) => visitor.visit_f32(*f32),
            Literal::Float64(f64) => visitor.visit_f64(*f64),
            Literal::Timestamp(..) => Err(LitError::custom("Not implemented: Timestamp")),
            Literal::Date(_) => Err(LitError::custom("Not implemented: Date")),
            Literal::Time(..) => Err(LitError::custom("Not implemented: Time")),
            Literal::Duration(..) => Err(LitError::custom("Not implemented: Duration")),
            Literal::Interval(..) => Err(LitError::custom("Not implemented: Interval")),
            Literal::Decimal(_, _, _) => Err(LitError::custom("Not implemented: Decimal")),
            Literal::List(s) => visitor.visit_seq(SeriesDeserializer::new(s)),
            #[cfg(feature = "python")]
            Literal::Python(_) => Err(LitError::custom("Not implemented: Python")),
            Literal::File(_) => Err(LitError::custom("Not implemented: File")),
            Literal::Struct(v) => visitor.visit_map(StructDeserializer::new(v)),
            Literal::Tensor { .. } => Err(LitError::custom("Not implemented: Tensor")),
            Literal::SparseTensor { .. } => Err(LitError::custom("Not implemented: SparseTensor")),
            Literal::Embedding { .. } => Err(LitError::custom("Not implemented: Embedding")),
            Literal::Map { .. } => Err(LitError::custom("Not implemented: Map")),
            Literal::Image(_) => Err(LitError::custom("Not implemented: Image")),
            Literal::Extension(_) => Err(LitError::custom("Not implemented: Extension")),
        }
    }
    // Override option deserialization
    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        match self.lit {
            Literal::Null => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        match self.lit {
            // Handle string representation (unit variants like "All")
            Literal::Utf8(s) => visitor.visit_enum(StringEnumAccess { variant: s }),

            // Handle numeric representation (discriminants)
            Literal::Int8(i) => visitor.visit_u8(*i as u8),
            Literal::Int16(i) => visitor.visit_u16(*i as u16),
            Literal::Int32(i) => visitor.visit_u32(*i as u32),
            Literal::Int64(i) => visitor.visit_u64(*i as u64),
            Literal::UInt8(i) => visitor.visit_u8(*i),
            Literal::UInt16(i) => visitor.visit_u16(*i),
            Literal::UInt32(i) => visitor.visit_u32(*i),
            Literal::UInt64(i) => visitor.visit_u64(*i),

            // Handle struct representation (like {"Variant": value} or {"Variant": {"field": value}})
            Literal::Struct(fields) if fields.len() == 1 => {
                let (key, value) = fields.iter().next().unwrap();
                visitor.visit_enum(StructEnumAccess {
                    variant: key,
                    value,
                })
            }

            _ => Err(LitError::custom(format!(
                "Invalid enum type: {:?}",
                self.lit.get_type()
            ))),
        }
    }
    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf unit unit_struct newtype_struct seq tuple
        tuple_struct map struct identifier ignored_any
    }
}
