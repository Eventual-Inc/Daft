use daft_core::{prelude::Field, series::Series};
use indexmap::IndexMap;
use serde::{
    de::{self},
    forward_to_deserialize_any,
    ser::Error,
    Deserializer,
};

use super::{serializer::LitError, LiteralValue};

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

        let value = LiteralValue::get_from_series(self.values, self.index)
            .map_err(|e| LitError::custom(e.to_string()))?;

        self.index += 1;

        let deserializer = OwnedLiteralValueDeserializer { lit: value };
        seed.deserialize(deserializer).map(Some)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.values.len() - self.index)
    }
}

struct StructDeserializer<'de> {
    fields: &'de IndexMap<Field, LiteralValue>,
    iter: indexmap::map::Iter<'de, Field, LiteralValue>,
    value: Option<&'de LiteralValue>,
}

impl<'de> StructDeserializer<'de> {
    fn new(fields: &'de IndexMap<Field, LiteralValue>) -> Self {
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
                let key_deserializer = StringDeserializer(&key.name);
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
                seed.deserialize(LiteralValueDeserializer { lit: value })
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

pub struct LiteralValueDeserializer<'de> {
    pub(super) lit: &'de LiteralValue,
}

// Owned deserializer
pub struct OwnedLiteralValueDeserializer {
    lit: LiteralValue,
}

// Helper struct for enum deserialization
pub struct EnumDeserializer<'de> {
    variant: &'de str,
}

impl<'de> serde::de::EnumAccess<'de> for EnumDeserializer<'de> {
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
    value: &'de LiteralValue,
}

impl<'de> serde::de::VariantAccess<'de> for VariantAccess<'de> {
    type Error = LitError;

    fn unit_variant(self) -> Result<(), Self::Error> {
        match self.value {
            LiteralValue::Null => Ok(()),
            _ => Err(LitError::custom("Expected null for unit variant")),
        }
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: serde::de::DeserializeSeed<'de>,
    {
        seed.deserialize(LiteralValueDeserializer { lit: self.value })
    }

    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        match self.value {
            LiteralValue::Series(series) => visitor.visit_seq(SeriesDeserializer::new(series)),
            _ => LiteralValueDeserializer { lit: self.value }.deserialize_seq(visitor),
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
            LiteralValue::Struct(fields) => visitor.visit_map(StructDeserializer::new(fields)),
            _ => Err(LitError::custom("Expected struct for struct variant")),
        }
    }
}

// For struct enum variants (handles all variant types)
pub struct StructEnumAccess<'de> {
    variant: &'de str,
    value: &'de LiteralValue,
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

impl<'de> Deserializer<'de> for OwnedLiteralValueDeserializer {
    type Error = LitError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, LitError>
    where
        V: serde::de::Visitor<'de>,
    {
        match self.lit {
            LiteralValue::Null => visitor.visit_none(),
            LiteralValue::Boolean(b) => visitor.visit_bool(b),
            LiteralValue::Utf8(s) => visitor.visit_string(s),
            LiteralValue::Binary(items) => visitor.visit_bytes(&items),
            LiteralValue::FixedSizeBinary(items, _) => visitor.visit_bytes(&items),
            LiteralValue::Int8(i8) => visitor.visit_i8(i8),
            LiteralValue::UInt8(u8) => visitor.visit_u8(u8),
            LiteralValue::Int16(i16) => visitor.visit_i16(i16),
            LiteralValue::UInt16(u16) => visitor.visit_u16(u16),
            LiteralValue::Int32(i32) => visitor.visit_i32(i32),
            LiteralValue::UInt32(u32) => visitor.visit_u32(u32),
            LiteralValue::Int64(i64) => visitor.visit_i64(i64),
            LiteralValue::UInt64(u64) => visitor.visit_u64(u64),
            LiteralValue::Float64(f64) => visitor.visit_f64(f64),
            LiteralValue::Timestamp(..) => Err(LitError::custom("Not implemented: Timestamp")),
            LiteralValue::Date(_) => Err(LitError::custom("Not implemented: Date")),
            LiteralValue::Time(..) => Err(LitError::custom("Not implemented: Time")),
            LiteralValue::Duration(..) => Err(LitError::custom("Not implemented: Duration")),
            LiteralValue::Interval(..) => Err(LitError::custom("Not implemented: Interval")),
            LiteralValue::Decimal(_, _, _) => Err(LitError::custom("Not implemented: Decimal")),
            LiteralValue::Series(_) => Err(LitError::custom("Not implemented: Series")),
            #[cfg(feature = "python")]
            LiteralValue::Python(_) => Err(LitError::custom("Not implemented: Python")),
            LiteralValue::Struct(_) => Err(LitError::custom("Not implemented: Struct")),
        }
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

impl<'de> Deserializer<'de> for LiteralValueDeserializer<'de> {
    type Error = LitError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, LitError>
    where
        V: serde::de::Visitor<'de>,
    {
        match self.lit {
            LiteralValue::Null => visitor.visit_none(),
            LiteralValue::Boolean(b) => visitor.visit_bool(*b),
            LiteralValue::Utf8(s) => visitor.visit_str(s),
            LiteralValue::Binary(items) => visitor.visit_bytes(items),
            LiteralValue::FixedSizeBinary(items, _) => visitor.visit_bytes(items),
            LiteralValue::Int8(i8) => visitor.visit_i8(*i8),
            LiteralValue::UInt8(u8) => visitor.visit_u8(*u8),
            LiteralValue::Int16(i16) => visitor.visit_i16(*i16),
            LiteralValue::UInt16(u16) => visitor.visit_u16(*u16),
            LiteralValue::Int32(i32) => visitor.visit_i32(*i32),
            LiteralValue::UInt32(u32) => visitor.visit_u32(*u32),
            LiteralValue::Int64(i64) => visitor.visit_i64(*i64),
            LiteralValue::UInt64(u64) => visitor.visit_u64(*u64),
            LiteralValue::Float64(f64) => visitor.visit_f64(*f64),
            LiteralValue::Timestamp(..) => Err(LitError::custom("Not implemented: Timestamp")),
            LiteralValue::Date(_) => Err(LitError::custom("Not implemented: Date")),
            LiteralValue::Time(..) => Err(LitError::custom("Not implemented: Time")),
            LiteralValue::Duration(..) => Err(LitError::custom("Not implemented: Duration")),
            LiteralValue::Interval(..) => Err(LitError::custom("Not implemented: Interval")),
            LiteralValue::Decimal(_, _, _) => Err(LitError::custom("Not implemented: Decimal")),
            LiteralValue::Series(s) => visitor.visit_seq(SeriesDeserializer::new(s)),
            #[cfg(feature = "python")]
            LiteralValue::Python(_) => Err(LitError::custom("Not implemented: Python")),
            LiteralValue::Struct(v) => visitor.visit_map(StructDeserializer::new(v)),
        }
    }
    // Override option deserialization
    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        match self.lit {
            LiteralValue::Null => visitor.visit_none(),
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
            LiteralValue::Utf8(s) => visitor.visit_enum(StringEnumAccess { variant: s }),

            // Handle numeric representation (discriminants)
            LiteralValue::Int8(i) => visitor.visit_u8(*i as u8),
            LiteralValue::Int16(i) => visitor.visit_u16(*i as u16),
            LiteralValue::Int32(i) => visitor.visit_u32(*i as u32),
            LiteralValue::Int64(i) => visitor.visit_u64(*i as u64),
            LiteralValue::UInt8(i) => visitor.visit_u8(*i),
            LiteralValue::UInt16(i) => visitor.visit_u16(*i),
            LiteralValue::UInt32(i) => visitor.visit_u32(*i),
            LiteralValue::UInt64(i) => visitor.visit_u64(*i),

            // Handle struct representation (like {"Variant": value} or {"Variant": {"field": value}})
            LiteralValue::Struct(fields) if fields.len() == 1 => {
                let (field, value) = fields.iter().next().unwrap();
                visitor.visit_enum(StructEnumAccess {
                    variant: &field.name,
                    value,
                })
            }

            _ => Err(LitError::custom(format!(
                "Invalid enum value: {:?}",
                self.lit
            ))),
        }
    }
    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf unit unit_struct newtype_struct seq tuple
        tuple_struct map struct identifier ignored_any
    }
}
