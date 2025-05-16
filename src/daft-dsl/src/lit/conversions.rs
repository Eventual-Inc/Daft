use common_error::{DaftError, DaftResult};
use common_io_config::IOConfig;
use daft_core::{
    datatypes::IntervalValue,
    prelude::{CountMode, ImageFormat, ImageMode},
    series::Series,
};
use num_traits::cast;
use serde::de::DeserializeOwned;

use super::{deserializer, serializer, FromLiteral, Literal, LiteralValue};
#[cfg(feature = "python")]
use crate::pyobj_serde::PyObjectWrapper;

impl Literal for IntervalValue {
    fn literal_value(self) -> LiteralValue {
        LiteralValue::Interval(self)
    }
}

impl Literal for String {
    fn literal_value(self) -> LiteralValue {
        LiteralValue::Utf8(self)
    }
}

impl FromLiteral for String {
    fn try_from_literal(lit: &LiteralValue) -> DaftResult<Self> {
        match lit {
            LiteralValue::Utf8(s) => Ok(s.clone()),
            _ => Err(DaftError::TypeError(format!(
                "Cannot convert {:?} to String",
                lit
            ))),
        }
    }
}

impl Literal for &'_ str {
    fn literal_value(self) -> LiteralValue {
        LiteralValue::Utf8(self.to_owned())
    }
}

impl Literal for &'_ [u8] {
    fn literal_value(self) -> LiteralValue {
        LiteralValue::Binary(self.to_vec())
    }
}

impl Literal for Series {
    fn literal_value(self) -> LiteralValue {
        LiteralValue::Series(self)
    }
}

#[cfg(feature = "python")]
impl Literal for pyo3::PyObject {
    fn literal_value(self) -> LiteralValue {
        LiteralValue::Python(PyObjectWrapper(std::sync::Arc::new(self)))
    }
}

impl<T> Literal for Option<T>
where
    T: Literal,
{
    fn literal_value(self) -> LiteralValue {
        match self {
            Some(val) => val.literal_value(),
            None => LiteralValue::Null,
        }
    }
}

macro_rules! make_literal {
    ($TYPE:ty, $SCALAR:ident) => {
        impl Literal for $TYPE {
            fn literal_value(self) -> LiteralValue {
                LiteralValue::$SCALAR(self)
            }
        }
        impl FromLiteral for $TYPE {
            fn try_from_literal(lit: &LiteralValue) -> DaftResult<Self> {
                match lit {
                    LiteralValue::$SCALAR(v) => Ok(*v),
                    _ => Err(DaftError::TypeError(format!(
                        "Expected {} literal",
                        stringify!($TYPE)
                    ))),
                }
            }
        }
    };
}
make_literal!(bool, Boolean);
make_literal!(i8, Int8);
make_literal!(u8, UInt8);
make_literal!(i16, Int16);
make_literal!(u16, UInt16);
make_literal!(i32, Int32);
make_literal!(u32, UInt32);
make_literal!(i64, Int64);
make_literal!(u64, UInt64);
make_literal!(f64, Float64);

impl FromLiteral for usize {
    fn try_from_literal(lit: &LiteralValue) -> DaftResult<Self> {
        match lit {
            LiteralValue::Int8(i8) => cast(*i8),
            LiteralValue::UInt8(u8) => cast(*u8),
            LiteralValue::Int16(i16) => cast(*i16),
            LiteralValue::UInt16(u16) => cast(*u16),
            LiteralValue::Int32(i32) => cast(*i32),
            LiteralValue::UInt32(u32) => cast(*u32),
            LiteralValue::Int64(i64) => cast(*i64),
            LiteralValue::UInt64(u64) => cast(*u64),
            _ => None,
        }
        .ok_or_else(|| DaftError::ValueError("Unsupported literal type".to_string()))
    }
}

/// Marker trait to allowlist what can be converted to a literal via serde
trait SerializableLiteral: serde::Serialize {}

/// Marker trait to allowlist what can be converted from a literal via serde
trait DeserializableLiteral: DeserializeOwned {}

impl SerializableLiteral for IOConfig {}
impl DeserializableLiteral for IOConfig {}
impl SerializableLiteral for ImageMode {}
impl DeserializableLiteral for ImageMode {}
impl SerializableLiteral for ImageFormat {}
impl DeserializableLiteral for ImageFormat {}
impl SerializableLiteral for CountMode {}
impl DeserializableLiteral for CountMode {}

impl<D> FromLiteral for D
where
    D: DeserializableLiteral,
{
    fn try_from_literal(lit: &LiteralValue) -> DaftResult<Self> {
        let deserializer = deserializer::LiteralValueDeserializer { lit };
        D::deserialize(deserializer).map_err(|e| e.into())
    }
}

impl<S> Literal for S
where
    S: SerializableLiteral,
{
    fn literal_value(self) -> LiteralValue {
        self.serialize(serializer::LiteralValueSerializer)
            .expect("serialization failed")
    }
}
