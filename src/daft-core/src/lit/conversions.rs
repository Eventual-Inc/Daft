use common_error::{DaftError, DaftResult};
use common_io_config::IOConfig;
#[cfg(feature = "python")]
use pyo3::{FromPyObject, Py, PyAny, PyClass, Python};

use super::{FromLiteral, Literal, deserializer::LiteralDeserializer};
#[cfg(feature = "python")]
use crate::python::{PyDataType, PyTimeUnit};
use crate::{
    datatypes::IntervalValue,
    file::FileReference,
    prelude::{CountMode, DataType, ImageFormat, ImageMode, ImageProperty, TimeUnit},
    series::Series,
};

macro_rules! impl_literal {
    ($TYPE:ty, $SCALAR:ident) => {
        impl From<$TYPE> for Literal {
            fn from(value: $TYPE) -> Self {
                Literal::$SCALAR(value)
            }
        }
    };
    ($TYPE:ty, $SCALAR:ident, $TRANSFORM:expr) => {
        impl From<$TYPE> for Literal {
            fn from(value: $TYPE) -> Self {
                Literal::$SCALAR($TRANSFORM(value))
            }
        }
    };
}

impl_literal!(bool, Boolean);
impl_literal!(i8, Int8);
impl_literal!(u8, UInt8);
impl_literal!(i16, Int16);
impl_literal!(u16, UInt16);
impl_literal!(i32, Int32);
impl_literal!(u32, UInt32);
impl_literal!(i64, Int64);
impl_literal!(u64, UInt64);
impl_literal!(f32, Float32);
impl_literal!(f64, Float64);
impl_literal!(IntervalValue, Interval);
impl_literal!(String, Utf8);
impl_literal!(Series, List);
impl_literal!(FileReference, File);

impl_literal!(&'_ str, Utf8, |s: &'_ str| s.to_owned());
impl_literal!(&'_ [u8], Binary, |s: &'_ [u8]| s.to_vec());
#[cfg(feature = "python")]
impl_literal!(Py<PyAny>, Python, |s: Py<PyAny>| {
    common_py_serde::PyObjectWrapper(std::sync::Arc::new(s))
});

impl<T> From<Option<T>> for Literal
where
    T: Into<Self>,
{
    fn from(value: Option<T>) -> Self {
        match value {
            Some(v) => v.into(),
            None => Self::Null,
        }
    }
}

macro_rules! impl_strict_fromliteral {
    ($TYPE:ty, $SCALAR:ident) => {
        impl FromLiteral for $TYPE {
            fn try_from_literal(lit: &Literal) -> DaftResult<Self> {
                if let Literal::$SCALAR(v) = lit {
                    Ok(v.clone())
                } else {
                    Err(DaftError::TypeError(format!(
                        "Expected {} literal, received: {}",
                        stringify!($TYPE),
                        lit
                    )))
                }
            }
        }
    };
}

macro_rules! impl_int_fromliteral {
    ($TYPE:ty) => {
        impl FromLiteral for $TYPE {
            fn try_from_literal(lit: &Literal) -> DaftResult<Self> {
                let casted = match lit {
                    Literal::Int8(v) => num_traits::cast(*v),
                    Literal::UInt8(v) => num_traits::cast(*v),
                    Literal::Int16(v) => num_traits::cast(*v),
                    Literal::UInt16(v) => num_traits::cast(*v),
                    Literal::Int32(v) => num_traits::cast(*v),
                    Literal::UInt32(v) => num_traits::cast(*v),
                    Literal::Int64(v) => num_traits::cast(*v),
                    Literal::UInt64(v) => num_traits::cast(*v),
                    Literal::Float32(v) => {
                        if v.fract() == 0.0 {
                            num_traits::cast(*v)
                        } else {
                            None
                        }
                    }
                    Literal::Float64(v) => {
                        if v.fract() == 0.0 {
                            num_traits::cast(*v)
                        } else {
                            None
                        }
                    }
                    _ => {
                        return Err(DaftError::ValueError(format!(
                            "Expected integer number, received: {lit}"
                        )));
                    }
                };

                casted.ok_or_else(|| {
                    DaftError::ValueError(format!(
                        "failed to cast {} to type {}",
                        lit,
                        std::any::type_name::<$TYPE>()
                    ))
                })
            }
        }
    };
}

macro_rules! impl_float_fromliteral {
    ($TYPE:ty) => {
        impl FromLiteral for $TYPE {
            fn try_from_literal(lit: &Literal) -> DaftResult<Self> {
                let casted = match lit {
                    Literal::Int8(v) => num_traits::cast(*v),
                    Literal::UInt8(v) => num_traits::cast(*v),
                    Literal::Int16(v) => num_traits::cast(*v),
                    Literal::UInt16(v) => num_traits::cast(*v),
                    Literal::Int32(v) => num_traits::cast(*v),
                    Literal::UInt32(v) => num_traits::cast(*v),
                    Literal::Int64(v) => num_traits::cast(*v),
                    Literal::UInt64(v) => num_traits::cast(*v),
                    Literal::Float32(v) => num_traits::cast(*v),
                    Literal::Float64(v) => num_traits::cast(*v),
                    _ => {
                        return Err(DaftError::ValueError(format!(
                            "Expected floating point number, received: {lit}"
                        )));
                    }
                };

                casted.ok_or_else(|| {
                    DaftError::ValueError(format!(
                        "failed to cast {} to type {}",
                        lit,
                        std::any::type_name::<$TYPE>()
                    ))
                })
            }
        }
    };
}

#[cfg(feature = "python")]
fn try_extract_py_lit<T>(value: &Literal) -> Option<T>
where
    T: Clone + PyClass + for<'a, 'py> FromPyObject<'a, 'py>,
{
    if let Literal::Python(py_value) = value {
        Python::attach(|py| py_value.0.extract::<T>(py).ok())
    } else {
        None
    }
}

macro_rules! impl_pyobj_fromliteral {
    ($TYPE:ty, $PY_TYPE:ty) => {
        impl FromLiteral for $TYPE {
            fn try_from_literal(lit: &Literal) -> DaftResult<Self> {
                use serde::Deserialize;

                #[cfg(feature = "python")]
                if let Some(py_val) = try_extract_py_lit::<$PY_TYPE>(lit) {
                    return Ok(py_val.into());
                }

                let deserializer = LiteralDeserializer { lit };
                Ok(Deserialize::deserialize(deserializer)?)
            }
        }
    };
}

impl<T> FromLiteral for Option<T>
where
    T: FromLiteral,
{
    fn try_from_literal(lit: &Literal) -> DaftResult<Self> {
        if *lit == Literal::Null {
            Ok(None)
        } else {
            T::try_from_literal(lit).map(Some)
        }
    }
}

impl_strict_fromliteral!(String, Utf8);
impl_strict_fromliteral!(bool, Boolean);
impl_strict_fromliteral!(FileReference, File);
impl_int_fromliteral!(i8);
impl_int_fromliteral!(u8);
impl_int_fromliteral!(i16);
impl_int_fromliteral!(u16);
impl_int_fromliteral!(i32);
impl_int_fromliteral!(u32);
impl_int_fromliteral!(i64);
impl_int_fromliteral!(u64);
impl_int_fromliteral!(usize);
impl_int_fromliteral!(isize);
impl_float_fromliteral!(f32);
impl_float_fromliteral!(f64);
impl_pyobj_fromliteral!(IOConfig, common_io_config::python::IOConfig);
impl_pyobj_fromliteral!(ImageMode, ImageMode);
impl_pyobj_fromliteral!(ImageProperty, ImageProperty);
impl_pyobj_fromliteral!(CountMode, CountMode);
impl_pyobj_fromliteral!(TimeUnit, PyTimeUnit);
impl_pyobj_fromliteral!(DataType, PyDataType);
impl_pyobj_fromliteral!(ImageFormat, ImageFormat);
