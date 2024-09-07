#[cfg(feature = "python")]
use pyo3::{types::PyAnyMethods, PyObject, Python};

use serde::{de::Error as DeError, de::Visitor, ser::Error as SerError, Deserializer, Serializer};
use std::fmt;
#[cfg(feature = "python")]

pub fn serialize_py_object<S>(obj: &PyObject, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let bytes = Python::with_gil(|py| {
        py.import_bound("daft.pickle")
            .and_then(|m| m.getattr("dumps"))
            .and_then(|f| f.call1((obj,)))
            .and_then(|b| b.extract::<Vec<u8>>())
            .map_err(|e| SerError::custom(e.to_string()))
    })?;
    s.serialize_bytes(bytes.as_slice())
}
#[cfg(feature = "python")]

struct PyObjectVisitor;
#[cfg(feature = "python")]

impl<'de> Visitor<'de> for PyObjectVisitor {
    type Value = PyObject;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a byte array containing the pickled partition bytes")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Python::with_gil(|py| {
            py.import_bound("daft.pickle")
                .and_then(|m| m.getattr("loads"))
                .and_then(|f| Ok(f.call1((v,))?.into()))
                .map_err(|e| DeError::custom(e.to_string()))
        })
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Python::with_gil(|py| {
            py.import_bound("daft.pickle")
                .and_then(|m| m.getattr("loads"))
                .and_then(|f| Ok(f.call1((v,))?.into()))
                .map_err(|e| DeError::custom(e.to_string()))
        })
    }
}

#[cfg(feature = "python")]
pub fn deserialize_py_object<'de, D>(d: D) -> Result<PyObject, D::Error>
where
    D: Deserializer<'de>,
{
    d.deserialize_bytes(PyObjectVisitor)
}

#[macro_export]
macro_rules! impl_bincode_py_state_serialization {
    ($ty:ty) => {
        #[cfg(feature = "python")]
        #[pymethods]
        impl $ty {
            pub fn __reduce__(&self, py: Python) -> PyResult<(PyObject, PyObject)> {
                use pyo3::{
                    types::{PyAnyMethods, PyBytes},
                    PyTypeInfo, ToPyObject,
                };
                Ok((
                    Self::type_object_bound(py)
                        .getattr("_from_serialized")?
                        .into(),
                    PyBytes::new_bound(py, &$crate::bincode::serialize(&self).unwrap()).into(),
                ))
            }

            #[staticmethod]
            pub fn _from_serialized(serialized: &[u8]) -> Self {
                $crate::bincode::deserialize(serialized).unwrap()
            }
        }
    };
}
