use std::fmt;

#[cfg(feature = "python")]
use pyo3::{types::PyAnyMethods, PyObject, Python};
use serde::{
    de::{Error as DeError, Visitor},
    ser::Error as SerError,
    Deserializer, Serializer,
};

#[cfg(feature = "python")]
pub fn serialize_py_object<S>(obj: &PyObject, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let bytes = Python::with_gil(|py| {
        py.import_bound(pyo3::intern!(py, "daft.pickle"))
            .and_then(|m| m.getattr(pyo3::intern!(py, "dumps")))
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
            py.import_bound(pyo3::intern!(py, "daft.pickle"))
                .and_then(|m| m.getattr(pyo3::intern!(py, "loads")))
                .and_then(|f| Ok(f.call1((v,))?.into()))
                .map_err(|e| DeError::custom(e.to_string()))
        })
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        self.visit_bytes(&v)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut v: Vec<u8> = Vec::with_capacity(seq.size_hint().unwrap_or_default());
        while let Some(elem) = seq.next_element()? {
            v.push(elem);
        }

        self.visit_bytes(&v)
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
            pub fn __reduce__<'py>(
                &self,
                py: Python<'py>,
            ) -> PyResult<(PyObject, (pyo3::Bound<'py, pyo3::types::PyBytes>,))> {
                use pyo3::{
                    types::{PyAnyMethods, PyBytes},
                    PyTypeInfo, ToPyObject,
                };
                Ok((
                    Self::type_object_bound(py)
                        .getattr(pyo3::intern!(py, "_from_serialized"))?
                        .into(),
                    (PyBytes::new_bound(
                        py,
                        &$crate::bincode::serialize(&self).unwrap(),
                    ),),
                ))
            }

            #[staticmethod]
            pub fn _from_serialized(serialized: &[u8]) -> Self {
                $crate::bincode::deserialize(serialized).unwrap()
            }
        }
    };
}
