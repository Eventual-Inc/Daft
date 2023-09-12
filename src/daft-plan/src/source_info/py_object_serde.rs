use pyo3::{PyObject, Python, ToPyObject};
use serde::{
    de::Error as DeError, de::Visitor, ser::Error as SerError, Deserializer, Serialize, Serializer,
};
use std::fmt;

pub(super) fn serialize_py_object<S>(obj: &PyObject, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let bytes = Python::with_gil(|py| {
        py.import(pyo3::intern!(py, "ray.cloudpickle"))
            .or_else(|_| py.import(pyo3::intern!(py, "pickle")))
            .and_then(|m| m.getattr(pyo3::intern!(py, "dumps")))
            .and_then(|f| f.call1((obj,)))
            .and_then(|b| b.extract::<Vec<u8>>())
            .map_err(|e| SerError::custom(e.to_string()))
    })?;
    s.serialize_bytes(bytes.as_slice())
}

struct PyObjectVisitor;

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
            py.import(pyo3::intern!(py, "ray.cloudpickle"))
                .or_else(|_| py.import(pyo3::intern!(py, "pickle")))
                .and_then(|m| m.getattr(pyo3::intern!(py, "loads")))
                .and_then(|f| Ok(f.call1((v,))?.to_object(py)))
                .map_err(|e| DeError::custom(e.to_string()))
        })
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Python::with_gil(|py| {
            py.import(pyo3::intern!(py, "ray.cloudpickle"))
                .or_else(|_| py.import(pyo3::intern!(py, "pickle")))
                .and_then(|m| m.getattr(pyo3::intern!(py, "loads")))
                .and_then(|f| Ok(f.call1((v,))?.to_object(py)))
                .map_err(|e| DeError::custom(e.to_string()))
        })
    }
}

#[cfg(feature = "python")]
pub(super) fn deserialize_py_object<'de, D>(d: D) -> Result<PyObject, D::Error>
where
    D: Deserializer<'de>,
{
    d.deserialize_bytes(PyObjectVisitor)
}

#[derive(Serialize)]
#[serde(transparent)]
#[cfg(feature = "python")]
struct PyObjSerdeWrapper<'a>(#[serde(serialize_with = "serialize_py_object")] &'a PyObject);

#[cfg(feature = "python")]
pub(super) fn serialize_py_object_optional<S>(
    obj: &Option<PyObject>,
    s: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match obj {
        Some(obj) => s.serialize_some(&PyObjSerdeWrapper(obj)),
        None => s.serialize_none(),
    }
}

struct OptPyObjectVisitor;

#[cfg(feature = "python")]
impl<'de> Visitor<'de> for OptPyObjectVisitor {
    type Value = Option<PyObject>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a byte array containing the pickled partition bytes")
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserialize_py_object(deserializer).map(Some)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Ok(None)
    }
}

#[cfg(feature = "python")]
pub(super) fn deserialize_py_object_optional<'de, D>(d: D) -> Result<Option<PyObject>, D::Error>
where
    D: Deserializer<'de>,
{
    d.deserialize_option(OptPyObjectVisitor)
}
