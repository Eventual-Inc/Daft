use std::{
    fmt,
    hash::{Hash, Hasher},
    io::Write,
    sync::Arc,
};

#[cfg(feature = "python")]
use pyo3::{Bound, Py, PyAny, PyResult, Python, types::PyAnyMethods};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{Error as DeError, Visitor},
    ser::Error as SerError,
};

#[cfg(feature = "python")]
pub fn pickle_dumps(py: Python, obj: &Py<PyAny>) -> PyResult<Vec<u8>> {
    py.import(pyo3::intern!(py, "daft.pickle"))?
        .getattr(pyo3::intern!(py, "dumps"))?
        .call1((obj,))?
        .extract::<Vec<u8>>()
}

#[cfg(feature = "python")]
pub fn pickle_loads(py: Python, bytes: impl AsRef<[u8]>) -> PyResult<Bound<PyAny>> {
    py.import(pyo3::intern!(py, "daft.pickle"))?
        .getattr(pyo3::intern!(py, "loads"))?
        .call1((bytes.as_ref(),))
}

#[cfg(feature = "python")]
pub fn serialize_py_object<S>(obj: &Py<PyAny>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let bytes =
        Python::attach(|py| pickle_dumps(py, obj).map_err(|e| SerError::custom(e.to_string())))?;

    s.serialize_bytes(bytes.as_slice())
}
#[cfg(feature = "python")]
struct PyObjectVisitor;

#[cfg(feature = "python")]
impl<'de> Visitor<'de> for PyObjectVisitor {
    type Value = Py<PyAny>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a byte array containing the pickled partition bytes")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Python::attach(|py| {
            py.import(pyo3::intern!(py, "daft.pickle"))
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
pub fn deserialize_py_object<'de, D>(d: D) -> Result<Arc<Py<PyAny>>, D::Error>
where
    D: Deserializer<'de>,
{
    d.deserialize_bytes(PyObjectVisitor).map(Into::into)
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
            ) -> PyResult<(
                pyo3::Py<pyo3::PyAny>,
                (pyo3::Bound<'py, pyo3::types::PyBytes>,),
            )> {
                use pyo3::{
                    PyErr, PyTypeInfo,
                    exceptions::PyRuntimeError,
                    types::{PyAnyMethods, PyBytes},
                };
                Ok((
                    Self::type_object(py)
                        .getattr(pyo3::intern!(py, "_from_serialized"))?
                        .into(),
                    (PyBytes::new(
                        py,
                        &$crate::bincode::serde::encode_to_vec(
                            &self,
                            $crate::bincode::config::legacy(),
                        )
                        .map_err(|error| {
                            PyErr::new::<PyRuntimeError, _>(format!(
                                "Failed to serialize: {}",
                                error.to_string()
                            ))
                        })?,
                    ),),
                ))
            }

            #[staticmethod]
            pub fn _from_serialized(serialized: &[u8]) -> Self {
                $crate::bincode::serde::decode_from_slice(
                    serialized,
                    $crate::bincode::config::legacy(),
                )
                .unwrap()
                .0
            }
        }
    };
}

#[cfg(feature = "python")]
// This is a Rust wrapper on top of a Python UDF to make it serde-able and hashable
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PyObjectWrapper(
    #[serde(
        serialize_with = "serialize_py_object",
        deserialize_with = "deserialize_py_object"
    )]
    pub Arc<Py<PyAny>>,
);

#[cfg(feature = "python")]
impl PartialEq for PyObjectWrapper {
    fn eq(&self, other: &Self) -> bool {
        Python::attach(|py| self.0.bind(py).eq(other.0.bind(py)).unwrap())
    }
}

#[cfg(feature = "python")]
impl Eq for PyObjectWrapper {}

#[cfg(feature = "python")]
impl Hash for PyObjectWrapper {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let py_obj_hash = Python::attach(|py| self.0.bind(py).hash());
        match py_obj_hash {
            // If Python object is hashable, hash the Python-side hash.
            Ok(py_obj_hash) => py_obj_hash.hash(state),
            // Fall back to hashing the pickled Python object.
            Err(_) => {
                let mut hasher = HashWriter { state };
                bincode::serde::encode_into_std_write(self, &mut hasher, bincode::config::legacy())
                    .expect("Pickling error occurred when computing hash of Pyobject");
            }
        }
    }
}

#[cfg(feature = "python")]
impl From<Arc<Py<PyAny>>> for PyObjectWrapper {
    fn from(value: Arc<Py<PyAny>>) -> Self {
        Self(value)
    }
}

struct HashWriter<'a, H: Hasher> {
    state: &'a mut H,
}

impl<H: Hasher> Write for HashWriter<'_, H> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        buf.hash(self.state);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
