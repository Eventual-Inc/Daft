use std::hash::{Hash, Hasher};

use pyo3::{
    types::{PyBytes, PyModule},
    PyObject, Python, ToPyObject,
};
use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};

// A curried Python function that takes as input a list of Series objects for execution
// This is a Rust wrapper on top of a daft.udf.PartialUDF Python object instance
#[derive(Debug, Clone)]
pub struct PartialUDF(pub PyObject);

impl Serialize for PartialUDF {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Python::with_gil(|py| {
            let serde_module = PyModule::import(py, pyo3::intern!(py, "daft.pickle")).unwrap();
            let dumps = serde_module.getattr(pyo3::intern!(py, "dumps")).unwrap();
            let pybytes = dumps.call1((self.0.clone_ref(py).into_ref(py),)).unwrap();
            serializer.serialize_bytes(pybytes.downcast::<PyBytes>().unwrap().as_bytes())
        })
    }
}

struct PartialUDFVisitor;

impl<'de> Visitor<'de> for PartialUDFVisitor {
    type Value = PartialUDF;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a Python object of type PartialUDF")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Python::with_gil(|py| {
            let serde_module = PyModule::import(py, pyo3::intern!(py, "daft.pickle")).unwrap();
            let loads = serde_module.getattr(pyo3::intern!(py, "loads")).unwrap();
            let py_partial_udf = loads.call1((v,)).unwrap();
            Ok(PartialUDF(py_partial_udf.to_object(py)))
        })
    }
}

impl<'de> Deserialize<'de> for PartialUDF {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(PartialUDFVisitor)
    }
}

impl PartialEq for PartialUDF {
    fn eq(&self, other: &Self) -> bool {
        Python::with_gil(|py| self.0.as_ref(py).eq(other.0.as_ref(py)).unwrap())
    }
}

impl Eq for PartialUDF {}

impl Hash for PartialUDF {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let py_obj_hash = Python::with_gil(|py| self.0.as_ref(py).hash());
        match py_obj_hash {
            // If Python object is hashable, hash the Python-side hash.
            Ok(py_obj_hash) => py_obj_hash.hash(state),
            // Fall back to hashing the pickled Python object.
            Err(_) => serde_json::to_vec(self).unwrap().hash(state),
        }
    }
}
