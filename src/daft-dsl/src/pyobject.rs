use std::hash::{Hash, Hasher};

use pyo3::prelude::*;

use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};

#[derive(Clone, Debug)]
pub struct DaftPyObject {
    pub pyobject: PyObject,
}

impl PartialEq for DaftPyObject {
    fn eq(&self, other: &Self) -> bool {
        Python::with_gil(|py| {
            self.pyobject
                .as_ref(py)
                .eq(other.pyobject.as_ref(py))
                .unwrap()
        })
    }
}

impl Eq for DaftPyObject {}

impl Hash for DaftPyObject {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let py_obj_hash = Python::with_gil(|py| self.pyobject.as_ref(py).hash());
        match py_obj_hash {
            // If Python object is hashable, hash the Python-side hash.
            Ok(py_obj_hash) => py_obj_hash.hash(state),
            // Fall back to hashing the pickled Python object.
            Err(_) => serde_json::to_vec(self).unwrap().hash(state),
        }
    }
}

impl Serialize for DaftPyObject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Python::with_gil(|py| {
            let pickle = PyModule::import(py, pyo3::intern!(py, "daft.pickle")).unwrap();
            let dumps = pickle.getattr(pyo3::intern!(py, "dumps")).unwrap();
            let bytes: &[u8] = dumps
                .call1((self.pyobject.clone_ref(py),))
                .unwrap()
                .extract()
                .unwrap();
            serializer.serialize_bytes(bytes)
        })
    }
}

struct DaftPyObjectVisitor;

impl<'de> Visitor<'de> for DaftPyObjectVisitor {
    type Value = DaftPyObject;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a Python object")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Python::with_gil(|py| {
            let serde_module = PyModule::import(py, pyo3::intern!(py, "daft.pickle")).unwrap();
            let loads = serde_module.getattr(pyo3::intern!(py, "loads")).unwrap();
            let pyobj = loads.call1((v,)).unwrap();
            Ok(DaftPyObject {
                pyobject: pyobj.to_object(py),
            })
        })
    }
}

impl<'de> Deserialize<'de> for DaftPyObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(DaftPyObjectVisitor)
    }
}
