use std::hash::{Hash, Hasher};

use common_py_serde::{deserialize_py_object, serialize_py_object};
use pyo3::{PyObject, Python};
use serde::{Deserialize, Serialize};

// This is a Rust wrapper on top of a Python PartialStatelessUDF or PartialStatefulUDF to make it serde-able and hashable
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PyPartialUDF(
    #[serde(
        serialize_with = "serialize_py_object",
        deserialize_with = "deserialize_py_object"
    )]
    pub PyObject,
);

impl PartialEq for PyPartialUDF {
    fn eq(&self, other: &Self) -> bool {
        Python::with_gil(|py| self.0.as_ref(py).eq(other.0.as_ref(py)).unwrap())
    }
}

impl Eq for PyPartialUDF {}

impl Hash for PyPartialUDF {
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
