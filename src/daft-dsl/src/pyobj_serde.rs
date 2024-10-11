use std::{
    hash::{Hash, Hasher},
    io::Write,
};

use common_py_serde::{deserialize_py_object, serialize_py_object};
use pyo3::{types::PyAnyMethods, PyObject, Python};
use serde::{Deserialize, Serialize};

// This is a Rust wrapper on top of a Python PartialStatelessUDF or PartialStatefulUDF to make it serde-able and hashable
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PyObjectWrapper(
    #[serde(
        serialize_with = "serialize_py_object",
        deserialize_with = "deserialize_py_object"
    )]
    pub PyObject,
);

impl PartialEq for PyObjectWrapper {
    fn eq(&self, other: &Self) -> bool {
        Python::with_gil(|py| self.0.bind(py).eq(other.0.bind(py)).unwrap())
    }
}

impl Eq for PyObjectWrapper {}

impl Hash for PyObjectWrapper {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let py_obj_hash = Python::with_gil(|py| self.0.bind(py).hash());
        match py_obj_hash {
            // If Python object is hashable, hash the Python-side hash.
            Ok(py_obj_hash) => py_obj_hash.hash(state),
            // Fall back to hashing the pickled Python object.
            Err(_) => {
                let hasher = HashWriter { state };
                bincode::serialize_into(hasher, self)
                    .expect("Pickling error occurred when computing hash of Pyobject");
            }
        }
    }
}

struct HashWriter<'a, H: Hasher> {
    state: &'a mut H,
}

impl<'a, H: Hasher> Write for HashWriter<'a, H> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        buf.hash(self.state);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
