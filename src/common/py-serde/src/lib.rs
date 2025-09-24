mod python;

pub use bincode;

#[cfg(feature = "python")]
pub use crate::python::{
    PyObjectWrapper, deserialize_py_object, pickle_dumps, pickle_loads, serialize_py_object,
};
