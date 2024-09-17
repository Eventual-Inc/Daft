mod python;

pub use bincode;

#[cfg(feature = "python")]
pub use crate::{python::deserialize_py_object, python::serialize_py_object};
