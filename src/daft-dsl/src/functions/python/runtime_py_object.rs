#![allow(clippy::all, reason = "todo: remove; getting a rustc error")]

use serde::{Deserialize, Serialize};

/// A wrapper around PyObject that is safe to use even when the Python feature flag isn't turned on
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RuntimePyObject {
    #[cfg(feature = "python")]
    obj: crate::pyobj_serde::PyObjectWrapper,
}

impl RuntimePyObject {
    #[cfg(feature = "test-utils")]
    pub fn new_testing_none() -> Self {
        #[cfg(feature = "python")]
        {
            let none_value = pyo3::Python::with_gil(|py| py.None());
            Self {
                obj: crate::pyobj_serde::PyObjectWrapper(none_value),
            }
        }
        #[cfg(not(feature = "python"))]
        {
            Self {}
        }
    }

    #[cfg(feature = "python")]
    pub fn new(value: pyo3::PyObject) -> Self {
        Self {
            obj: crate::pyobj_serde::PyObjectWrapper(value),
        }
    }

    #[cfg(feature = "python")]
    pub fn unwrap(self) -> pyo3::PyObject {
        self.obj.0
    }
}

#[cfg(feature = "python")]
impl AsRef<pyo3::PyObject> for RuntimePyObject {
    /// Retrieves a reference to the underlying pyo3::PyObject object
    fn as_ref(&self) -> &pyo3::PyObject {
        &self.obj.0
    }
}

#[cfg(feature = "python")]
impl From<pyo3::PyObject> for RuntimePyObject {
    fn from(value: pyo3::PyObject) -> Self {
        Self::new(value)
    }
}
