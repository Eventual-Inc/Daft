pub mod arrow;
pub mod hashable_float_wrapper;
pub mod supertype;

#[macro_export]
macro_rules! impl_binary_trait_by_reference {
    ($ty:ty, $trait:ident, $fname:ident) => {
        impl $trait for $ty {
            type Output = DaftResult<$ty>;
            fn $fname(self, other: Self) -> Self::Output {
                (&self).$fname(&other)
            }
        }
    };
}

#[macro_export]
macro_rules! impl_bincode_py_state_serialization {
    ($ty:ty) => {
        #[cfg(feature = "python")]
        #[pymethods]
        impl $ty {
            pub fn __reduce__(&self, py: Python) -> PyResult<(PyObject, PyObject)> {
                Ok((
                    Self::type_object(py)
                        .getattr("_from_serialized")?
                        .to_object(py),
                    (PyBytes::new(py, &bincode::serialize(&self).unwrap()).to_object(py),)
                        .to_object(py),
                ))
            }

            #[staticmethod]
            pub fn _from_serialized(py: Python, serialized: PyObject) -> PyResult<Self> {
                serialized
                    .extract::<&PyBytes>(py)
                    .map(|s| bincode::deserialize(s.as_bytes()).unwrap())
            }
        }
    };
}
