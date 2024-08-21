pub mod arrow;
pub mod display_table;
pub mod dyn_compare;
pub mod supertype;

pub use bincode;

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
        #[pyo3::pymethods]
        impl $ty {
            pub fn __reduce__(
                &self,
                py: pyo3::Python,
            ) -> pyo3::PyResult<(pyo3::PyObject, pyo3::PyObject)> {
                use pyo3::PyTypeInfo;

                Ok((
                    Self::type_object(py)
                        .getattr("_from_serialized")?
                        .to_object(py),
                    (pyo3::types::PyBytes::new(
                        py,
                        &$crate::utils::bincode::serialize(&self).unwrap(),
                    )
                    .to_object(py),)
                        .to_object(py),
                ))
            }

            #[staticmethod]
            pub fn _from_serialized(
                py: pyo3::Python,
                serialized: pyo3::PyObject,
            ) -> pyo3::PyResult<Self> {
                serialized
                    .extract::<&pyo3::types::PyBytes>(py)
                    .map(|s| $crate::utils::bincode::deserialize(s.as_bytes()).unwrap())
            }
        }
    };
}
