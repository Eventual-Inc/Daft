pub mod arrow;
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
            pub fn __setstate__(&mut self, state: &PyBytes) -> PyResult<()> {
                *self = bincode::deserialize(state.as_bytes()).unwrap();
                Ok(())
            }

            pub fn __getstate__<'py>(&self, py: Python<'py>) -> PyResult<&'py PyBytes> {
                Ok(PyBytes::new(py, &bincode::serialize(&self).unwrap()))
            }
        }
    };
}
