use std::sync::Arc;

use numpy::{PyArrayDyn, PyArrayMethods, ToPyArray};
use pyo3::prelude::*;

use crate::NdArray;

#[derive(FromPyObject, IntoPyObject)]
/// Wrapper around rust-numpy PyArrayDyn to support dynamic types
pub enum NumpyArray<'py> {
    I8(Bound<'py, PyArrayDyn<i8>>),
    U8(Bound<'py, PyArrayDyn<u8>>),
    I16(Bound<'py, PyArrayDyn<i16>>),
    U16(Bound<'py, PyArrayDyn<u16>>),
    I32(Bound<'py, PyArrayDyn<i32>>),
    U32(Bound<'py, PyArrayDyn<u32>>),
    I64(Bound<'py, PyArrayDyn<i64>>),
    U64(Bound<'py, PyArrayDyn<u64>>),
    F32(Bound<'py, PyArrayDyn<f32>>),
    F64(Bound<'py, PyArrayDyn<f64>>),
    Py(Bound<'py, PyArrayDyn<Py<PyAny>>>),
}

impl<'py> NumpyArray<'py> {
    pub fn to_ndarray(&self) -> NdArray {
        match self {
            Self::I8(arr) => NdArray::I8(arr.to_owned_array()),
            Self::U8(arr) => NdArray::U8(arr.to_owned_array()),
            Self::I16(arr) => NdArray::I16(arr.to_owned_array()),
            Self::U16(arr) => NdArray::U16(arr.to_owned_array()),
            Self::I32(arr) => NdArray::I32(arr.to_owned_array()),
            Self::U32(arr) => NdArray::U32(arr.to_owned_array()),
            Self::I64(arr) => NdArray::I64(arr.to_owned_array()),
            Self::U64(arr) => NdArray::U64(arr.to_owned_array()),
            Self::F32(arr) => NdArray::F32(arr.to_owned_array()),
            Self::F64(arr) => NdArray::F64(arr.to_owned_array()),
            Self::Py(arr) => NdArray::Py(
                arr.to_owned_array()
                    .map(|e| Arc::new(e.clone_ref(arr.py())).into()),
            ),
        }
    }

    pub fn from_ndarray(arr: &NdArray, py: Python<'py>) -> Self {
        match arr {
            NdArray::I8(arr) => Self::I8(arr.to_pyarray(py)),
            NdArray::U8(arr) => Self::U8(arr.to_pyarray(py)),
            NdArray::I16(arr) => Self::I16(arr.to_pyarray(py)),
            NdArray::U16(arr) => Self::U16(arr.to_pyarray(py)),
            NdArray::I32(arr) => Self::I32(arr.to_pyarray(py)),
            NdArray::U32(arr) => Self::U32(arr.to_pyarray(py)),
            NdArray::I64(arr) => Self::I64(arr.to_pyarray(py)),
            NdArray::U64(arr) => Self::U64(arr.to_pyarray(py)),
            NdArray::F32(arr) => Self::F32(arr.to_pyarray(py)),
            NdArray::F64(arr) => Self::F64(arr.to_pyarray(py)),
            NdArray::Py(arr) => Self::Py(
                arr.map(|e: &common_py_serde::PyObjectWrapper| e.0.clone_ref(py))
                    .to_pyarray(py),
            ),
        }
    }
}
