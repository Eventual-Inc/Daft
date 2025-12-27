use std::sync::Arc;

use numpy::{PyArrayDyn, PyArrayMethods, ToPyArray};
use pyo3::prelude::*;
use pyo3::exceptions::PyTypeError;
use half::bf16;

use crate::NdArray;

#[derive(Debug)]
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
    BFloat16(Bound<'py, PyArrayDyn<bf16>>),
    F32(Bound<'py, PyArrayDyn<f32>>),
    F64(Bound<'py, PyArrayDyn<f64>>),
    Py(Bound<'py, PyArrayDyn<Py<PyAny>>>),
}

impl<'a, 'py> FromPyObject<'a, 'py> for NumpyArray<'py> {
    type Error = PyErr;
    fn extract(ob: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(val) = ob.extract::<Bound<'py, PyArrayDyn<i8>>>() { return Ok(NumpyArray::I8(val)); }
        if let Ok(val) = ob.extract::<Bound<'py, PyArrayDyn<u8>>>() { return Ok(NumpyArray::U8(val)); }
        if let Ok(val) = ob.extract::<Bound<'py, PyArrayDyn<i16>>>() { return Ok(NumpyArray::I16(val)); }
        if let Ok(val) = ob.extract::<Bound<'py, PyArrayDyn<u16>>>() { return Ok(NumpyArray::U16(val)); }
        if let Ok(val) = ob.extract::<Bound<'py, PyArrayDyn<i32>>>() { return Ok(NumpyArray::I32(val)); }
        if let Ok(val) = ob.extract::<Bound<'py, PyArrayDyn<u32>>>() { return Ok(NumpyArray::U32(val)); }
        if let Ok(val) = ob.extract::<Bound<'py, PyArrayDyn<i64>>>() { return Ok(NumpyArray::I64(val)); }
        if let Ok(val) = ob.extract::<Bound<'py, PyArrayDyn<u64>>>() { return Ok(NumpyArray::U64(val)); }
        
        // Check for BFloat16 safely
        // We need to check the dtype name string because rust-numpy's extraction for bf16 
        // can panic if the environment doesn't have the bfloat16 type registered correctly,
        // or when checking against non-bf16 arrays in some versions.
        let is_bfloat16 = ob.getattr("dtype")
            .and_then(|dt| dt.getattr("name"))
            .and_then(|name| name.extract::<String>())
            .map(|name| name == "bfloat16")
            .unwrap_or(false);

        if is_bfloat16 {
             if let Ok(val) = ob.extract::<Bound<'py, PyArrayDyn<bf16>>>() { return Ok(NumpyArray::BFloat16(val)); }
        }

        if let Ok(val) = ob.extract::<Bound<'py, PyArrayDyn<f32>>>() { return Ok(NumpyArray::F32(val)); }
        if let Ok(val) = ob.extract::<Bound<'py, PyArrayDyn<f64>>>() { return Ok(NumpyArray::F64(val)); }
        if let Ok(val) = ob.extract::<Bound<'py, PyArrayDyn<Py<PyAny>>>>() { return Ok(NumpyArray::Py(val)); }

        Err(PyTypeError::new_err("Could not convert to NumpyArray"))
    }
}

impl<'py> IntoPyObject<'py> for NumpyArray<'py> {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, _py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            NumpyArray::I8(b) => Ok(b.into_any()),
            NumpyArray::U8(b) => Ok(b.into_any()),
            NumpyArray::I16(b) => Ok(b.into_any()),
            NumpyArray::U16(b) => Ok(b.into_any()),
            NumpyArray::I32(b) => Ok(b.into_any()),
            NumpyArray::U32(b) => Ok(b.into_any()),
            NumpyArray::I64(b) => Ok(b.into_any()),
            NumpyArray::U64(b) => Ok(b.into_any()),
            NumpyArray::BFloat16(b) => Ok(b.into_any()),
            NumpyArray::F32(b) => Ok(b.into_any()),
            NumpyArray::F64(b) => Ok(b.into_any()),
            NumpyArray::Py(b) => Ok(b.into_any()),
        }
    }
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
            Self::BFloat16(arr) => NdArray::BFloat16(arr.to_owned_array()),
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
            NdArray::BFloat16(arr) => Self::BFloat16(arr.to_pyarray(py)),
            NdArray::F32(arr) => Self::F32(arr.to_pyarray(py)),
            NdArray::F64(arr) => Self::F64(arr.to_pyarray(py)),
            NdArray::Py(arr) => Self::Py(
                arr.map(|e: &common_py_serde::PyObjectWrapper| e.0.clone_ref(py))
                    .to_pyarray(py),
            ),
        }
    }
}
