#[cfg(feature = "python")]
mod python;

#[cfg(feature = "python")]
use common_py_serde::PyObjectWrapper;
use ndarray::ArrayD;
#[cfg(feature = "python")]
pub use python::NumpyArray;

pub enum NdArray {
    I8(ArrayD<i8>),
    U8(ArrayD<u8>),
    I16(ArrayD<i16>),
    U16(ArrayD<u16>),
    I32(ArrayD<i32>),
    U32(ArrayD<u32>),
    I64(ArrayD<i64>),
    U64(ArrayD<u64>),
    F32(ArrayD<f32>),
    F64(ArrayD<f64>),
    #[cfg(feature = "python")]
    Py(ArrayD<PyObjectWrapper>),
}

impl NdArray {
    pub fn shape(&self) -> &[usize] {
        match self {
            Self::I8(arr) => arr.shape(),
            Self::U8(arr) => arr.shape(),
            Self::I16(arr) => arr.shape(),
            Self::U16(arr) => arr.shape(),
            Self::I32(arr) => arr.shape(),
            Self::U32(arr) => arr.shape(),
            Self::I64(arr) => arr.shape(),
            Self::U64(arr) => arr.shape(),
            Self::F32(arr) => arr.shape(),
            Self::F64(arr) => arr.shape(),
            #[cfg(feature = "python")]
            Self::Py(arr) => arr.shape(),
        }
    }
}
