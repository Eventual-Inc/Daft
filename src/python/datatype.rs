use crate::datatypes::DataType;
use pyo3::prelude::*;

#[pyclass]
#[derive(Clone)]
pub struct PyDataType {
    pub dtype: DataType,
}

#[pymethods]
impl PyDataType {
    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.dtype))
    }

    #[staticmethod]
    pub fn null() -> PyResult<Self> {
        Ok(DataType::Null.into())
    }

    #[staticmethod]
    pub fn bool() -> PyResult<Self> {
        Ok(DataType::Boolean.into())
    }

    #[staticmethod]
    pub fn int8() -> PyResult<Self> {
        Ok(DataType::Int8.into())
    }

    #[staticmethod]
    pub fn int16() -> PyResult<Self> {
        Ok(DataType::Int16.into())
    }

    #[staticmethod]
    pub fn int32() -> PyResult<Self> {
        Ok(DataType::Int32.into())
    }

    #[staticmethod]
    pub fn int64() -> PyResult<Self> {
        Ok(DataType::Int64.into())
    }

    #[staticmethod]
    pub fn uint8() -> PyResult<Self> {
        Ok(DataType::UInt8.into())
    }

    #[staticmethod]
    pub fn uint16() -> PyResult<Self> {
        Ok(DataType::UInt16.into())
    }

    #[staticmethod]
    pub fn uint32() -> PyResult<Self> {
        Ok(DataType::UInt32.into())
    }

    #[staticmethod]
    pub fn uint64() -> PyResult<Self> {
        Ok(DataType::UInt64.into())
    }

    #[staticmethod]
    pub fn float32() -> PyResult<Self> {
        Ok(DataType::Float32.into())
    }

    #[staticmethod]
    pub fn float64() -> PyResult<Self> {
        Ok(DataType::Float64.into())
    }

    #[staticmethod]
    pub fn binary() -> PyResult<Self> {
        Ok(DataType::Binary.into())
    }

    #[staticmethod]
    pub fn string() -> PyResult<Self> {
        Ok(DataType::Utf8.into())
    }
}

impl From<DataType> for PyDataType {
    fn from(value: DataType) -> Self {
        PyDataType { dtype: value }
    }
}

impl From<PyDataType> for DataType {
    fn from(item: PyDataType) -> Self {
        item.dtype
    }
}
