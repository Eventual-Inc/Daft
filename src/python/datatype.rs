use crate::datatypes::{DataType, Field, ImageMode, TimeUnit};
use pyo3::{
    class::basic::CompareOp,
    exceptions::PyValueError,
    prelude::*,
    types::{PyBytes, PyDict, PyString, PyTuple},
};

#[pyclass]
#[derive(Clone)]
pub struct PyTimeUnit {
    pub timeunit: TimeUnit,
}

impl From<TimeUnit> for PyTimeUnit {
    fn from(value: TimeUnit) -> Self {
        PyTimeUnit { timeunit: value }
    }
}

impl From<PyTimeUnit> for TimeUnit {
    fn from(item: PyTimeUnit) -> Self {
        item.timeunit
    }
}

#[pymethods]
impl PyTimeUnit {
    #[staticmethod]
    pub fn nanoseconds() -> PyResult<Self> {
        Ok(TimeUnit::Nanoseconds.into())
    }
    #[staticmethod]
    pub fn microseconds() -> PyResult<Self> {
        Ok(TimeUnit::Microseconds.into())
    }
    #[staticmethod]
    pub fn milliseconds() -> PyResult<Self> {
        Ok(TimeUnit::Milliseconds.into())
    }
    #[staticmethod]
    pub fn seconds() -> PyResult<Self> {
        Ok(TimeUnit::Seconds.into())
    }
    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.timeunit))
    }
    pub fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        //https://pyo3.rs/v0.19.0/class/object.html
        match op {
            CompareOp::Eq => Ok(self.timeunit == other.timeunit),
            CompareOp::Ne => Ok(self.timeunit != other.timeunit),
            _ => Err(pyo3::exceptions::PyNotImplementedError::new_err(())),
        }
    }
    pub fn __hash__(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hash;
        use std::hash::Hasher;
        let mut hasher = DefaultHasher::new();
        self.timeunit.hash(&mut hasher);
        hasher.finish()
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyDataType {
    pub dtype: DataType,
}

#[pymethods]
impl PyDataType {
    #[new]
    #[pyo3(signature = (*args))]
    pub fn new(args: &PyTuple) -> PyResult<Self> {
        match args.len() {
            0 => Ok(DataType::new_null().into()),
            _ => Err(PyValueError::new_err(format!(
                "expected no arguments to make new PyDataType, got : {}",
                args.len()
            ))),
        }
    }

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

    #[staticmethod]
    pub fn date() -> PyResult<Self> {
        Ok(DataType::Date.into())
    }

    #[staticmethod]
    pub fn timestamp(timeunit: PyTimeUnit, timezone: Option<String>) -> PyResult<Self> {
        Ok(DataType::Timestamp(timeunit.timeunit, timezone).into())
    }

    #[staticmethod]
    pub fn list(name: &str, data_type: Self) -> PyResult<Self> {
        Ok(DataType::List(Box::new(Field::new(name, data_type.dtype))).into())
    }

    #[staticmethod]
    pub fn fixed_size_list(name: &str, data_type: Self, size: i64) -> PyResult<Self> {
        if size <= 0 {
            return Err(PyValueError::new_err(format!(
                "The size for fixed-size list types must be a positive integer, but got: {}",
                size
            )));
        }
        Ok(DataType::FixedSizeList(
            Box::new(Field::new(name, data_type.dtype)),
            usize::try_from(size)?,
        )
        .into())
    }

    #[staticmethod]
    pub fn r#struct(fields: &PyDict) -> PyResult<Self> {
        Ok(DataType::Struct(
            fields
                .iter()
                .map(|(name, dtype)| {
                    Ok(Field::new(
                        name.downcast::<PyString>()?.to_str()?,
                        dtype.extract::<PyDataType>()?.dtype,
                    ))
                })
                .collect::<PyResult<Vec<Field>>>()?,
        )
        .into())
    }

    #[staticmethod]
    pub fn extension(
        name: &str,
        storage_data_type: Self,
        metadata: Option<&str>,
    ) -> PyResult<Self> {
        Ok(DataType::Extension(
            name.to_string(),
            Box::new(storage_data_type.dtype),
            metadata.map(|s| s.to_string()),
        )
        .into())
    }

    #[staticmethod]
    pub fn embedding(name: &str, data_type: Self, size: i64) -> PyResult<Self> {
        if size <= 0 {
            return Err(PyValueError::new_err(format!(
                "The size for embedding types must be a positive integer, but got: {}",
                size
            )));
        }
        if !data_type.dtype.is_numeric() {
            return Err(PyValueError::new_err(format!(
                "The data type for an embedding must be numeric, but got: {}",
                data_type.dtype
            )));
        }

        Ok(DataType::Embedding(
            Box::new(Field::new(name, data_type.dtype)),
            usize::try_from(size)?,
        )
        .into())
    }

    #[staticmethod]
    pub fn image(
        mode: Option<ImageMode>,
        height: Option<u32>,
        width: Option<u32>,
    ) -> PyResult<Self> {
        // TODO(Clark): Make dtype optional instead of falling back to UInt8 here.
        let dtype = mode.map_or(Box::new(DataType::UInt8), |m| Box::new(DataType::from(&m)));
        if !dtype.is_numeric() {
            panic!(
                "The data type for an image must be numeric, but got: {}",
                dtype
            );
        }
        match (height, width) {
            (Some(height), Some(width)) => {
                let image_mode = mode.ok_or(PyValueError::new_err(
                    "Image mode must be provided if specifying an image size.",
                ))?;
                Ok(DataType::FixedShapeImage(dtype, image_mode, height, width).into())
            }
            (None, None) => Ok(DataType::Image(dtype, mode).into()),
            (_, _) => Err(PyValueError::new_err(format!("Height and width for image type must both be specified or both not specified, but got: height={:?}, width={:?}", height, width))),
        }
    }

    #[staticmethod]
    pub fn python() -> PyResult<Self> {
        Ok(DataType::Python.into())
    }

    pub fn is_logical(&self) -> PyResult<bool> {
        Ok(self.dtype.is_logical())
    }

    pub fn is_equal(&self, other: &PyAny) -> PyResult<bool> {
        if other.is_instance_of::<PyDataType>()? {
            let other = other.extract::<PyDataType>()?;
            Ok(self.dtype == other.dtype)
        } else {
            Ok(false)
        }
    }

    pub fn __setstate__(&mut self, py: Python, state: PyObject) -> PyResult<()> {
        match state.extract::<&PyBytes>(py) {
            Ok(s) => {
                self.dtype = bincode::deserialize(s.as_bytes()).unwrap();
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub fn __getstate__(&self, py: Python) -> PyResult<PyObject> {
        Ok(PyBytes::new(py, &bincode::serialize(&self.dtype).unwrap()).to_object(py))
    }

    pub fn __hash__(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hash;
        use std::hash::Hasher;
        let mut hasher = DefaultHasher::new();
        self.dtype.hash(&mut hasher);
        hasher.finish()
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
