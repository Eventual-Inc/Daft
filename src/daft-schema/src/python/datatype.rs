use common_arrow_ffi as ffi;
use common_py_serde::impl_bincode_py_state_serialization;
use indexmap::IndexMap;
use pyo3::{class::basic::CompareOp, exceptions::PyValueError, prelude::*};
use serde::{Deserialize, Serialize};

use crate::{dtype::DataType, field::Field, image_mode::ImageMode, time_unit::TimeUnit};

#[pyclass]
#[derive(Clone)]
pub struct PyTimeUnit {
    pub timeunit: TimeUnit,
}

impl From<TimeUnit> for PyTimeUnit {
    fn from(value: TimeUnit) -> Self {
        Self { timeunit: value }
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

    #[must_use]
    pub fn __hash__(&self) -> u64 {
        use std::{
            collections::hash_map::DefaultHasher,
            hash::{Hash, Hasher},
        };
        let mut hasher = DefaultHasher::new();
        self.timeunit.hash(&mut hasher);
        hasher.finish()
    }
}

#[pyclass(module = "daft.daft")]
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub fn fixed_size_binary(size: i64) -> PyResult<Self> {
        if size <= 0 {
            return Err(PyValueError::new_err(format!(
                "The size for fixed-size binary types must be a positive integer, but got: {size}"
            )));
        }
        Ok(DataType::FixedSizeBinary(usize::try_from(size)?).into())
    }

    #[staticmethod]
    pub fn string() -> PyResult<Self> {
        Ok(DataType::Utf8.into())
    }

    #[staticmethod]
    pub fn decimal128(precision: usize, scale: usize) -> PyResult<Self> {
        Ok(DataType::Decimal128(precision, scale).into())
    }

    #[staticmethod]
    pub fn date() -> PyResult<Self> {
        Ok(DataType::Date.into())
    }

    #[staticmethod]
    pub fn time(timeunit: PyTimeUnit) -> PyResult<Self> {
        if !matches!(
            timeunit.timeunit,
            TimeUnit::Microseconds | TimeUnit::Nanoseconds
        ) {
            return Err(PyValueError::new_err(format!(
                "The time unit for time types must be microseconds or nanoseconds, but got: {}",
                timeunit.timeunit
            )));
        }
        Ok(DataType::Time(timeunit.timeunit).into())
    }

    #[staticmethod]
    #[pyo3(signature = (timeunit, timezone=None))]
    pub fn timestamp(timeunit: PyTimeUnit, timezone: Option<String>) -> PyResult<Self> {
        Ok(DataType::Timestamp(timeunit.timeunit, timezone).into())
    }

    #[staticmethod]
    pub fn duration(timeunit: PyTimeUnit) -> PyResult<Self> {
        Ok(DataType::Duration(timeunit.timeunit).into())
    }
    #[staticmethod]
    pub fn interval() -> PyResult<Self> {
        Ok(DataType::Interval.into())
    }

    #[staticmethod]
    pub fn list(data_type: Self) -> PyResult<Self> {
        Ok(DataType::List(Box::new(data_type.dtype)).into())
    }

    #[staticmethod]
    pub fn fixed_size_list(data_type: Self, size: i64) -> PyResult<Self> {
        if size <= 0 {
            return Err(PyValueError::new_err(format!(
                "The size for fixed-size list types must be a positive integer, but got: {size}"
            )));
        }
        Ok(DataType::FixedSizeList(Box::new(data_type.dtype), usize::try_from(size)?).into())
    }

    #[staticmethod]
    pub fn map(key_type: Self, value_type: Self) -> PyResult<Self> {
        Ok(DataType::Map {
            key: Box::new(key_type.dtype),
            value: Box::new(value_type.dtype),
        }
        .into())
    }

    #[staticmethod]
    #[must_use]
    pub fn r#struct(fields: IndexMap<String, Self>) -> Self {
        DataType::Struct(
            fields
                .into_iter()
                .map(|(name, dtype)| Field::new(name, dtype.dtype))
                .collect::<Vec<Field>>(),
        )
        .into()
    }

    #[staticmethod]
    #[pyo3(signature = (name, storage_data_type, metadata=None))]
    pub fn extension(
        name: &str,
        storage_data_type: Self,
        metadata: Option<&str>,
    ) -> PyResult<Self> {
        Ok(DataType::Extension(
            name.to_string(),
            Box::new(storage_data_type.dtype),
            metadata.map(std::string::ToString::to_string),
        )
        .into())
    }

    #[staticmethod]
    pub fn embedding(data_type: Self, size: i64) -> PyResult<Self> {
        if size <= 0 {
            return Err(PyValueError::new_err(format!(
                "The size for embedding types must be a positive integer, but got: {size}"
            )));
        }
        if !data_type.dtype.is_numeric() {
            return Err(PyValueError::new_err(format!(
                "The data type for an embedding must be numeric, but got: {}",
                data_type.dtype
            )));
        }

        Ok(DataType::Embedding(Box::new(data_type.dtype), usize::try_from(size)?).into())
    }

    #[staticmethod]
    #[pyo3(signature = (mode=None, height=None, width=None))]
    pub fn image(
        mode: Option<ImageMode>,
        height: Option<u32>,
        width: Option<u32>,
    ) -> PyResult<Self> {
        match (height, width) {
            (Some(height), Some(width)) => {
                let image_mode = mode.ok_or_else(|| PyValueError::new_err(
                    "Image mode must be provided if specifying an image size.",
                ))?;
                Ok(DataType::FixedShapeImage(image_mode, height, width).into())
            }
            (None, None) => Ok(DataType::Image(mode).into()),
            (_, _) => Err(PyValueError::new_err(format!("Height and width for image type must both be specified or both not specified, but got: height={height:?}, width={width:?}"))),
        }
    }

    #[staticmethod]
    #[pyo3(signature = (dtype, shape=None))]
    pub fn tensor(dtype: Self, shape: Option<Vec<u64>>) -> PyResult<Self> {
        // TODO(Clark): Add support for non-numeric (e.g. string) tensor columns.
        if !dtype.dtype.is_numeric() {
            return Err(PyValueError::new_err(format!(
                "The data type for a tensor column must be numeric, but got: {}",
                dtype.dtype
            )));
        }
        let dtype = Box::new(dtype.dtype);
        match shape {
            Some(shape) => Ok(DataType::FixedShapeTensor(dtype, shape).into()),
            None => Ok(DataType::Tensor(dtype).into()),
        }
    }

    #[staticmethod]
    #[pyo3(signature = (dtype, shape=None, use_offset_indices=false))]
    pub fn sparse_tensor(
        dtype: Self,
        shape: Option<Vec<u64>>,
        use_offset_indices: bool,
    ) -> PyResult<Self> {
        if !dtype.dtype.is_numeric() {
            return Err(PyValueError::new_err(format!(
                "The data type for a tensor column must be numeric, but got: {}",
                dtype.dtype
            )));
        }
        let dtype = Box::new(dtype.dtype);
        match shape {
            Some(shape) => {
                Ok(DataType::FixedShapeSparseTensor(dtype, shape, use_offset_indices).into())
            }
            None => Ok(DataType::SparseTensor(dtype, use_offset_indices).into()),
        }
    }

    #[staticmethod]
    pub fn python() -> PyResult<Self> {
        Ok(DataType::Python.into())
    }

    pub fn to_arrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pyarrow = py.import(pyo3::intern!(py, "pyarrow"))?;
        match &self.dtype {
            DataType::FixedShapeTensor(dtype, shape) => {
                if py
                    .import(pyo3::intern!(py, "daft.utils"))?
                    .getattr(pyo3::intern!(py, "pyarrow_supports_fixed_shape_tensor"))?
                    .call0()?
                    .extract()?
                {
                    pyarrow
                        .getattr(pyo3::intern!(py, "fixed_shape_tensor"))?
                        .call1((
                            Self {
                                dtype: *dtype.clone(),
                            }
                            .to_arrow(py)?,
                            pyo3::types::PyTuple::new(py, shape.clone())?,
                        ))
                } else {
                    // Fall back to default Daft super extension representation if installed pyarrow doesn't have the
                    // canonical tensor extension type.
                    ffi::dtype_to_py(py, &self.dtype.to_arrow()?, pyarrow)
                }
            }
            _ => ffi::dtype_to_py(py, &self.dtype.to_arrow()?, pyarrow),
        }
    }
    pub fn is_numeric(&self) -> PyResult<bool> {
        Ok(self.dtype.is_numeric())
    }

    pub fn is_integer(&self) -> PyResult<bool> {
        Ok(self.dtype.is_integer())
    }

    pub fn is_image(&self) -> PyResult<bool> {
        Ok(self.dtype.is_image())
    }
    pub fn get_image_inner(&self) -> PyResult<Option<ImageMode>> {
        match &self.dtype {
            DataType::Image(mode) => Ok(*mode),
            _ => Err(PyValueError::new_err(format!(
                "Data type is not an image, but is: {}",
                self.dtype
            ))),
        }
    }

    pub fn is_fixed_shape_image(&self) -> PyResult<bool> {
        Ok(self.dtype.is_fixed_shape_image())
    }

    pub fn get_fixed_shape_image_inner(&self) -> PyResult<(ImageMode, u32, u32)> {
        match &self.dtype {
            DataType::FixedShapeImage(mode, height, width) => Ok((*mode, *height, *width)),
            _ => Err(PyValueError::new_err(format!(
                "Data type is not a fixed-shape image, but is: {}",
                self.dtype
            ))),
        }
    }

    pub fn is_tensor(&self) -> PyResult<bool> {
        Ok(self.dtype.is_tensor())
    }
    pub fn get_tensor_inner(&self) -> PyResult<Self> {
        match &self.dtype {
            DataType::Tensor(data_type) => Ok(Self {
                dtype: *data_type.clone(),
            }),
            _ => Err(PyValueError::new_err(format!(
                "Data type is not a tensor, but is: {}",
                self.dtype
            ))),
        }
    }

    pub fn is_fixed_shape_tensor(&self) -> PyResult<bool> {
        Ok(self.dtype.is_fixed_shape_tensor())
    }
    fn get_fixed_shape_tensor_inner(&self) -> PyResult<(Self, Vec<u64>)> {
        match &self.dtype {
            DataType::FixedShapeTensor(data_type, shape) => Ok((
                Self {
                    dtype: *data_type.clone(),
                },
                shape.clone(),
            )),
            _ => Err(PyValueError::new_err(format!(
                "Data type is not a fixed-shape tensor, but is: {}",
                self.dtype
            ))),
        }
    }

    pub fn is_sparse_tensor(&self) -> PyResult<bool> {
        Ok(self.dtype.is_sparse_tensor())
    }
    pub fn get_sparse_tensor_inner(&self) -> PyResult<(Self, bool)> {
        match &self.dtype {
            DataType::SparseTensor(data_type, use_offset_indices) => Ok((
                Self {
                    dtype: *data_type.clone(),
                },
                *use_offset_indices,
            )),
            _ => Err(PyValueError::new_err(format!(
                "Data type is not a sparse tensor, but is: {}",
                self.dtype
            ))),
        }
    }

    pub fn is_fixed_shape_sparse_tensor(&self) -> PyResult<bool> {
        Ok(self.dtype.is_fixed_shape_sparse_tensor())
    }
    pub fn get_fixed_shape_sparse_tensor_inner(&self) -> PyResult<(Self, Vec<u64>, bool)> {
        match &self.dtype {
            DataType::FixedShapeSparseTensor(data_type, shape, use_offset_indices) => Ok((
                Self {
                    dtype: *data_type.clone(),
                },
                shape.clone(),
                *use_offset_indices,
            )),
            _ => Err(PyValueError::new_err(format!(
                "Data type is not a fixed-shape sparse tensor, but is: {}",
                self.dtype
            ))),
        }
    }

    pub fn is_boolean(&self) -> PyResult<bool> {
        Ok(self.dtype.is_boolean())
    }

    pub fn is_string(&self) -> PyResult<bool> {
        Ok(self.dtype.is_string())
    }

    pub fn is_logical(&self) -> PyResult<bool> {
        Ok(self.dtype.is_logical())
    }

    pub fn is_temporal(&self) -> PyResult<bool> {
        Ok(self.dtype.is_temporal())
    }

    pub fn is_duration(&self) -> PyResult<bool> {
        Ok(matches!(self.dtype, DataType::Duration(_)))
    }
    pub fn is_embedding(&self) -> PyResult<bool> {
        Ok(matches!(self.dtype, DataType::Embedding(_, _)))
    }

    pub fn get_embedding_inner(&self) -> PyResult<(Self, i64)> {
        match &self.dtype {
            DataType::Embedding(data_type, size) => Ok((
                Self {
                    dtype: *data_type.clone(),
                },
                i64::try_from(*size)?,
            )),
            _ => Err(PyValueError::new_err(format!(
                "Data type is not an embedding, but is: {}",
                self.dtype
            ))),
        }
    }

    pub fn is_decimal(&self) -> PyResult<bool> {
        Ok(matches!(self.dtype, DataType::Decimal128(_, _)))
    }

    pub fn get_decimal_inner(&self) -> PyResult<(usize, usize)> {
        match &self.dtype {
            DataType::Decimal128(precision, scale) => Ok((*precision, *scale)),
            _ => Err(PyValueError::new_err(format!(
                "Data type is not a decimal, but is: {}",
                self.dtype
            ))),
        }
    }

    pub fn get_duration_inner(&self) -> PyResult<PyTimeUnit> {
        match &self.dtype {
            DataType::Duration(timeunit) => Ok(PyTimeUnit::from(*timeunit)),
            _ => Err(PyValueError::new_err(format!(
                "Data type is not a duration, but is: {}",
                self.dtype
            ))),
        }
    }

    pub fn is_timestamp(&self) -> PyResult<bool> {
        Ok(self.dtype.is_timestamp())
    }

    pub fn get_timestamp_inner(&self) -> PyResult<(PyTimeUnit, Option<String>)> {
        match &self.dtype {
            DataType::Timestamp(timeunit, timezone) => {
                Ok((PyTimeUnit::from(*timeunit), timezone.clone()))
            }
            _ => Err(PyValueError::new_err(format!(
                "Data type is not a timestamp, but is: {}",
                self.dtype
            ))),
        }
    }
    pub fn is_time(&self) -> PyResult<bool> {
        Ok(matches!(self.dtype, DataType::Time(_)))
    }

    pub fn get_time_inner(&self) -> PyResult<PyTimeUnit> {
        match &self.dtype {
            DataType::Time(timeunit) => Ok(PyTimeUnit::from(*timeunit)),
            _ => Err(PyValueError::new_err(format!(
                "Data type is not a time, but is: {}",
                self.dtype
            ))),
        }
    }

    pub fn is_fixed_size_binary(&self) -> PyResult<bool> {
        Ok(matches!(self.dtype, DataType::FixedSizeBinary(_)))
    }

    pub fn get_fixed_size_binary_inner(&self) -> PyResult<i64> {
        match &self.dtype {
            DataType::FixedSizeBinary(size) => Ok(i64::try_from(*size)?),
            _ => Err(PyValueError::new_err(format!(
                "Data type is not a fixed-size binary, but is: {}",
                self.dtype
            ))),
        }
    }

    pub fn is_list(&self) -> PyResult<bool> {
        Ok(self.dtype.is_list())
    }

    pub fn get_list_inner(&self) -> PyResult<Self> {
        match &self.dtype {
            DataType::List(data_type) => Ok(Self {
                dtype: *data_type.clone(),
            }),
            _ => Err(PyValueError::new_err(format!(
                "Data type is not a list, but is: {}",
                self.dtype
            ))),
        }
    }

    pub fn is_fixed_size_list(&self) -> PyResult<bool> {
        Ok(matches!(self.dtype, DataType::FixedSizeList(_, _)))
    }

    pub fn get_fixed_size_list_inner(&self) -> PyResult<(Self, i64)> {
        match &self.dtype {
            DataType::FixedSizeList(data_type, size) => Ok((
                Self {
                    dtype: *data_type.clone(),
                },
                i64::try_from(*size)?,
            )),
            _ => Err(PyValueError::new_err(format!(
                "Data type is not a fixed-size list, but is: {}",
                self.dtype
            ))),
        }
    }

    pub fn is_map(&self) -> PyResult<bool> {
        Ok(self.dtype.is_map())
    }

    pub fn get_map_inner(&self) -> PyResult<(Self, Self)> {
        match &self.dtype {
            DataType::Map { key, value } => Ok((
                Self {
                    dtype: *key.clone(),
                },
                Self {
                    dtype: *value.clone(),
                },
            )),
            _ => Err(PyValueError::new_err(format!(
                "Data type is not a map, but is: {}",
                self.dtype
            ))),
        }
    }

    pub fn is_struct(&self) -> PyResult<bool> {
        Ok(matches!(self.dtype, DataType::Struct(_)))
    }

    pub fn get_struct_inner(&self) -> PyResult<IndexMap<String, Self>> {
        match &self.dtype {
            DataType::Struct(fields) => Ok(fields
                .iter()
                .map(|field| (field.name.to_string(), Self::from(field.dtype.clone())))
                .collect()),
            _ => Err(PyValueError::new_err(format!(
                "Data type is not a struct, but is: {}",
                self.dtype
            ))),
        }
    }

    pub fn is_extension(&self) -> PyResult<bool> {
        Ok(matches!(self.dtype, DataType::Extension(_, _, _)))
    }

    pub fn get_extension_inner(&self) -> PyResult<(String, Self, Option<String>)> {
        match &self.dtype {
            DataType::Extension(name, storage_data_type, metadata) => Ok((
                name.clone(),
                Self {
                    dtype: *storage_data_type.clone(),
                },
                metadata.clone(),
            )),
            _ => Err(PyValueError::new_err(format!(
                "Data type is not an extension, but is: {}",
                self.dtype
            ))),
        }
    }

    pub fn is_equal(&self, other: Bound<PyAny>) -> PyResult<bool> {
        if other.is_instance_of::<Self>() {
            let other = other.extract::<Self>()?;
            Ok(self.dtype == other.dtype)
        } else {
            Ok(false)
        }
    }

    #[staticmethod]
    pub fn from_json(serialized: &str) -> PyResult<Self> {
        Ok(DataType::from_json(serialized)?.into())
    }

    #[must_use]
    pub fn __hash__(&self) -> u64 {
        use std::{
            collections::hash_map::DefaultHasher,
            hash::{Hash, Hasher},
        };
        let mut hasher = DefaultHasher::new();
        self.dtype.hash(&mut hasher);
        hasher.finish()
    }
}

impl_bincode_py_state_serialization!(PyDataType);

impl From<DataType> for PyDataType {
    fn from(value: DataType) -> Self {
        Self { dtype: value }
    }
}

impl From<PyDataType> for DataType {
    fn from(item: PyDataType) -> Self {
        item.dtype
    }
}
