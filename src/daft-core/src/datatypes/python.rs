use std::{fmt, ops::Deref, sync::Arc};

use arrow::{array::ArrayRef, buffer::NullBuffer};
use common_error::{DaftError, DaftResult};
use common_py_serde::pickle_dumps;
use daft_schema::{dtype::DataType, field::Field};
use pyo3::{Py, PyAny, PyResult, Python};

use crate::{
    prelude::DaftArrayType,
    series::{ArrayWrapper, IntoSeries, Series},
};

/// A lightweight buffer for holding `Arc<Py<PyAny>>` values, replacing
/// `arrow2::buffer::Buffer<Arc<Py<PyAny>>>` which cannot be represented
/// by arrow-rs's byte-oriented `Buffer`.
#[derive(Clone)]
pub struct PythonBuffer {
    data: Arc<Vec<Arc<Py<PyAny>>>>,
    offset: usize,
    length: usize,
}

impl PythonBuffer {
    /// Returns an O(1) sliced view of this buffer.
    pub fn sliced(self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.length,
            "slice {}..{} out of bounds for buffer of length {}",
            offset,
            offset + length,
            self.length
        );
        Self {
            data: self.data,
            offset: self.offset + offset,
            length,
        }
    }

    pub fn len(&self) -> usize {
        self.length
    }
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }
}

impl From<Vec<Arc<Py<PyAny>>>> for PythonBuffer {
    fn from(vec: Vec<Arc<Py<PyAny>>>) -> Self {
        let length = vec.len();
        Self {
            data: Arc::new(vec),
            offset: 0,
            length,
        }
    }
}

impl FromIterator<Arc<Py<PyAny>>> for PythonBuffer {
    fn from_iter<I: IntoIterator<Item = Arc<Py<PyAny>>>>(iter: I) -> Self {
        Vec::from_iter(iter).into()
    }
}

impl Deref for PythonBuffer {
    type Target = [Arc<Py<PyAny>>];

    fn deref(&self) -> &[Arc<Py<PyAny>>] {
        &self.data[self.offset..self.offset + self.length]
    }
}

impl fmt::Debug for PythonBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PythonBuffer")
            .field("len", &self.length)
            .field("offset", &self.offset)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub struct PythonArray {
    field: Arc<Field>,
    values: PythonBuffer,
    nulls: Option<NullBuffer>,
}

impl IntoSeries for PythonArray {
    fn into_series(self) -> Series {
        Series {
            inner: Arc::new(ArrayWrapper(self)),
        }
    }
}

impl PythonArray {
    /// Create a new PythonArray.
    ///
    /// Elements in `values` that are None must have validity set to false.
    pub fn new(field: Arc<Field>, values: PythonBuffer, nulls: Option<NullBuffer>) -> Self {
        assert_eq!(
            field.dtype,
            DataType::Python,
            "Can only construct PythonArray for Python data type, got: {}",
            field.dtype
        );
        if let Some(v) = &nulls {
            assert_eq!(
                values.len(),
                v.len(),
                "validity mask length must match PythonArray length, got: {} vs {}",
                v.len(),
                values.len()
            );
        }

        debug_assert!(
            values.iter().enumerate().all(|(i, v)| {
                !(Python::attach(|py| v.is_none(py))
                    && nulls.as_ref().is_none_or(|val| val.is_valid(i)))
            }),
            "None values must have validity set to false"
        );

        Self {
            field,
            values,
            nulls,
        }
    }

    #[deprecated(note = "arrow2 migration")]
    pub fn to_pickled_arrow2(&self) -> DaftResult<daft_arrow::array::BinaryArray<i64>> {
        let pickled = Python::attach(|py| {
            self.iter()
                .map(|v| v.map(|obj| pickle_dumps(py, obj)).transpose())
                .collect::<PyResult<Vec<_>>>()
        })?;

        Ok(daft_arrow::array::BinaryArray::from(pickled))
    }

    pub fn to_pickled_arrow(&self) -> DaftResult<arrow::array::LargeBinaryArray> {
        Python::attach(|py| {
            self.iter()
                .map(|v| v.map(|obj| pickle_dumps(py, obj)).transpose())
                .collect::<PyResult<arrow::array::LargeBinaryArray>>()
        })
        .map_err(DaftError::from)
    }

    #[deprecated(note = "arrow2 migration")]
    pub fn to_arrow2(&self) -> DaftResult<Box<dyn daft_arrow::array::Array>> {
        let arrow_logical_type = self.data_type().to_arrow2().unwrap();
        let physical_arrow_array = self.to_pickled_arrow2()?;
        let logical_arrow_array = daft_arrow::array::Array::convert_logical_type(
            &physical_arrow_array,
            arrow_logical_type,
        );
        Ok(logical_arrow_array)
    }

    pub fn to_arrow(&self) -> DaftResult<ArrayRef> {
        // unlike arrow2, the extension type is stored in the field, so we can just directly return the physical array
        self.to_pickled_arrow().map(|arr| Arc::new(arr) as _)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn with_nulls(&self, nulls: Option<NullBuffer>) -> DaftResult<Self> {
        self.clone().set_nulls(nulls)
    }

    fn set_nulls(mut self, nulls: Option<NullBuffer>) -> DaftResult<Self> {
        if let Some(v) = &nulls
            && v.len() != self.len()
        {
            return Err(DaftError::ValueError(format!(
                "validity mask length does not match PythonArray length, {} vs {}",
                v.len(),
                self.len()
            )));
        }
        self.nulls = nulls;
        Ok(self)
    }

    pub fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    pub fn slice(&self, start: usize, end: usize) -> DaftResult<Self> {
        if start > end {
            return Err(DaftError::ValueError(format!(
                "Trying to slice array with negative length, start: {start} vs end: {end}"
            )));
        }

        let length = end - start;
        let new_values = self.values.clone().sliced(start, length);
        let new_nulls = self.nulls.clone().map(|v| v.slice(start, length));
        Ok(Self::new(self.field.clone(), new_values, new_nulls))
    }

    pub fn values(&self) -> &PythonBuffer {
        &self.values
    }

    pub fn field(&self) -> &Field {
        &self.field
    }

    pub fn name(&self) -> &str {
        &self.field.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.field.dtype
    }

    pub fn null_count(&self) -> usize {
        self.nulls().map_or(0, |v| v.null_count())
    }

    pub fn is_valid(&self, idx: usize) -> bool {
        self.nulls().is_none_or(|v| v.is_valid(idx))
    }

    pub fn rename(&self, name: &str) -> Self {
        Self::new(
            Arc::new(self.field.rename(name)),
            self.values.clone(),
            self.nulls.clone(),
        )
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn head(&self, num: usize) -> DaftResult<Self> {
        self.slice(0, num)
    }

    pub fn iter(&self) -> impl Iterator<Item = Option<&Arc<Py<PyAny>>>> {
        let nulls = self.nulls.clone();
        self.values.iter().enumerate().map(move |(i, v)| {
            if nulls.as_ref().is_none_or(|n| n.is_valid(i)) {
                Some(v)
            } else {
                None
            }
        })
    }
}

impl DaftArrayType for PythonArray {
    fn data_type(&self) -> &DataType {
        self.data_type()
    }
}
