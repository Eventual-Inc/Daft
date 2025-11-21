use std::sync::Arc;

use arrow2::{
    array::Array,
    bitmap::{Bitmap, utils::ZipValidity},
    buffer::Buffer,
};
use common_error::{DaftError, DaftResult};
use common_py_serde::pickle_dumps;
use daft_schema::{dtype::DataType, field::Field};
use pyo3::{Py, PyAny, PyResult, Python};

use crate::{
    prelude::DaftArrayType,
    series::{ArrayWrapper, IntoSeries, Series},
};

#[derive(Debug, Clone)]
pub struct PythonArray {
    field: Arc<Field>,
    values: Buffer<Arc<Py<PyAny>>>,
    validity: Option<Bitmap>,
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
    pub fn new(
        field: Arc<Field>,
        values: Buffer<Arc<Py<PyAny>>>,
        validity: Option<Bitmap>,
    ) -> Self {
        assert_eq!(
            field.dtype,
            DataType::Python,
            "Can only construct PythonArray for Python data type, got: {}",
            field.dtype
        );
        if let Some(v) = &validity {
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
                    && validity.as_ref().is_none_or(|val| val.get_bit(i)))
            }),
            "None values must have validity set to false"
        );

        Self {
            field,
            values,
            validity,
        }
    }

    pub fn to_pickled_arrow(&self) -> DaftResult<arrow2::array::BinaryArray<i64>> {
        let pickled = Python::attach(|py| {
            self.iter()
                .map(|v| v.map(|obj| pickle_dumps(py, obj)).transpose())
                .collect::<PyResult<Vec<_>>>()
        })?;

        Ok(arrow2::array::BinaryArray::from(pickled))
    }

    pub fn to_arrow(&self) -> DaftResult<Box<dyn arrow2::array::Array>> {
        let arrow_logical_type = self.data_type().to_arrow().unwrap();
        let physical_arrow_array = self.to_pickled_arrow()?;
        let logical_arrow_array = physical_arrow_array.convert_logical_type(arrow_logical_type);
        Ok(logical_arrow_array)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn with_validity(&self, validity: Option<Bitmap>) -> DaftResult<Self> {
        self.clone().set_validity(validity)
    }

    fn set_validity(mut self, validity: Option<Bitmap>) -> DaftResult<Self> {
        if let Some(v) = &validity
            && v.len() != self.len()
        {
            return Err(DaftError::ValueError(format!(
                "validity mask length does not match PythonArray length, {} vs {}",
                v.len(),
                self.len()
            )));
        }
        self.validity = validity;
        Ok(self)
    }

    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    pub fn slice(&self, start: usize, end: usize) -> DaftResult<Self> {
        if start > end {
            return Err(DaftError::ValueError(format!(
                "Trying to slice array with negative length, start: {start} vs end: {end}"
            )));
        }

        let length = end - start;
        let new_values = self.values.clone().sliced(start, length);
        let new_validity = self.validity.clone().map(|v| v.sliced(start, length));
        Ok(Self::new(self.field.clone(), new_values, new_validity))
    }

    pub fn values(&self) -> &Buffer<Arc<Py<PyAny>>> {
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
        self.validity().map_or(0, |v| v.unset_bits())
    }

    pub fn is_valid(&self, idx: usize) -> bool {
        self.validity().is_none_or(|v| v.get_bit(idx))
    }

    pub fn rename(&self, name: &str) -> Self {
        Self::new(
            Arc::new(self.field.rename(name)),
            self.values.clone(),
            self.validity.clone(),
        )
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn head(&self, num: usize) -> DaftResult<Self> {
        self.slice(0, num)
    }

    pub fn iter(&self) -> impl Iterator<Item = Option<&Arc<Py<PyAny>>>> {
        ZipValidity::new_with_validity(self.values.iter(), self.validity())
    }
}

impl DaftArrayType for PythonArray {
    fn data_type(&self) -> &DataType {
        self.data_type()
    }
}
