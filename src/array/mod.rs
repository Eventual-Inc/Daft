pub mod from;
pub mod iterator;
pub mod nested;
pub mod ops;
pub mod pseudo_arrow;
use std::{any::Any, marker::PhantomData, sync::Arc};

use crate::{
    datatypes::{DaftDataType, DataType, Field},
    error::{DaftError, DaftResult},
    series::Series,
};

pub trait BaseArray: Any + Send + Sync {
    fn data(&self) -> &dyn arrow2::array::Array;

    fn data_type(&self) -> &DataType;

    fn name(&self) -> &str;

    fn rename(&self, name: &str) -> Box<dyn BaseArray>;

    fn field(&self) -> &Field;

    fn len(&self) -> usize;

    fn as_any(&self) -> &dyn std::any::Any;

    fn boxed(self) -> Box<dyn BaseArray>;

    fn arced(self) -> Arc<dyn BaseArray>;

    fn into_series(self) -> Series;
}

impl std::fmt::Debug for dyn BaseArray {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.data_type())
    }
}

#[derive(Debug)]
pub struct DataArray<T: DaftDataType> {
    field: Arc<Field>,
    data: Box<dyn arrow2::array::Array>,
    marker_: PhantomData<T>,
}

impl<T: DaftDataType> Clone for DataArray<T> {
    fn clone(&self) -> Self {
        DataArray::new(self.field.clone(), self.data.clone()).unwrap()
    }
}

impl<T> DataArray<T>
where
    T: DaftDataType,
{
    pub fn new(field: Arc<Field>, data: Box<dyn arrow2::array::Array>) -> DaftResult<DataArray<T>> {
        if let Ok(arrow_dtype) = field.dtype.to_arrow() {
            if !arrow_dtype.eq(data.data_type()) {
                return Err(DaftError::SchemaMismatch(format!(
                    "expected {:?}, got {:?}",
                    arrow_dtype,
                    data.data_type()
                )));
            }
        }

        Ok(DataArray {
            field,
            data,
            marker_: PhantomData,
        })
    }

    pub fn with_validity(&self, validity: &[bool]) -> DaftResult<Self> {
        if validity.len() != self.data.len() {
            return Err(DaftError::ValueError(format!(
                "validity mask length does not match DataArray length, {} vs {}",
                validity.len(),
                self.data.len()
            )));
        }
        use arrow2::bitmap::Bitmap;
        let with_bitmap = self.data.with_validity(Some(Bitmap::from(validity)));
        DataArray::new(self.field.clone(), with_bitmap)
    }

    pub fn slice(&self, start: usize, end: usize) -> DaftResult<Self> {
        if start > end {
            return Err(DaftError::ValueError(format!(
                "Trying to slice array with negative length, start: {start} vs end: {end}"
            )));
        }
        let sliced = self.data.sliced(start, end - start);
        Self::new(self.field.clone(), sliced)
    }

    pub fn head(&self, num: usize) -> DaftResult<Self> {
        self.slice(0, num)
    }
}

impl<T: DaftDataType + 'static> BaseArray for DataArray<T> {
    fn data(&self) -> &dyn arrow2::array::Array {
        self.data.as_ref()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> &DataType {
        &self.field.dtype
    }

    fn name(&self) -> &str {
        self.field.name.as_str()
    }

    fn rename(&self, name: &str) -> Box<dyn BaseArray> {
        Box::new(Self::new(Arc::new(self.field.rename(name)), self.data.clone()).unwrap())
    }

    fn field(&self) -> &Field {
        &self.field
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn boxed(self) -> Box<dyn BaseArray> {
        Box::new(self)
    }

    fn arced(self) -> Arc<dyn BaseArray> {
        Arc::new(self)
    }

    fn into_series(self) -> Series {
        Series::new(self.arced())
    }
}
