pub mod from;
pub mod iterator;
pub mod ops;

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
    data: Arc<dyn arrow2::array::Array>,
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
    pub fn new(field: Arc<Field>, data: Arc<dyn arrow2::array::Array>) -> DaftResult<DataArray<T>> {
        if !field.dtype.to_arrow()?.eq(data.data_type()) {
            return Err(DaftError::SchemaMismatch(format!(
                "expected {:?}, got {:?}",
                field.dtype.to_arrow(),
                data.data_type()
            )));
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
        DataArray::new(self.field.clone(), with_bitmap.into())
    }

    pub fn head(&self, num: usize) -> DaftResult<Self> {
        let sliced = self.data.slice(0, num);
        Self::new(self.field.clone(), Arc::from(sliced))
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
