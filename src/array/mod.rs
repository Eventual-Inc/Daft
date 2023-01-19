pub mod from;
pub mod ops;

use std::{any::Any, marker::PhantomData, sync::Arc};

use crate::{
    datatypes::{DaftDataType, DataType, Field},
    error::{DaftError, DaftResult},
    series::Series,
};

pub trait BaseArray: Any {
    fn data(&self) -> &dyn arrow2::array::Array;

    fn data_type(&self) -> &DataType;

    fn name(&self) -> &str;

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

#[derive(Debug, Clone)]
pub struct DataArray<T: DaftDataType> {
    field: Arc<Field>,
    data: Arc<dyn arrow2::array::Array>,
    marker_: PhantomData<T>,
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
