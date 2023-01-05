use std::{any::Any, sync::Arc};

use crate::{
    array::data_array::{BaseArray, DataArray},
    datatypes,
    datatypes::DataType,
};

#[derive(Debug, Clone)]
pub struct Series {
    array: Arc<dyn BaseArray>,
}

impl From<Box<dyn arrow2::array::Array>> for Series {
    fn from(item: Box<dyn arrow2::array::Array>) -> Self {
        match item.data_type().into() {
            DataType::Int64 => Series {
                array: Arc::from(DataArray::<datatypes::Int64Type>::from(item)),
            },
            DataType::Utf8 => Series {
                array: Arc::from(DataArray::<datatypes::Utf8Type>::from(item)),
            },
            _ => panic!("help!"),
        }
    }
}

impl Series {}

#[cfg(test)]
mod tests {}
