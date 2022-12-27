use std::any::Any;

use pyo3::PyObject;

use crate::{
    datatypes::DataType,
    error::{DaftError, DaftResult},
};

// enum DataArray {
//     Arrow(Box<dyn arrow2::array::Array>),
//     Python(Box<Vec<PyObject>>),
//     Empty
// }
// impl From<Box<dyn arrow2::array::Array>> for DataArray {
//     fn from(item: Box<dyn arrow2::array::Array>) -> Self {
//         DataArray::Arrow(item)
//     }
// }

trait DataArray: Any {
    fn data_type(&self) -> DataType;

    fn add(&self, other: &dyn DataArray) -> DaftResult<Box<dyn DataArray>>;

    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        &self
    }
}

struct ArrowDataArray {
    data: Box<dyn arrow2::array::Array>,
}

impl From<Box<dyn arrow2::array::Array>> for ArrowDataArray {
    fn from(item: Box<dyn arrow2::array::Array>) -> Self {
        ArrowDataArray { data: item }
    }
}

impl DataArray for ArrowDataArray {
    fn data_type(&self) -> DataType {
        return DataType::Arrow(self.data.data_type().clone());
    }

    fn add(&self, other: &dyn DataArray) -> DaftResult<Box<dyn DataArray>> {
        Ok(Box::from(ArrowDataArray::from(
            arrow2::compute::arithmetics::add(
                self.data.as_ref(),
                other
                    .as_any()
                    .downcast_ref::<ArrowDataArray>()
                    .unwrap()
                    .data
                    .as_ref(),
            ),
        )))
    }
}

pub struct Series {
    array: Box<dyn DataArray>,
}

impl From<Box<dyn arrow2::array::Array>> for Series {
    fn from(item: Box<dyn arrow2::array::Array>) -> Self {
        Series {
            array: Box::from(ArrowDataArray::from(item)),
        }
    }
}

impl Series {
    #[inline]
    fn check_metatype(&self, other: &Series) -> DaftResult<()> {
        match (self.array.data_type(), other.array.data_type()) {
            (DataType::Arrow(_), DataType::Arrow(_)) => Ok(()),
            (left, right) => Err(DaftError::TypeError(format!(
                "Metatype mismatch: {:?} vs {:?}",
                left, right
            ))),
        }
    }

    pub fn add(&self, other: &Series) -> DaftResult<Series> {
        self.check_metatype(other)?;
        Ok(Series {
            array: self.array.add(other.array.as_ref())?,
        })
    }
}
