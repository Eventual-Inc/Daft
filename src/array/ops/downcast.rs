use arrow2;
use arrow2::array;

use crate::{
    array::{BaseArray, DataArray},
    datatypes::{BinaryArray, BooleanArray, DaftNumericType, DateArray, Utf8Array},
};

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    // downcasts a DataArray<T> to an Arrow PrimitiveArray.
    pub fn downcast(&self) -> &array::PrimitiveArray<T::Native> {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl Utf8Array {
    // downcasts a DataArray<T> to an Arrow Utf8Array.
    pub fn downcast(&self) -> &array::Utf8Array<i64> {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl BooleanArray {
    // downcasts a DataArray<T> to an Arrow BooleanArray.
    pub fn downcast(&self) -> &array::BooleanArray {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl BinaryArray {
    // downcasts a DataArray<T> to an Arrow BinaryArray.
    pub fn downcast(&self) -> &array::BinaryArray<i64> {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl DateArray {
    // downcasts a DataArray<T> to an Arrow DateArray.
    pub fn downcast(&self) -> &array::PrimitiveArray<i32> {
        self.data().as_any().downcast_ref().unwrap()
    }
}

#[cfg(feature = "python")]
impl crate::datatypes::PythonArray {
    // downcasts a DataArray<T> to a VecBackedArray of PyObject.
    pub fn downcast(&self) -> &crate::array::vec_backed::VecBackedArray<pyo3::PyObject> {
        self.data().as_any().downcast_ref().unwrap()
    }
}
