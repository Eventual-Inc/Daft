use arrow2;
use arrow2::array;

use crate::{
    array::{BaseArray, DataArray},
    datatypes::{
        BinaryArray, BooleanArray, DaftNumericType, DateArray, FixedSizeListArray, Utf8Array,
    },
};

pub trait Downcastable {
    type Output;

    fn downcast(&self) -> &Self::Output;
}

impl<T> Downcastable for DataArray<T>
where
    T: DaftNumericType,
{
    type Output = array::PrimitiveArray<T::Native>;

    // downcasts a DataArray<T> to an Arrow PrimitiveArray.
    fn downcast(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl Downcastable for Utf8Array {
    type Output = array::Utf8Array<i64>;

    // downcasts a DataArray<T> to an Arrow Utf8Array.
    fn downcast(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl Downcastable for BooleanArray {
    type Output = array::BooleanArray;

    // downcasts a DataArray<T> to an Arrow BooleanArray.
    fn downcast(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl Downcastable for BinaryArray {
    type Output = array::BinaryArray<i64>;

    // downcasts a DataArray<T> to an Arrow BinaryArray.
    fn downcast(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl Downcastable for DateArray {
    type Output = array::PrimitiveArray<i32>;

    // downcasts a DataArray<T> to an Arrow DateArray.
    fn downcast(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl Downcastable for FixedSizeListArray {
    type Output = array::FixedSizeListArray;
    // downcasts a DataArray<T> to an Arrow Array.
    fn downcast(&self) -> &Self::Output {
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
