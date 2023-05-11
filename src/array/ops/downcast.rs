use arrow2;
use arrow2::array;

use crate::{
    array::DataArray,
    datatypes::{
        BinaryArray, BooleanArray, DaftNumericType, DateArray, FixedSizeListArray, ListArray,
        StructArray, Utf8Array,
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

impl Downcastable for ListArray {
    type Output = array::ListArray<i64>;

    // downcasts a DataArray<T> to an Arrow ListArray.
    fn downcast(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl Downcastable for FixedSizeListArray {
    type Output = array::FixedSizeListArray;

    // downcasts a DataArray<T> to an Arrow FixedSizeListArray.
    fn downcast(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl Downcastable for StructArray {
    type Output = array::StructArray;

    // downcasts a DataArray<T> to an Arrow StructArray.
    fn downcast(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

#[cfg(feature = "python")]
impl Downcastable for crate::datatypes::PythonArray {
    type Output = crate::array::pseudo_arrow::PseudoArrowArray<pyo3::PyObject>;

    // downcasts a DataArray<T> to a PseudoArrowArray of PyObject.
    fn downcast(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}
