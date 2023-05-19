use arrow2;
use arrow2::array;

use crate::{
    array::DataArray,
    datatypes::{
        logical::{DateArray, EmbeddingArray},
        BinaryArray, BooleanArray, DaftNumericType, FixedSizeListArray, ListArray, StructArray,
        Utf8Array,
    },
};

pub trait AsArrow {
    type Output;

    fn as_arrow(&self) -> &Self::Output;
}

impl<T> AsArrow for DataArray<T>
where
    T: DaftNumericType,
{
    type Output = array::PrimitiveArray<T::Native>;

    // downcasts a DataArray<T> to an Arrow PrimitiveArray.
    fn as_arrow(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for Utf8Array {
    type Output = array::Utf8Array<i64>;

    // downcasts a DataArray<T> to an Arrow Utf8Array.
    fn as_arrow(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for BooleanArray {
    type Output = array::BooleanArray;

    // downcasts a DataArray<T> to an Arrow BooleanArray.
    fn as_arrow(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for BinaryArray {
    type Output = array::BinaryArray<i64>;

    // downcasts a DataArray<T> to an Arrow BinaryArray.
    fn as_arrow(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for DateArray {
    type Output = array::PrimitiveArray<i32>;

    // downcasts a DataArray<T> to an Arrow DateArray.
    fn as_arrow(&self) -> &Self::Output {
        self.physical.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for ListArray {
    type Output = array::ListArray<i64>;

    // downcasts a DataArray<T> to an Arrow ListArray.
    fn as_arrow(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for FixedSizeListArray {
    type Output = array::FixedSizeListArray;

    // downcasts a DataArray<T> to an Arrow FixedSizeListArray.
    fn as_arrow(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for StructArray {
    type Output = array::StructArray;

    // downcasts a DataArray<T> to an Arrow StructArray.
    fn as_arrow(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

#[cfg(feature = "python")]
impl AsArrow for crate::datatypes::PythonArray {
    type Output = crate::array::pseudo_arrow::PseudoArrowArray<pyo3::PyObject>;

    // downcasts a DataArray<T> to a PseudoArrowArray of PyObject.
    fn as_arrow(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for EmbeddingArray {
    type Output = array::FixedSizeListArray;

    // downcasts a DataArray<T> to an Arrow FixedSizeListArray.
    fn as_arrow(&self) -> &Self::Output {
        self.physical.data().as_any().downcast_ref().unwrap()
    }
}
