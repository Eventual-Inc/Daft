use arrow2;
use arrow2::array;

use crate::{
    array::DataArray,
    datatypes::{
        logical::{DateArray, EmbeddingArray, FixedShapeImageArray, ImageArray, TimestampArray},
        BinaryArray, BooleanArray, DaftNumericType, FixedSizeListArray, ListArray, StructArray,
        Utf8Array,
    },
};

#[cfg(feature = "python")]
use crate::array::pseudo_arrow::PseudoArrowArray;
#[cfg(feature = "python")]
use crate::datatypes::PythonArray;

pub trait AsArrow {
    type Output;

    // Convert to an arrow2::array::Array.
    fn as_arrow(&self) -> &Self::Output;
}

impl<T> AsArrow for DataArray<T>
where
    T: DaftNumericType,
{
    type Output = array::PrimitiveArray<T::Native>;

    fn as_arrow(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for Utf8Array {
    type Output = array::Utf8Array<i64>;

    fn as_arrow(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for BooleanArray {
    type Output = array::BooleanArray;

    fn as_arrow(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for BinaryArray {
    type Output = array::BinaryArray<i64>;

    fn as_arrow(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for DateArray {
    type Output = array::PrimitiveArray<i32>;

    fn as_arrow(&self) -> &Self::Output {
        self.physical.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for TimestampArray {
    type Output = array::PrimitiveArray<i64>;

    fn as_arrow(&self) -> &Self::Output {
        self.physical.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for ListArray {
    type Output = array::ListArray<i64>;

    fn as_arrow(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for FixedSizeListArray {
    type Output = array::FixedSizeListArray;

    fn as_arrow(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for StructArray {
    type Output = array::StructArray;

    fn as_arrow(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

#[cfg(feature = "python")]
impl AsArrow for PythonArray {
    // Converts DataArray<PythonType> to PseudoArrowArray<PyObject>.
    type Output = PseudoArrowArray<pyo3::PyObject>;

    fn as_arrow(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for EmbeddingArray {
    type Output = array::FixedSizeListArray;

    // downcasts an EmbeddingArray to an Arrow FixedSizeListArray.
    fn as_arrow(&self) -> &Self::Output {
        self.physical.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for ImageArray {
    type Output = array::StructArray;

    // downcasts an ImageArray to an Arrow StructArray.
    fn as_arrow(&self) -> &Self::Output {
        self.physical.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for FixedShapeImageArray {
    type Output = array::FixedSizeListArray;

    // downcasts a FixedShapeImageArray to an Arrow FixedSizeListArray.
    fn as_arrow(&self) -> &Self::Output {
        self.physical.data().as_any().downcast_ref().unwrap()
    }
}
