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

    // Retrieve the underlying concrete Arrow2 array.
    fn as_arrow(&self) -> &Self::Output;
}

impl<T> AsArrow for DataArray<T>
where
    T: DaftNumericType,
{
    type Output = array::PrimitiveArray<T::Native>;

    // For DataArray<T: DaftNumericType>, retrieve the underlying Arrow2 PrimitiveArray.
    fn as_arrow(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for Utf8Array {
    type Output = array::Utf8Array<i64>;

    // For DataArray<Utf8Type>, retrieve the underlying Arrow2 Utf8Array.
    fn as_arrow(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for BooleanArray {
    type Output = array::BooleanArray;

    // For DataArray<BooleanType>, retrieve the underlying Arrow2 BooleanArray.
    fn as_arrow(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for BinaryArray {
    type Output = array::BinaryArray<i64>;

    // For DataArray<BinaryType>, retrieve the underlying Arrow2 BinaryArray.
    fn as_arrow(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for DateArray {
    type Output = array::PrimitiveArray<i32>;

    // For LogicalArray<DateType>, retrieve the underlying Arrow2 i32 array.
    fn as_arrow(&self) -> &Self::Output {
        self.physical.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for TimestampArray {
    type Output = array::PrimitiveArray<i64>;

    // For LogicalArray<TimestampType>, retrieve the underlying Arrow2 i64 array.
    fn as_arrow(&self) -> &Self::Output {
        self.physical.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for ListArray {
    type Output = array::ListArray<i64>;

    // For DataArray<ListType>, retrieve the underlying Arrow2 ListArray.
    fn as_arrow(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for FixedSizeListArray {
    type Output = array::FixedSizeListArray;

    // For DataArray<DateType>, retrieve the underlying Arrow2 i32 array.
    fn as_arrow(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for StructArray {
    type Output = array::StructArray;

    // For DataArray<StructType>, retrieve the underlying Arrow2 StructArray.
    fn as_arrow(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

#[cfg(feature = "python")]
impl AsArrow for PythonArray {
    type Output = PseudoArrowArray<pyo3::PyObject>;

    // For DataArray<PythonType>, retrieve the underlying PseudoArrowArray<PyObject>.
    fn as_arrow(&self) -> &Self::Output {
        self.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for EmbeddingArray {
    type Output = array::FixedSizeListArray;

    // For LogicalArray<EmbeddingType>, retrieve the underlying Arrow2 FixedSizeListArray.
    fn as_arrow(&self) -> &Self::Output {
        self.physical.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for ImageArray {
    type Output = array::StructArray;

    // For LogicalArray<ImageType>, retrieve the underlying Arrow2 StructArray.
    fn as_arrow(&self) -> &Self::Output {
        self.physical.data().as_any().downcast_ref().unwrap()
    }
}

impl AsArrow for FixedShapeImageArray {
    type Output = array::FixedSizeListArray;

    // For LogicalArray<FixedShapeImageType>, retrieve the underlying Arrow2 FixedSizeListArray.
    fn as_arrow(&self) -> &Self::Output {
        self.physical.data().as_any().downcast_ref().unwrap()
    }
}
