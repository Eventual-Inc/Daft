use arrow2;
use arrow2::array;

use crate::{
    array::DataArray,
    datatypes::{
        logical::{
            DateArray, Decimal128Array, DurationArray, EmbeddingArray, FixedShapeImageArray,
            FixedShapeTensorArray, ImageArray, TensorArray, TimestampArray,
        },
        nested_arrays::FixedSizeListArray,
        BinaryArray, BooleanArray, DaftNumericType, ListArray, StructArray, Utf8Array,
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

macro_rules! impl_asarrow_dataarray {
    ($da:ident, $output:ty) => {
        impl AsArrow for $da {
            type Output = $output;
            fn as_arrow(&self) -> &Self::Output {
                self.data().as_any().downcast_ref().unwrap()
            }
        }
    };
}

macro_rules! impl_asarrow_logicalarray {
    ($da:ident, $output:ty) => {
        impl AsArrow for $da {
            type Output = $output;
            fn as_arrow(&self) -> &Self::Output {
                self.physical.data().as_any().downcast_ref().unwrap()
            }
        }
    };
}

macro_rules! impl_asarrow_logical_fsl_array {
    ($da:ident, $output:ty) => {
        impl AsArrow for $da {
            type Output = $output;
            fn as_arrow(&self) -> &Self::Output {
                // TODO(FixedSizeList): Fix kernels that rely on this functionality
                unimplemented!("as_arrow for FixedSizeListArray will not be implemented")
            }
        }
    };
}

impl AsArrow for FixedSizeListArray {
    type Output = array::FixedSizeListArray;
    fn as_arrow(&self) -> &Self::Output {
        // TODO(FixedSizeList): Fix kernels that rely on this functionality
        unimplemented!("as_arrow for FixedSizeListArray will not be implemented")
    }
}

impl_asarrow_dataarray!(Utf8Array, array::Utf8Array<i64>);
impl_asarrow_dataarray!(BooleanArray, array::BooleanArray);
impl_asarrow_dataarray!(BinaryArray, array::BinaryArray<i64>);
impl_asarrow_dataarray!(ListArray, array::ListArray<i64>);
impl_asarrow_dataarray!(StructArray, array::StructArray);

#[cfg(feature = "python")]
impl_asarrow_dataarray!(PythonArray, PseudoArrowArray<pyo3::PyObject>);

impl_asarrow_logicalarray!(Decimal128Array, array::PrimitiveArray<i128>);
impl_asarrow_logicalarray!(DateArray, array::PrimitiveArray<i32>);
impl_asarrow_logicalarray!(DurationArray, array::PrimitiveArray<i64>);
impl_asarrow_logicalarray!(TimestampArray, array::PrimitiveArray<i64>);
impl_asarrow_logicalarray!(ImageArray, array::StructArray);
impl_asarrow_logicalarray!(TensorArray, array::StructArray);

impl_asarrow_logical_fsl_array!(EmbeddingArray, array::FixedSizeListArray);
impl_asarrow_logical_fsl_array!(FixedShapeImageArray, array::FixedSizeListArray);
impl_asarrow_logical_fsl_array!(FixedShapeTensorArray, array::FixedSizeListArray);
