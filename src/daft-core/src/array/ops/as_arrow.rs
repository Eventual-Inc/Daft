use arrow2::{array, types::months_days_ns};

#[cfg(feature = "python")]
use crate::array::pseudo_arrow::PseudoArrowArray;
#[cfg(feature = "python")]
use crate::datatypes::PythonArray;
use crate::{
    array::DataArray,
    datatypes::{
        logical::{DateArray, DurationArray, TimeArray, TimestampArray},
        BinaryArray, BooleanArray, DaftPrimitiveType, FixedSizeBinaryArray, IntervalArray,
        NullArray, Utf8Array,
    },
};

pub trait AsArrow {
    type Output;

    /// This does not correct for the logical types and will just yield the physical type of the array.
    /// For example, a TimestampArray will yield an arrow Int64Array rather than a arrow Timestamp Array.
    /// To get a corrected arrow type, see `.to_arrow()`.
    fn as_arrow(&self) -> &Self::Output;
}

impl<T> AsArrow for DataArray<T>
where
    T: DaftPrimitiveType,
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

impl_asarrow_dataarray!(NullArray, array::NullArray);
impl_asarrow_dataarray!(Utf8Array, array::Utf8Array<i64>);
impl_asarrow_dataarray!(BooleanArray, array::BooleanArray);
impl_asarrow_dataarray!(BinaryArray, array::BinaryArray<i64>);
impl_asarrow_dataarray!(FixedSizeBinaryArray, array::FixedSizeBinaryArray);
impl_asarrow_dataarray!(IntervalArray, array::PrimitiveArray<months_days_ns>);

#[cfg(feature = "python")]
impl_asarrow_dataarray!(PythonArray, PseudoArrowArray<pyo3::PyObject>);

impl_asarrow_logicalarray!(DateArray, array::PrimitiveArray<i32>);
impl_asarrow_logicalarray!(TimeArray, array::PrimitiveArray<i64>);
impl_asarrow_logicalarray!(DurationArray, array::PrimitiveArray<i64>);
impl_asarrow_logicalarray!(TimestampArray, array::PrimitiveArray<i64>);
