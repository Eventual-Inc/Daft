use std::sync::Arc;

use arrow2::{array, types::months_days_ns};

#[cfg(feature = "python")]
use crate::array::pseudo_arrow::PseudoArrowArray;
#[cfg(feature = "python")]
use crate::datatypes::PythonArray;
use crate::{
    array::DataArray,
    datatypes::{
        BinaryArray, BooleanArray, DaftPrimitiveType, FixedSizeBinaryArray, IntervalArray,
        NullArray, Utf8Array,
        logical::{DateArray, DurationArray, TimeArray, TimestampArray},
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
                self.data().as_any().downcast_ref().unwrap_or_else(|| {
                    panic!(
                        "Failed to downcast {} array to expected type {}. Actual type: {:?}",
                        stringify!($da),
                        stringify!($output),
                        self.data().data_type()
                    )
                })
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
// AsArrow implementation for Utf8Array that handles both i32 and i64 offsets based on the DataType
// For DataType::Utf8, returns Utf8Array<i32>; for DataType::LargeUtf8, returns Utf8Array<i64>
impl AsArrow for Utf8Array {
    type Output = array::Utf8Array<i64>;
    fn as_arrow(&self) -> &Self::Output {
        match self.data_type() {
            crate::datatypes::DataType::Utf8 => {
                // For DataType::Utf8, we expect Utf8Array<i32>, but AsArrow signature requires i64
                // We need to ensure DataArray::new converts i32 to i64 when creating Utf8Array
                if let Some(large_utf8) = self.data().as_any().downcast_ref::<array::Utf8Array<i64>>() {
                    return large_utf8;
                }
                panic!(
                    "DataType::Utf8 should contain Utf8Array<i64> after conversion in DataArray::new. \
                     Found: {:?}. This indicates DataArray::new failed to convert i32 to i64 offsets.",
                    self.data().data_type()
                )
            }
            crate::datatypes::DataType::LargeUtf8 => {
                // For DataType::LargeUtf8, we expect Utf8Array<i64> directly
                if let Some(large_utf8) = self.data().as_any().downcast_ref::<array::Utf8Array<i64>>() {
                    return large_utf8;
                }
                panic!(
                    "DataType::LargeUtf8 should contain Utf8Array<i64>. \
                     Found: {:?}. This indicates a type system inconsistency.",
                    self.data().data_type()
                )
            }
            _ => panic!(
                "Utf8Array should only have DataType::Utf8 or DataType::LargeUtf8, found: {:?}",
                self.data_type()
            )
        }
    }
}
impl_asarrow_dataarray!(BooleanArray, array::BooleanArray);
impl_asarrow_dataarray!(BinaryArray, array::BinaryArray<i64>);
impl_asarrow_dataarray!(FixedSizeBinaryArray, array::FixedSizeBinaryArray);
impl_asarrow_dataarray!(IntervalArray, array::PrimitiveArray<months_days_ns>);

#[cfg(feature = "python")]
impl_asarrow_dataarray!(PythonArray, PseudoArrowArray<Arc<pyo3::PyObject>>);

impl_asarrow_logicalarray!(DateArray, array::PrimitiveArray<i32>);
impl_asarrow_logicalarray!(TimeArray, array::PrimitiveArray<i64>);
impl_asarrow_logicalarray!(DurationArray, array::PrimitiveArray<i64>);
impl_asarrow_logicalarray!(TimestampArray, array::PrimitiveArray<i64>);
