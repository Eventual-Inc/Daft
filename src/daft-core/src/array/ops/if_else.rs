use crate::array::DataArray;
use crate::datatypes::logical::{
    DateArray, Decimal128Array, DurationArray, EmbeddingArray, FixedShapeImageArray,
    FixedShapeTensorArray, ImageArray, TensorArray, TimestampArray,
};
use crate::datatypes::{
    BinaryArray, BooleanArray, DaftArrowBackedType, DaftNumericType, ExtensionArray, Field,
    FixedSizeListArray, ListArray, NullArray, StructArray, Utf8Array,
};
use crate::utils::arrow::arrow_bitmap_and_helper;
use common_error::{DaftError, DaftResult};
use std::convert::identity;
use std::sync::Arc;

use super::as_arrow::AsArrow;
use super::broadcast::Broadcastable;

#[cfg(feature = "python")]
use crate::datatypes::PythonArray;

// Helper macro for broadcasting if/else across the if_true/if_false/predicate DataArrays
//
// `array_type`: the Arrow2 array type that if_true/if_false should be downcasted to
// `if_true/if_false`: the DataArrays to select data from, conditional on the predicate
// `predicate`: the predicate boolean DataArray
// `scalar_copy`: a simple inlined function to run on each borrowed scalar value when iterating through if_true/if_else.
//   Note that this is potentially the identity function if Arrow2 allows for creation of this Array from borrowed data (e.g. &str)
macro_rules! broadcast_if_else{(
    $array_type:ty, $if_true:expr, $if_false:expr, $predicate:expr, $scalar_copy:expr, $if_then_else:expr,
) => ({
    match ($if_true.len(), $if_false.len(), $predicate.len()) {
        // CASE: Equal lengths across all 3 arguments
        (self_len, other_len, predicate_len) if self_len == other_len && other_len == predicate_len => {
            let result = $if_then_else($predicate.as_arrow(), $if_true.data(), $if_false.data())?;
            DataArray::try_from(($if_true.field.clone(), result))
        },
        // CASE: Broadcast predicate
        (self_len, _, 1) => {
            let predicate_scalar = $predicate.get(0);
            match predicate_scalar {
                None => Ok(DataArray::full_null($if_true.name(), $if_true.data_type(), self_len)),
                Some(predicate_scalar_value) => {
                    if predicate_scalar_value {
                        Ok($if_true.clone())
                    } else {
                        Ok($if_false.clone())
                    }
                }
            }
        }
        // CASE: Broadcast both arrays
        (1, 1, _) => {
            let self_scalar = $if_true.get(0);
            let other_scalar = $if_false.get(0);
            let predicate_arr = $predicate.as_arrow();
            let predicate_values = predicate_arr.values();
            let naive_if_else: $array_type = predicate_values.iter().map(
                |pred_val| match pred_val {
                    true => self_scalar,
                    false => other_scalar,
                }
            ).collect();
            let validity = arrow_bitmap_and_helper(predicate_arr.validity(), naive_if_else.validity());
            DataArray::new($if_true.field.clone(), Box::new(naive_if_else.with_validity(validity)))
        }
        // CASE: Broadcast truthy array
        (1, o, p)  if o == p => {
            let self_scalar = $if_true.get(0);
            let predicate_arr = $predicate.as_arrow();
            let predicate_values = predicate_arr.values();
            let naive_if_else: $array_type = $if_false.as_arrow().iter().zip(predicate_values.iter()).map(
                |(other_val, pred_val)| match pred_val {
                    true => self_scalar,
                    false => $scalar_copy(other_val),
                }
            ).collect();
            let validity = arrow_bitmap_and_helper(predicate_arr.validity(), naive_if_else.validity());
            DataArray::new($if_true.field.clone(), Box::new(naive_if_else.with_validity(validity)))
        }
        // CASE: Broadcast falsey array
        (s, 1, p)  if s == p => {
            let other_scalar = $if_false.get(0);
            let predicate_arr = $predicate.as_arrow();
            let predicate_values = predicate_arr.values();
            let naive_if_else: $array_type = $if_true.as_arrow().iter().zip(predicate_values.iter()).map(
                |(self_val, pred_val)| match pred_val {
                    true => $scalar_copy(self_val),
                    false => other_scalar,
                }
            ).collect();
            let validity = arrow_bitmap_and_helper(predicate_arr.validity(), naive_if_else.validity());
            DataArray::new($if_true.field.clone(), Box::new(naive_if_else.with_validity(validity)))
        }
        (s, o, p) => Err(DaftError::ValueError(format!("Cannot run if_else against arrays with non-broadcastable lengths: self={s}, other={o}, predicate={p}")))
    }
})}

#[inline(always)]
fn copy_optional_native<T: DaftNumericType>(v: Option<&T::Native>) -> Option<T::Native> {
    v.copied()
}

fn get_result_size(if_true_len: usize, if_false_len: usize, pred_len: usize) -> DaftResult<usize> {
    let input_lengths = [if_true_len, if_false_len, pred_len];
    let result_len = *input_lengths.iter().max().unwrap();
    let length_error = Err(DaftError::ValueError(format!(
        "Cannot run if_else against arrays with non-broadcastable lengths: if_true={}, if_false={}, predicate={}",
        if_true_len, if_false_len, pred_len
    )));
    for len in input_lengths {
        if len != 1 && len != result_len {
            return length_error;
        }
    }
    Ok(result_len)
}

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn if_else(
        &self,
        other: &DataArray<T>,
        predicate: &BooleanArray,
    ) -> DaftResult<DataArray<T>> {
        broadcast_if_else!(
            arrow2::array::PrimitiveArray::<T::Native>,
            self,
            other,
            predicate,
            copy_optional_native::<T>,
            arrow2::compute::if_then_else::if_then_else,
        )
    }
}

impl Utf8Array {
    pub fn if_else(&self, other: &Utf8Array, predicate: &BooleanArray) -> DaftResult<Utf8Array> {
        broadcast_if_else!(
            arrow2::array::Utf8Array<i64>,
            self,
            other,
            predicate,
            identity,
            arrow2::compute::if_then_else::if_then_else,
        )
    }
}

impl BooleanArray {
    pub fn if_else(
        &self,
        other: &BooleanArray,
        predicate: &BooleanArray,
    ) -> DaftResult<BooleanArray> {
        broadcast_if_else!(
            arrow2::array::BooleanArray,
            self,
            other,
            predicate,
            identity,
            arrow2::compute::if_then_else::if_then_else,
        )
    }
}

impl BinaryArray {
    pub fn if_else(
        &self,
        other: &BinaryArray,
        predicate: &BooleanArray,
    ) -> DaftResult<BinaryArray> {
        broadcast_if_else!(
            arrow2::array::BinaryArray<i64>,
            self,
            other,
            predicate,
            identity,
            arrow2::compute::if_then_else::if_then_else,
        )
    }
}

impl NullArray {
    pub fn if_else(&self, other: &NullArray, predicate: &BooleanArray) -> DaftResult<NullArray> {
        let result_len = get_result_size(self.len(), other.len(), predicate.len())?;
        Ok(DataArray::full_null(
            self.name(),
            self.data_type(),
            result_len,
        ))
    }
}

#[cfg(feature = "python")]
impl PythonArray {
    pub fn if_else(
        &self,
        other: &PythonArray,
        predicate: &BooleanArray,
    ) -> DaftResult<PythonArray> {
        use crate::array::pseudo_arrow::PseudoArrowArray;
        use crate::datatypes::PythonType;
        use pyo3::prelude::*;

        let result_len = get_result_size(self.len(), other.len(), predicate.len())?;

        let predicate_arr = match predicate.len() {
            1 => predicate.broadcast(result_len)?,
            _ => predicate.clone(),
        };

        let if_true_arr = match self.len() {
            1 => self.broadcast(result_len)?,
            _ => self.clone(),
        };

        let if_false_arr = match other.len() {
            1 => other.broadcast(result_len)?,
            _ => other.clone(),
        };

        DataArray::<PythonType>::new(
            self.field.clone(),
            Box::new(PseudoArrowArray::<PyObject>::if_then_else(
                predicate_arr.as_arrow(),
                if_true_arr.as_arrow(),
                if_false_arr.as_arrow(),
            )),
        )
    }
}

fn nested_if_then_else<T: DaftArrowBackedType + 'static>(
    predicate: &BooleanArray,
    if_true: &DataArray<T>,
    if_false: &DataArray<T>,
) -> DaftResult<DataArray<T>>
where
    DataArray<T>: Broadcastable
        + for<'a> TryFrom<(Arc<Field>, Box<dyn arrow2::array::Array>), Error = DaftError>,
{
    // TODO(Clark): Support streaming broadcasting, i.e. broadcasting without inflating scalars to full array length.
    let result = match (predicate.len(), if_true.len(), if_false.len()) {
        (predicate_len, if_true_len, if_false_len)
            if predicate_len == if_true_len && if_true_len == if_false_len =>
        {
            arrow2::compute::if_then_else::if_then_else(
                predicate.as_arrow(),
                if_true.data(),
                if_false.data(),
            )?
        }
        (1, if_true_len, 1) => arrow2::compute::if_then_else::if_then_else(
            predicate.broadcast(if_true_len)?.as_arrow(),
            if_true.data(),
            if_false.broadcast(if_true_len)?.data(),
        )?,
        (1, 1, if_false_len) => arrow2::compute::if_then_else::if_then_else(
            predicate.broadcast(if_false_len)?.as_arrow(),
            if_true.broadcast(if_false_len)?.data(),
            if_false.data(),
        )?,
        (predicate_len, 1, 1) => arrow2::compute::if_then_else::if_then_else(
            predicate.as_arrow(),
            if_true.broadcast(predicate_len)?.data(),
            if_false.broadcast(predicate_len)?.data(),
        )?,
        (predicate_len, if_true_len, 1) if predicate_len == if_true_len => {
            arrow2::compute::if_then_else::if_then_else(
                predicate.as_arrow(),
                if_true.data(),
                if_false.broadcast(predicate_len)?.data(),
            )?
        }
        (predicate_len, 1, if_false_len) if predicate_len == if_false_len => {
            arrow2::compute::if_then_else::if_then_else(
                predicate.as_arrow(),
                if_true.broadcast(predicate_len)?.data(),
                if_false.data(),
            )?
        }
        (p, s, o) => {
            return Err(DaftError::ValueError(format!("Cannot run if_else against arrays with non-broadcastable lengths: if_true={s}, if_false={o}, predicate={p}")));
        }
    };
    DataArray::try_from((if_true.field.clone(), result))
}

impl ListArray {
    pub fn if_else(&self, other: &ListArray, predicate: &BooleanArray) -> DaftResult<ListArray> {
        nested_if_then_else(predicate, self, other)
    }
}

impl FixedSizeListArray {
    pub fn if_else(
        &self,
        other: &FixedSizeListArray,
        predicate: &BooleanArray,
    ) -> DaftResult<FixedSizeListArray> {
        nested_if_then_else(predicate, self, other)
    }
}

impl StructArray {
    pub fn if_else(
        &self,
        other: &StructArray,
        predicate: &BooleanArray,
    ) -> DaftResult<StructArray> {
        nested_if_then_else(predicate, self, other)
    }
}

impl ExtensionArray {
    pub fn if_else(
        &self,
        other: &ExtensionArray,
        predicate: &BooleanArray,
    ) -> DaftResult<ExtensionArray> {
        nested_if_then_else(predicate, self, other)
    }
}

macro_rules! impl_logicalarray_if_else {
    ($ArrayT:ty) => {
        impl $ArrayT {
            pub fn if_else(&self, other: &Self, predicate: &BooleanArray) -> DaftResult<Self> {
                let new_array = self.physical.if_else(&other.physical, predicate)?;
                Ok(Self::new(self.field.clone(), new_array))
            }
        }
    };
}

impl_logicalarray_if_else!(Decimal128Array);
impl_logicalarray_if_else!(DateArray);
impl_logicalarray_if_else!(DurationArray);
impl_logicalarray_if_else!(TimestampArray);
impl_logicalarray_if_else!(EmbeddingArray);
impl_logicalarray_if_else!(ImageArray);
impl_logicalarray_if_else!(FixedShapeImageArray);
impl_logicalarray_if_else!(TensorArray);
impl_logicalarray_if_else!(FixedShapeTensorArray);
