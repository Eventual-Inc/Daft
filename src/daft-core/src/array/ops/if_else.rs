use crate::array::growable::{Growable, GrowableArray};
use crate::array::ops::full::FullNull;
use crate::array::{DataArray, FixedSizeListArray, ListArray, StructArray};
use crate::datatypes::{BooleanArray, DaftPhysicalType};
use crate::{DataType, IntoSeries, Series};
use arrow2::array::Array;
use common_error::DaftResult;

use super::as_arrow::AsArrow;

fn generic_if_else<T: GrowableArray + FullNull + Clone + IntoSeries>(
    predicate: &BooleanArray,
    name: &str,
    lhs: &T,
    rhs: &T,
    dtype: &DataType,
    lhs_len: usize,
    rhs_len: usize,
) -> DaftResult<Series> {
    // CASE 1: Broadcast predicate
    if predicate.len() == 1 {
        return match predicate.get(0) {
            None => Ok(T::full_null(name, dtype, lhs_len).into_series()),
            Some(predicate_scalar_value) => {
                if predicate_scalar_value {
                    Ok(lhs.clone().into_series())
                } else {
                    Ok(rhs.clone().into_series())
                }
            }
        };
    }

    // Build the result using a Growable
    let predicate = predicate.as_arrow();
    let mut growable = T::make_growable(
        name,
        dtype,
        vec![lhs, rhs],
        predicate.null_count() > 0, // If predicate has nulls, we will need to append nulls to growable
        predicate.len(),
    );
    // CASE 2: predicate is not broadcastable, and contains nulls
    //
    // NOTE: we iterate through the predicate+validity which is slightly slower than using a SlicesIterator approach.
    // An alternative is to naively apply the raw predicate values (without considering validity), and then applying
    // validity afterwards. However this could use more memory than required, since we copy even values that will
    // later then be considered null.
    if predicate.null_count() > 0 {
        for (i, pred) in predicate.into_iter().enumerate() {
            match pred {
                None => {
                    growable.add_nulls(1);
                }
                Some(true) => {
                    let idx = if lhs_len == 1 { 0 } else { i };
                    growable.extend(0, idx, 1);
                }
                Some(false) => {
                    let idx = if rhs_len == 1 { 0 } else { i };
                    growable.extend(1, idx, 1);
                }
            }
        }
        growable.build()

    // CASE 3: predicate is not broadcastable, and does not contain nulls
    } else {
        // Helper to extend the growable, taking into account broadcast semantics
        let (broadcast_lhs, broadcast_rhs) = (lhs_len == 1, rhs_len == 1);
        let mut extend = |pred: bool, start, len| {
            match (pred, broadcast_lhs, broadcast_rhs) {
                (true, false, _) => {
                    growable.extend(0, start, len);
                }
                (false, _, false) => {
                    growable.extend(1, start, len);
                }
                (true, true, _) => {
                    for _ in 0..len {
                        growable.extend(0, 0, 1);
                    }
                }
                (false, _, true) => {
                    for _ in 0..len {
                        growable.extend(1, 0, 1);
                    }
                }
            };
        };

        // Iterate through the predicate using SlicesIterator, which is a much faster way of iterating through a bitmap
        let mut start_falsy = 0;
        let mut total_len = 0;
        for (start, len) in arrow2::bitmap::utils::SlicesIterator::new(predicate.values()) {
            if start != start_falsy {
                extend(false, start_falsy, start - start_falsy);
                total_len += start - start_falsy;
            };
            extend(true, start, len);
            total_len += len;
            start_falsy = start + len;
        }
        if total_len != predicate.len() {
            extend(false, total_len, predicate.len() - total_len);
        }
        growable.build()
    }
}

impl<T> DataArray<T>
where
    T: DaftPhysicalType,
    DataArray<T>: GrowableArray + IntoSeries,
{
    pub fn if_else(
        &self,
        other: &DataArray<T>,
        predicate: &BooleanArray,
    ) -> DaftResult<DataArray<T>> {
        generic_if_else(
            predicate,
            self.name(),
            self,
            other,
            self.data_type(),
            self.len(),
            other.len(),
        )?
        .downcast::<DataArray<T>>()
        .map(|arr| arr.clone())
    }
}

macro_rules! impl_if_else_nested_array {
    ($arr:ident) => {
        impl<'a> $arr {
            pub fn if_else(
                &'a self,
                other: &'a $arr,
                predicate: &BooleanArray,
            ) -> DaftResult<$arr> {
                generic_if_else(
                    predicate,
                    self.name(),
                    self,
                    other,
                    self.data_type(),
                    self.len(),
                    other.len(),
                )?
                .downcast::<$arr>()
                .map(|arr| arr.clone())
            }
        }
    };
}

impl_if_else_nested_array!(ListArray);
impl_if_else_nested_array!(FixedSizeListArray);
impl_if_else_nested_array!(StructArray);
