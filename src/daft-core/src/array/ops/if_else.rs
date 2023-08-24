use crate::array::growable::{Growable, GrowableArray};
use crate::array::ops::full::FullNull;
use crate::array::DataArray;
use crate::datatypes::logical::LogicalArrayImpl;
use crate::datatypes::{BooleanArray, DaftLogicalType, DaftPhysicalType};
use crate::DataType;
use arrow2::array::Array;
use common_error::DaftResult;
use std::convert::identity;

use super::as_arrow::AsArrow;

fn generic_if_else<'a, T: GrowableArray<'a> + FullNull + Clone>(
    predicate: &BooleanArray,
    name: &str,
    lhs: &'a T,
    rhs: &'a T,
    dtype: &DataType,
    lhs_len: usize,
    rhs_len: usize,
) -> DaftResult<T> {
    // Broadcast predicate
    if predicate.len() == 1 {
        return match predicate.get(0) {
            None => Ok(T::full_null(name, dtype, lhs_len)),
            Some(predicate_scalar_value) => {
                if predicate_scalar_value {
                    Ok(lhs.clone())
                } else {
                    Ok(rhs.clone())
                }
            }
        };
    }

    // If either lhs or rhs has len == 1, we perform broadcasting by always selecting the 0th element
    let broadcasted_getter = |_i: usize| 0usize;
    let get_lhs = if lhs_len == 1 {
        broadcasted_getter
    } else {
        identity
    };
    let get_rhs = if rhs_len == 1 {
        broadcasted_getter
    } else {
        identity
    };

    // Build the result using a Growable
    let predicate = predicate.as_arrow();
    if predicate.null_count() > 0 {
        let mut growable = T::make_growable(
            name.to_string(),
            dtype,
            vec![lhs, rhs],
            predicate.len(),
            true,
        );
        for (i, pred) in predicate.into_iter().enumerate() {
            match pred {
                None => {
                    growable.add_nulls(1);
                }
                Some(true) => {
                    growable.extend(0, get_lhs(i), 1);
                }
                Some(false) => {
                    growable.extend(1, get_rhs(i), 1);
                }
            }
        }
        growable.build()
    } else {
        let mut growable = T::make_growable(
            name.to_string(),
            dtype,
            vec![lhs, rhs],
            predicate.len(),
            false,
        );
        let mut start_falsy = 0;
        let mut total_len = 0;

        let mut extend = |arr_idx: usize, start: usize, len: usize| {
            if arr_idx == 0 {
                if lhs_len == 1 {
                    for _ in 0..len {
                        growable.extend(0, 0, 1);
                    }
                } else {
                    growable.extend(0, start, len);
                }
            } else if rhs_len == 1 {
                for _ in 0..len {
                    growable.extend(1, 0, 1);
                }
            } else {
                growable.extend(1, start, len);
            }
        };

        for (start, len) in arrow2::bitmap::utils::SlicesIterator::new(predicate.values()) {
            if start != start_falsy {
                extend(1, start_falsy, start - start_falsy);
                total_len += start - start_falsy;
            };
            extend(0, start, len);
            total_len += len;
            start_falsy = start + len;
        }
        if total_len != predicate.len() {
            extend(1, total_len, predicate.len() - total_len);
        }
        growable.build()
    }
}

impl<'a, T> DataArray<T>
where
    T: DaftPhysicalType + 'static,
    DataArray<T>: GrowableArray<'a>,
{
    pub fn if_else(
        &'a self,
        other: &'a DataArray<T>,
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
        )
    }
}

impl<'a, L> LogicalArrayImpl<L, DataArray<L::PhysicalType>>
where
    L: DaftLogicalType,
    LogicalArrayImpl<L, DataArray<L::PhysicalType>>: GrowableArray<'a>,
    LogicalArrayImpl<L, DataArray<L::PhysicalType>>: FullNull,
{
    pub fn if_else(
        &'a self,
        other: &'a LogicalArrayImpl<L, DataArray<L::PhysicalType>>,
        predicate: &BooleanArray,
    ) -> DaftResult<LogicalArrayImpl<L, DataArray<L::PhysicalType>>> {
        generic_if_else(
            predicate,
            self.name(),
            self,
            other,
            self.data_type(),
            self.len(),
            other.len(),
        )
    }
}
