use crate::array::growable::{Growable, GrowableArray};
use crate::array::ops::full::FullNull;
use crate::array::DataArray;
use crate::datatypes::logical::LogicalArrayImpl;
use crate::datatypes::{BooleanArray, DaftLogicalType, DaftPhysicalType};
use common_error::DaftResult;
use std::convert::identity;

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
        // Broadcast predicate case
        if predicate.len() == 1 {
            return match predicate.get(0) {
                None => Ok(DataArray::full_null(
                    self.name(),
                    self.data_type(),
                    self.len(),
                )),
                Some(predicate_scalar_value) => {
                    if predicate_scalar_value {
                        Ok(self.clone())
                    } else {
                        Ok(other.clone())
                    }
                }
            };
        }

        // If either lhs or rhs has len == 1, we perform broadcasting by always selecting the 0th element
        let broadcasted_getter = |_i: usize| 0usize;
        let get_lhs = if self.len() == 1 {
            broadcasted_getter
        } else {
            identity
        };
        let get_rhs = if other.len() == 1 {
            broadcasted_getter
        } else {
            identity
        };

        // Build the result using a Growable
        let mut growable = DataArray::<T>::make_growable(
            self.name().to_string(),
            self.data_type(),
            vec![self, other],
            predicate.len(),
        );
        for (i, pred) in predicate.into_iter().enumerate() {
            match pred {
                None => {
                    growable.add_nulls(1);
                }
                Some(pred) if pred => {
                    growable.extend(0, get_lhs(i), 1);
                }
                Some(_) => {
                    growable.extend(1, get_rhs(i), 1);
                }
            }
        }

        growable.build()
    }
}

impl<'a, L> LogicalArrayImpl<L, DataArray<L::PhysicalType>>
where
    L: DaftLogicalType,
    LogicalArrayImpl<L, DataArray<L::PhysicalType>>: GrowableArray<'a>,
{
    pub fn if_else(
        &'a self,
        other: &'a LogicalArrayImpl<L, DataArray<L::PhysicalType>>,
        predicate: &BooleanArray,
    ) -> DaftResult<LogicalArrayImpl<L, DataArray<L::PhysicalType>>> {
        // Broadcast predicate case
        if predicate.len() == 1 {
            return match predicate.get(0) {
                None => Ok(LogicalArrayImpl::<L, DataArray<L::PhysicalType>>::new(
                    self.field.clone(),
                    DataArray::<L::PhysicalType>::full_null(
                        self.name(),
                        self.physical.data_type(),
                        self.len(),
                    ),
                )),
                Some(predicate_scalar_value) => {
                    if predicate_scalar_value {
                        Ok(self.clone())
                    } else {
                        Ok(other.clone())
                    }
                }
            };
        }

        // If either lhs or rhs has len == 1, we perform broadcasting by always selecting the 0th element
        let broadcasted_getter = |_i: usize| 0usize;
        let get_lhs = if self.len() == 1 {
            broadcasted_getter
        } else {
            identity
        };
        let get_rhs = if other.len() == 1 {
            broadcasted_getter
        } else {
            identity
        };

        // Build the result using a Growable
        let mut growable = LogicalArrayImpl::<L, DataArray<L::PhysicalType>>::make_growable(
            self.name().to_string(),
            self.data_type(),
            vec![self, other],
            predicate.len(),
        );
        for (i, pred) in predicate.into_iter().enumerate() {
            match pred {
                None => {
                    growable.add_nulls(1);
                }
                Some(pred) if pred => {
                    growable.extend(0, get_lhs(i), 1);
                }
                Some(_) => {
                    growable.extend(1, get_rhs(i), 1);
                }
            }
        }

        growable.build()
    }
}
