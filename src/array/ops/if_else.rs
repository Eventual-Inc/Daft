use crate::datatypes::{BinaryArray, DaftNumericType, NullArray};
use crate::datatypes::{BooleanArray, Utf8Array};
use crate::error::DaftError;
use crate::{array::DataArray, error::DaftResult};
use arrow2::compute::if_then_else::if_then_else;
use std::sync::Arc;

use crate::array::BaseArray;

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn if_else(
        &self,
        other: &DataArray<T>,
        predicate: &BooleanArray,
    ) -> DaftResult<DataArray<T>> {
        match (self.len(), other.len(), predicate.len()) {
            // CASE: Equal lengths across all 3 arguments
            (self_len, other_len, predicate_len) if self_len == other_len && other_len == predicate_len => {
                let result = if_then_else(predicate.downcast(), self.data(), other.data())?;
                DataArray::try_from((self.name(), result))
            },
            // CASE: Broadcast predicate
            (self_len, _, 1) => {
                let predicate_scalar = predicate.get(0);
                match predicate_scalar {
                    None => Ok(DataArray::full_null(self.name(), self_len)),
                    Some(predicate_scalar_value) => {
                        if predicate_scalar_value {
                            Ok(self.clone())
                        } else {
                            Ok(other.clone())
                        }
                    }
                }
            }
            // CASE: Broadcast truthy array
            (1, o, p)  if o == p => {
                let self_scalar = self.get(0);
                let predicate_arr = predicate.downcast();
                let predicate_values = predicate_arr.values();
                let naive_if_else: arrow2::array::PrimitiveArray<T::Native> = other.downcast().iter().zip(predicate_values.iter()).map(
                    |(other_val, pred_val)| match pred_val {
                        true => self_scalar,
                        false => other_val.copied(),
                    }
                ).collect();
                DataArray::new(self.field.clone(), Arc::new(naive_if_else.with_validity(predicate_arr.validity().cloned())))
            }
            // CASE: Broadcast falsey array
            (s, 1, p)  if s == p => {
                let other_scalar = other.get(0);
                let predicate_arr = predicate.downcast();
                let predicate_values = predicate_arr.values();
                let naive_if_else: arrow2::array::PrimitiveArray<T::Native> = self.downcast().iter().zip(predicate_values.iter()).map(
                    |(self_val, pred_val)| match pred_val {
                        true => self_val.copied(),
                        false => other_scalar,
                    }
                ).collect();
                DataArray::new(self.field.clone(), Arc::new(naive_if_else.with_validity(predicate_arr.validity().cloned())))
            }
            (s, o, p) => Err(DaftError::ValueError(format!("Cannot run if_else against arrays with mismatched lengths: self={s}, other={o}, predicate={p}")))
        }
    }
}

impl Utf8Array {
    pub fn if_else(&self, other: &Utf8Array, predicate: &BooleanArray) -> DaftResult<Utf8Array> {
        match (self.len(), other.len(), predicate.len()) {
            // CASE: Equal lengths across all 3 arguments
            (self_len, other_len, predicate_len) if self_len == other_len && other_len == predicate_len => {
                let result = if_then_else(predicate.downcast(), self.data(), other.data())?;
                DataArray::try_from((self.name(), result))
            },
            // CASE: Broadcast predicate
            (self_len, _, 1) => {
                let predicate_scalar = predicate.get(0);
                match predicate_scalar {
                    None => Ok(DataArray::full_null(self.name(), self_len)),
                    Some(predicate_scalar_value) => {
                        if predicate_scalar_value {
                            Ok(self.clone())
                        } else {
                            Ok(other.clone())
                        }
                    }
                }
            }
            // CASE: Broadcast truthy array
            (1, o, p)  if o == p => {
                let self_scalar = self.get(0);
                let predicate_arr = predicate.downcast();
                let predicate_values = predicate_arr.values();
                let naive_if_else: arrow2::array::Utf8Array<i64> = other.downcast().iter().zip(predicate_values.iter()).map(
                    |(other_val, pred_val)| match pred_val {
                        true => self_scalar,
                        false => other_val,
                    }
                ).collect();
                DataArray::new(self.field.clone(), Arc::new(naive_if_else.with_validity(predicate_arr.validity().cloned())))
            }
            // CASE: Broadcast falsey array
            (s, 1, p)  if s == p => {
                let other_scalar = other.get(0);
                let predicate_arr = predicate.downcast();
                let predicate_values = predicate_arr.values();
                let naive_if_else: arrow2::array::Utf8Array<i64> = self.downcast().iter().zip(predicate_values.iter()).map(
                    |(self_val, pred_val)| match pred_val {
                        true => self_val,
                        false => other_scalar,
                    }
                ).collect();
                DataArray::new(self.field.clone(), Arc::new(naive_if_else.with_validity(predicate_arr.validity().cloned())))
            }
            (s, o, p) => Err(DaftError::ValueError(format!("Cannot run if_else against arrays with mismatched lengths: self={s}, other={o}, predicate={p}")))
        }
    }
}

impl BooleanArray {
    pub fn if_else(
        &self,
        other: &BooleanArray,
        predicate: &BooleanArray,
    ) -> DaftResult<BooleanArray> {
        match (self.len(), other.len(), predicate.len()) {
            // CASE: Equal lengths across all 3 arguments
            (self_len, other_len, predicate_len) if self_len == other_len && other_len == predicate_len => {
                let result = if_then_else(predicate.downcast(), self.data(), other.data())?;
                DataArray::try_from((self.name(), result))
            },
            // CASE: Broadcast predicate
            (self_len, _, 1) => {
                let predicate_scalar = predicate.get(0);
                match predicate_scalar {
                    None => Ok(DataArray::full_null(self.name(), self_len)),
                    Some(predicate_scalar_value) => {
                        if predicate_scalar_value {
                            Ok(self.clone())
                        } else {
                            Ok(other.clone())
                        }
                    }
                }
            }
            // CASE: Broadcast truthy array
            (1, o, p)  if o == p => {
                let self_scalar = self.get(0);
                let predicate_arr = predicate.downcast();
                let predicate_values = predicate_arr.values();
                let naive_if_else: arrow2::array::BooleanArray = other.downcast().iter().zip(predicate_values.iter()).map(
                    |(other_val, pred_val)| match pred_val {
                        true => self_scalar,
                        false => other_val,
                    }
                ).collect();
                DataArray::new(self.field.clone(), Arc::new(naive_if_else.with_validity(predicate_arr.validity().cloned())))
            }
            // CASE: Broadcast falsey array
            (s, 1, p)  if s == p => {
                let other_scalar = other.get(0);
                let predicate_arr = predicate.downcast();
                let predicate_values = predicate_arr.values();
                let naive_if_else: arrow2::array::BooleanArray = self.downcast().iter().zip(predicate_values.iter()).map(
                    |(self_val, pred_val)| match pred_val {
                        true => self_val,
                        false => other_scalar,
                    }
                ).collect();
                DataArray::new(self.field.clone(), Arc::new(naive_if_else.with_validity(predicate_arr.validity().cloned())))
            }
            (s, o, p) => Err(DaftError::ValueError(format!("Cannot run if_else against arrays with mismatched lengths: self={s}, other={o}, predicate={p}")))
        }
    }
}

impl BinaryArray {
    pub fn if_else(
        &self,
        other: &BinaryArray,
        predicate: &BooleanArray,
    ) -> DaftResult<BinaryArray> {
        match (self.len(), other.len(), predicate.len()) {
            // CASE: Equal lengths across all 3 arguments
            (self_len, other_len, predicate_len) if self_len == other_len && other_len == predicate_len => {
                let result = if_then_else(predicate.downcast(), self.data(), other.data())?;
                DataArray::try_from((self.name(), result))
            },
            // CASE: Broadcast predicate
            (self_len, _, 1) => {
                let predicate_scalar = predicate.get(0);
                match predicate_scalar {
                    None => Ok(DataArray::full_null(self.name(), self_len)),
                    Some(predicate_scalar_value) => {
                        if predicate_scalar_value {
                            Ok(self.clone())
                        } else {
                            Ok(other.clone())
                        }
                    }
                }
            }
            // CASE: Broadcast truthy array
            (1, o, p)  if o == p => {
                let self_scalar = self.get(0);
                let predicate_arr = predicate.downcast();
                let predicate_values = predicate_arr.values();
                let naive_if_else: arrow2::array::BinaryArray<i64> = other.downcast().iter().zip(predicate_values.iter()).map(
                    |(other_val, pred_val)| match pred_val {
                        true => self_scalar,
                        false => other_val,
                    }
                ).collect();
                DataArray::new(self.field.clone(), Arc::new(naive_if_else.with_validity(predicate_arr.validity().cloned())))
            }
            // CASE: Broadcast falsey array
            (s, 1, p)  if s == p => {
                let other_scalar = other.get(0);
                let predicate_arr = predicate.downcast();
                let predicate_values = predicate_arr.values();
                let naive_if_else: arrow2::array::BinaryArray<i64> = self.downcast().iter().zip(predicate_values.iter()).map(
                    |(self_val, pred_val)| match pred_val {
                        true => self_val,
                        false => other_scalar,
                    }
                ).collect();
                DataArray::new(self.field.clone(), Arc::new(naive_if_else.with_validity(predicate_arr.validity().cloned())))
            }
            (s, o, p) => Err(DaftError::ValueError(format!("Cannot run if_else against arrays with mismatched lengths: self={s}, other={o}, predicate={p}")))
        }
    }
}

impl NullArray {
    pub fn if_else(&self, _other: &NullArray, _predicate: &BooleanArray) -> DaftResult<NullArray> {
        Ok(DataArray::full_null(self.name(), self.len()))
    }
}
