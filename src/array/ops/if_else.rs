use crate::datatypes::DaftNumericType;
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
            (self_len, other_len, predicate_len) if self_len == other_len && other_len == predicate_len => {
                let result = if_then_else(predicate.downcast(), self.data(), other.data())?;
                DataArray::try_from((self.name(), result))
            },
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
            (1, o, p)  if o == p => {
                let self_scalar = self.get(0);
                let new_arr: arrow2::array::PrimitiveArray<T::Native> = other.downcast().iter().zip(predicate.downcast().iter()).map(
                    |(other_val, pred_val)| match pred_val {
                        None => None,
                        Some(true) => self_scalar,
                        Some(false) => other_val.copied()
                    }
                ).collect();
                DataArray::new(self.field.clone(), Arc::new(new_arr))
            }
            (s, 1, p)  if s == p => {
                let other_scalar = other.get(0);
                let new_arr: arrow2::array::PrimitiveArray<T::Native> = self.downcast().iter().zip(predicate.downcast().iter()).map(
                    |(self_val, pred_val)| match pred_val {
                        None => None,
                        Some(true) => self_val.copied(),
                        Some(false) => other_scalar
                    }
                ).collect();
                DataArray::new(self.field.clone(), Arc::new(new_arr))
            }
            (s, o, p) => Err(DaftError::ValueError(format!("Cannot run if_else against arrays with mismatched lengths: self={s}, other={o}, predicate={p}")))
        }
    }
}

impl Utf8Array {
    pub fn if_else(&self, other: &Utf8Array, predicate: &BooleanArray) -> DaftResult<Utf8Array> {
        match (self.len(), other.len(), predicate.len()) {
            (self_len, other_len, predicate_len) if self_len == other_len && other_len == predicate_len => {
                let result = if_then_else(predicate.downcast(), self.data(), other.data())?;
                DataArray::try_from((self.name(), result))
            },
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
            (1, o, p)  if o == p => {
                let self_scalar = self.get(0);
                let new_arr: arrow2::array::Utf8Array<i64> = other.downcast().iter().zip(predicate.downcast().iter()).map(
                    |(other_val, pred_val)| match pred_val {
                        None => None,
                        Some(true) => self_scalar,
                        Some(false) => other_val,
                    }
                ).collect();
                DataArray::new(self.field.clone(), Arc::new(new_arr))
            }
            (s, 1, p)  if s == p => {
                let other_scalar = other.get(0);
                let new_arr: arrow2::array::Utf8Array<i64> = self.downcast().iter().zip(predicate.downcast().iter()).map(
                    |(self_val, pred_val)| match pred_val {
                        None => None,
                        Some(true) => self_val,
                        Some(false) => other_scalar,
                    }
                ).collect();
                DataArray::new(self.field.clone(), Arc::new(new_arr))
            }
            (s, o, p) => Err(DaftError::ValueError(format!("Cannot run if_else against arrays with mismatched lengths: self={s}, other={o}, predicate={p}")))
        }
    }
}
