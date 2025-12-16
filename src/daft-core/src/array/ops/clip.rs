use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use num_traits::{clamp, clamp_max, clamp_min};

use crate::{array::DataArray, datatypes::DaftNumericType};

impl<T> DataArray<T>
where
    T: DaftNumericType,
    T::Native: PartialOrd,
{
    /// Clips the values in the array to the provided left and right bounds.
    ///
    /// # Arguments
    ///
    /// * `left_bound` - The lower bound for clipping.
    /// * `right_bound` - The upper bound for clipping.
    ///
    /// # Returns
    ///
    /// * `DaftResult<Self>` - The clipped DataArray.
    pub fn clip(&self, left_bound: &Self, right_bound: &Self) -> DaftResult<Self> {
        match (self.len(), left_bound.len(), right_bound.len()) {
            // Case where all arrays have the same length
            (array_size, lbound_size, rbound_size)
                if array_size == lbound_size && array_size == rbound_size =>
            {
                let values = self.values();
                let result = values
                    .iter()
                    .zip(left_bound.into_iter())
                    .zip(right_bound.into_iter())
                    .map(|((value, left), right)| match (left, right) {
                        (Some(l), Some(r)) => Some(clamp(*value, *l, *r)),
                        (Some(l), None) => Some(clamp_min(*value, *l)),
                        (None, Some(r)) => Some(clamp_max(*value, *r)),
                        (None, None) => Some(*value),
                    });
                let data_array = Self::from_iter(Arc::new(self.field().clone()), result)
                    .with_validity(self.validity().cloned())?;
                Ok(data_array)
            }
            // Case where left_bound has the same length as self and right_bound has length 1
            (array_size, lbound_size, 1) if array_size == lbound_size => {
                let right = right_bound.get(0);
                // We check the validity of right_bound here, since it has length 1.
                // This avoids a validity check in the clamp function
                match right {
                    Some(r) => {
                        let values = self.values();
                        // Right is valid, so we just clamp/clamp_max the values depending on the left bound
                        let result =
                            values
                                .iter()
                                .zip(left_bound.into_iter())
                                .map(move |(value, left)| match left {
                                    Some(l) => Some(clamp(*value, *l, r)),
                                    None => Some(clamp_max(*value, r)), // If left is null, we can just clamp_max
                                });
                        let data_array = Self::from_iter(Arc::new(self.field().clone()), result)
                            .with_validity(self.validity().cloned())?;
                        Ok(data_array)
                    }
                    None => {
                        let values = self.values();
                        // In this case, right_bound is null, so we can just do a simple clamp_min
                        let result = values.iter().zip(left_bound.into_iter()).map(
                            |(value, left)| match left {
                                Some(l) => Some(clamp_min(*value, *l)),
                                None => Some(*value), // Left null, and right null, so we just don't do anything
                            },
                        );
                        let data_array = Self::from_iter(Arc::new(self.field().clone()), result)
                            .with_validity(self.validity().cloned())?;
                        Ok(data_array)
                    }
                }
            }
            // Case where right_bound has the same length as self and left_bound has length 1
            (array_size, 1, rbound_size) if array_size == rbound_size => {
                let left = left_bound.get(0);
                match left {
                    Some(l) => {
                        let values = self.values();
                        let result = values.iter().zip(right_bound.into_iter()).map(
                            move |(value, right)| match right {
                                Some(r) => Some(clamp(*value, l, *r)),
                                None => Some(clamp_min(*value, l)), // Right null, so we can just clamp_min
                            },
                        );
                        let data_array = Self::from_iter(Arc::new(self.field().clone()), result)
                            .with_validity(self.validity().cloned())?;
                        Ok(data_array)
                    }
                    None => {
                        let values = self.values();
                        let result =
                            values
                                .iter()
                                .zip(right_bound.into_iter())
                                .map(|(value, right)| match right {
                                    Some(r) => Some(clamp_max(*value, *r)),
                                    None => Some(*value),
                                });
                        let data_array = Self::from_iter(Arc::new(self.field().clone()), result)
                            .with_validity(self.validity().cloned())?;
                        Ok(data_array)
                    }
                }
            }
            // Case where both left_bound and right_bound have length 1
            (_, 1, 1) => {
                let left = left_bound.get(0);
                let right = right_bound.get(0);
                match (left, right) {
                    (Some(l), Some(r)) => self.apply(|value| clamp(value, l, r)),
                    (Some(l), None) => self.apply(|value| clamp_min(value, l)),
                    (None, Some(r)) => self.apply(|value| clamp_max(value, r)),
                    (None, None) => {
                        // Not doing anything here, so we can just return self
                        Ok(self.clone())
                    }
                }
            }
            // Handle incompatible lengths
            _ => Err(DaftError::ValueError(format!(
                "Unable to clip incompatible length arrays: {}: {}, {}: {}, {}: {}",
                self.name(),
                self.len(),
                left_bound.name(),
                left_bound.len(),
                right_bound.name(),
                right_bound.len()
            ))),
        }
    }
}
