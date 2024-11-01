use arrow2::array::PrimitiveArray;
use common_error::{DaftError, DaftResult};
use num_traits::{clamp, clamp_max, clamp_min};

use crate::{array::DataArray, datatypes::DaftNumericType, prelude::AsArrow};

/// Helper function to perform clamping on a value with optional bounds using `Option<T>` directly
fn clamp_helper<T: PartialOrd + Copy>(
    value: Option<&T>,
    left_bound: Option<&T>,
    right_bound: Option<&T>,
) -> Option<T> {
    match (value, left_bound, right_bound) {
        (None, _, _) => None,
        (Some(v), Some(l), Some(r)) => {
            assert!(l <= r, "Left bound is greater than right bound");
            Some(clamp(*v, *l, *r))
        }
        (Some(v), Some(l), None) => Some(clamp_min(*v, *l)),
        (Some(v), None, Some(r)) => Some(clamp_max(*v, *r)),
        (Some(v), None, None) => Some(*v),
    }
}

macro_rules! create_data_array {
    ($self:ident, $result:expr) => {{
        let result =
            unsafe { PrimitiveArray::<T::Native>::from_trusted_len_iter_unchecked($result) };
        let data_array = Self::from(($self.name(), Box::new(result)))
            .with_validity($self.validity().cloned())?;
        Ok(data_array)
    }};
}

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
                let result = self
                    .as_arrow()
                    .iter()
                    .zip(left_bound.as_arrow().iter())
                    .zip(right_bound.as_arrow().iter())
                    .map(|((a, b), c)| clamp_helper(a, b, c));
                create_data_array!(self, result)
            }
            // Case where left_bound has the same length as self and right_bound has length 1
            (array_size, lbound_size, 1) if array_size == lbound_size => {
                let right = right_bound.get(0);
                let result = self
                    .as_arrow()
                    .iter()
                    .zip(left_bound.as_arrow().iter())
                    .map(|(a, b)| clamp_helper(a, b, right.as_ref()));
                create_data_array!(self, result)
            }
            // Case where right_bound has the same length as self and left_bound has length 1
            (array_size, 1, rbound_size) if array_size == rbound_size => {
                let left = left_bound.get(0);
                let result = self
                    .as_arrow()
                    .iter()
                    .zip(right_bound.as_arrow().iter())
                    .map(|(a, b)| clamp_helper(a, left.as_ref(), b));
                create_data_array!(self, result)
            }
            // Case where both left_bound and right_bound have length 1
            (_, 1, 1) => {
                let left = left_bound.get(0);
                let right = right_bound.get(0);
                let result = self
                    .as_arrow()
                    .iter()
                    .map(|v| clamp_helper(v, left.as_ref(), right.as_ref()));
                create_data_array!(self, result)
            }
            // Handle incompatible lengths
            _ => Err(DaftError::ValueError(format!(
                "trying to operate on incompatible length arrays: {}: {}, {}: {}, {}: {}",
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
