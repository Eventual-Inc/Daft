use common_error::DaftResult;

use crate::{join::FillNullStrategy, series::Series};

impl Series {
    pub fn is_null(&self) -> DaftResult<Self> {
        self.inner.is_null()
    }

    pub fn not_null(&self) -> DaftResult<Self> {
        self.inner.not_null()
    }

    pub fn fill_null(&self, fill_value: &Self) -> DaftResult<Self> {
        let predicate = self.not_null()?;
        self.if_else(fill_value, &predicate)
    }

    pub fn fill_null_with_strategy(
        &self,
        fill_value: Option<&Self>,
        strategy: FillNullStrategy,
    ) -> DaftResult<Self> {
        match strategy {
            FillNullStrategy::Value => {
                let fill_value = fill_value.ok_or_else(|| {
                    common_error::DaftError::ValueError(
                        "fill_value must be provided when using Value strategy".to_string(),
                    )
                })?;
                self.fill_null(fill_value)
            }
            FillNullStrategy::Forward => self.fill_null_forward(),
            FillNullStrategy::Backward => self.fill_null_backward(),
        }
    }

    fn fill_null_forward(&self) -> DaftResult<Self> {
        // For forward fill, we need to iterate through the series and fill null values
        // with the most recent non-null value

        let array = self.to_arrow();
        let validity = array.validity();

        // If there are no null values, return the series as-is
        if validity.is_none() || validity.unwrap().unset_bits() == 0 {
            return Ok(self.clone());
        }

        // Create a new validity bitmap for the result
        let mut result_validity = arrow2::bitmap::MutableBitmap::with_capacity(array.len());
        result_validity.extend_constant(array.len(), true);

        // Create indices for taking values - we'll build an array of indices
        // where each null position gets the index of the previous non-null value
        let mut indices = Vec::with_capacity(array.len());
        let mut last_valid_idx: Option<usize> = None;

        for i in 0..array.len() {
            if validity.as_ref().unwrap().get_bit(i) {
                // This position has a valid value
                last_valid_idx = Some(i);
                indices.push(i);
            } else {
                // This position is null
                match last_valid_idx {
                    Some(idx) => {
                        // Fill with the last valid index
                        indices.push(idx);
                    }
                    None => {
                        // No previous valid value, keep as null
                        indices.push(i);
                        result_validity.set(i, false);
                    }
                }
            }
        }

        // Create the take indices as a Series
        let indices_array =
            arrow2::array::UInt64Array::from_vec(indices.into_iter().map(|i| i as u64).collect());
        let indices_field =
            daft_schema::field::Field::new("indices", daft_schema::dtype::DataType::UInt64);
        let indices_series = Self::from_arrow(indices_field.into(), Box::new(indices_array))?;

        // Take the values using the indices
        let result = self.take(&indices_series)?;

        // Apply the new validity bitmap
        let result_validity = result_validity.into();
        result.with_validity(Some(result_validity))
    }

    fn fill_null_backward(&self) -> DaftResult<Self> {
        // For backward fill, we need to iterate through the series in reverse
        // and fill null values with the next non-null value

        let array = self.to_arrow();
        let validity = array.validity();

        // If there are no null values, return the series as-is
        if validity.is_none() || validity.unwrap().unset_bits() == 0 {
            return Ok(self.clone());
        }

        // Create a new validity bitmap for the result
        let mut result_validity = arrow2::bitmap::MutableBitmap::with_capacity(array.len());
        result_validity.extend_constant(array.len(), true);

        // Create indices for taking values - we'll build an array of indices
        // where each null position gets the index of the next non-null value
        let mut indices = vec![0; array.len()];
        let mut next_valid_idx: Option<usize> = None;

        // Iterate in reverse to find the next valid value for each position
        for i in (0..array.len()).rev() {
            if validity.as_ref().unwrap().get_bit(i) {
                // This position has a valid value
                next_valid_idx = Some(i);
                indices[i] = i;
            } else {
                // This position is null
                match next_valid_idx {
                    Some(idx) => {
                        // Fill with the next valid index
                        indices[i] = idx;
                    }
                    None => {
                        // No next valid value, keep as null
                        indices[i] = i;
                        result_validity.set(i, false);
                    }
                }
            }
        }

        // Create the take indices as a Series
        let indices_array =
            arrow2::array::UInt64Array::from_vec(indices.into_iter().map(|i| i as u64).collect());
        let indices_field =
            daft_schema::field::Field::new("indices", daft_schema::dtype::DataType::UInt64);
        let indices_series = Self::from_arrow(indices_field.into(), Box::new(indices_array))?;

        // Take the values using the indices
        let result = self.take(&indices_series)?;

        // Apply the new validity bitmap
        let result_validity = result_validity.into();
        result.with_validity(Some(result_validity))
    }
}
