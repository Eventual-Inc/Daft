use common_error::DaftResult;

use crate::{datatypes::DataType, series::Series};

impl Series {
    pub fn cast(&self, datatype: &DataType) -> DaftResult<Self> {
        let casted = self.inner.cast(datatype)?;
        if casted.name() == self.name() {
            Ok(casted)
        } else {
            Ok(casted.rename(self.name()))
        }
    }

    /// Safe type cast: returns null instead of raising an error when conversion fails
    pub fn try_cast(&self, datatype: &DataType) -> DaftResult<Self> {
        // If the target type is the same as the current type, return directly
        if self.data_type() == datatype {
            return Ok(self.clone());
        }

        // Try bulk cast first, return directly if successful
        match self.cast(datatype) {
            Ok(casted) => Ok(casted),
            Err(_) => {
                // Bulk cast failed, try element-wise.
                // NOTE: Known performance limitation - this is O(n) slice+cast+concat which
                // can be very slow at scale. For columns where all rows fail the cast, this
                // creates N single-element Series objects. A future optimization could use a
                // validity bitmap to perform a vectorized cast. See: https://github.com/Eventual-Inc/Daft/issues/6959
                let len = self.len();
                let mut results: Vec<Self> = Vec::with_capacity(len);

                for i in 0..len {
                    let element = self.slice(i, i + 1)?;
                    match element.cast(datatype) {
                        Ok(casted) => results.push(casted),
                        Err(_) => {
                            results.push(Self::full_null(self.name(), datatype, 1));
                        }
                    }
                }

                // Concatenate all single-element Series
                if results.is_empty() {
                    Ok(Self::empty(self.name(), datatype))
                } else {
                    let ref_results: Vec<&Self> = results.iter().collect();
                    Self::concat(&ref_results)
                }
            }
        }
    }
}
