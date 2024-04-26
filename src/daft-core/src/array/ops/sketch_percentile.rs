use crate::array::{ListArray, StructArray};

use common_error::{DaftError, DaftResult};

impl StructArray {
    pub fn sketch_percentile(&self, percentiles: &[f64]) -> DaftResult<ListArray> {
        let sketches_array = daft_sketch::from_arrow2(self.to_arrow())?;

        let percentiles_arr = sketches_array
            .iter()
            .map(|sketch| match sketch {
                None => Ok(None),
                Some(s) => Ok(Some(
                    percentiles
                        .iter()
                        .map(|percentile| {
                            s.quantile(*percentile).map_err(|err| {
                                DaftError::ComputeError(format!(
                                    "Error with calculating percentile {}: {}",
                                    percentile, err
                                ))
                            })
                        })
                        .collect::<DaftResult<Vec<Option<f64>>>>()?,
                )),
            })
            .collect::<DaftResult<Vec<Option<Vec<Option<f64>>>>>>()?;

        ListArray::try_from((self.field.name.as_str(), percentiles_arr.as_slice()))
    }
}
