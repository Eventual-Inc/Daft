use crate::{
    array::{ListArray, StructArray},
    utils::approx_percentile::compute_percentiles,
};

use common_error::DaftResult;

impl StructArray {
    pub fn sketch_percentile(&self, percentiles: &[f64]) -> DaftResult<ListArray> {
        let sketches_array = daft_sketch::from_arrow2(self.to_arrow())?;

        let percentiles_arr = sketches_array
            .iter()
            .map(|sketch| match sketch {
                None => Ok(None),
                Some(s) => Ok(Some(compute_percentiles(s, percentiles)?)),
            })
            .collect::<DaftResult<Vec<Option<Vec<Option<f64>>>>>>()?;

        ListArray::try_from((self.field.name.as_str(), percentiles_arr.as_slice()))
    }
}
