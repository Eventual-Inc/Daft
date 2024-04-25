use common_error::DaftResult;
use sketches_ddsketch::DDSketch;

use crate::Series;

pub fn convert_q_to_vec(percentiles: &Series) -> DaftResult<Vec<f64>> {
    // TODO: Fix. Incoming series is going to be a Series of f64. We can drastically simplify code
    percentiles
        .f64()
        .map(|arr| arr.into_iter().map(|x| *x.unwrap()).collect::<Vec<f64>>())
}

pub fn compute_percentiles(sketch: &DDSketch, percentiles: &[f64]) -> DaftResult<Vec<Option<f64>>> {
    percentiles
        .iter()
        .map(|q| Ok(sketch.quantile(*q)?))
        .collect::<DaftResult<Vec<Option<f64>>>>()
}
