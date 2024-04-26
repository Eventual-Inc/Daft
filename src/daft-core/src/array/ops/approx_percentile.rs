use super::as_arrow::AsArrow;
use super::DaftApproxPercentileAggable;
use crate::array::ops::GroupIndices;
use crate::array::ListArray;
use crate::{array::DataArray, datatypes::*};
use arrow2;
use arrow2::array::Array;
use common_error::{DaftError, DaftResult};
use sketches_ddsketch::{Config, DDSketch};

fn compute_percentiles(sketch: &DDSketch, percentiles: &[f64]) -> DaftResult<Vec<Option<f64>>> {
    percentiles
        .iter()
        .map(|percentile| {
            sketch.quantile(*percentile).map_err(|err| {
                DaftError::ComputeError(format!("Error computing percentiles: {}", err))
            })
        })
        .collect()
}

impl DaftApproxPercentileAggable for &DataArray<Float64Type> {
    type Output = DaftResult<ListArray>;

    fn approx_percentiles(&self, percentiles: &[f64]) -> Self::Output {
        let primitive_arr = self.as_arrow();
        let percentiles = if primitive_arr.null_count() > 0 {
            let sketch = primitive_arr
                .iter()
                .fold(None, |acc, value| match (acc, value) {
                    (acc, None) => acc,
                    (None, Some(v)) => {
                        let mut sketch = DDSketch::new(Config::defaults());
                        sketch.add(*v);
                        Some(sketch)
                    }
                    (Some(mut acc), Some(v)) => {
                        acc.add(*v);
                        Some(acc)
                    }
                });
            match sketch {
                Some(s) => Some(compute_percentiles(&s, percentiles)?),
                None => None,
            }
        } else {
            let sketch = primitive_arr.values_iter().fold(
                DDSketch::new(Config::defaults()),
                |mut acc, value| {
                    acc.add(*value);
                    acc
                },
            );
            Some(compute_percentiles(&sketch, percentiles)?)
        };

        ListArray::try_from((self.field.name.as_str(), [percentiles].as_slice()))
    }

    fn grouped_approx_percentiles(
        &self,
        groups: &GroupIndices,
        percentiles: &[f64],
    ) -> Self::Output {
        let arrow_array = self.as_arrow();
        let percentiles_per_group = if arrow_array.null_count() > 0 {
            groups
                .iter()
                .map(|g| {
                    let sketch = g.iter().fold(None, |acc, index| {
                        let idx = *index as usize;
                        match (acc, arrow_array.is_null(idx)) {
                            (acc, true) => acc,
                            (None, false) => {
                                let mut sketch = DDSketch::new(Config::defaults());
                                sketch.add(arrow_array.value(idx));
                                Some(sketch)
                            }
                            (Some(mut acc), false) => {
                                acc.add(arrow_array.value(idx));
                                Some(acc)
                            }
                        }
                    });

                    match sketch {
                        Some(s) => Ok(Some(compute_percentiles(&s, percentiles)?)),
                        None => Ok(None),
                    }
                })
                .collect::<DaftResult<Vec<Option<Vec<Option<f64>>>>>>()?
        } else {
            groups
                .iter()
                .map(|g| {
                    let sketch =
                        g.iter()
                            .fold(DDSketch::new(Config::defaults()), |mut acc, index| {
                                let idx = *index as usize;
                                acc.add(arrow_array.value(idx));
                                acc
                            });

                    Ok(Some(compute_percentiles(&sketch, percentiles)?))
                })
                .collect::<DaftResult<Vec<Option<Vec<Option<f64>>>>>>()?
        };

        ListArray::try_from((self.field.name.as_str(), percentiles_per_group.as_slice()))
    }
}
