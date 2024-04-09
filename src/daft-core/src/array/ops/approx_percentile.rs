use super::as_arrow::AsArrow;
use super::DaftApproxPercentileAggable;
use crate::array::ops::GroupIndices;
use crate::utils::sketch::Sketch;
use crate::Series;
use crate::{array::DataArray, datatypes::*};
use arrow2;
use arrow2::array::Array;
use common_error::{DaftError, DaftResult};

impl DaftApproxPercentileAggable for &DataArray<Float64Type> {
    type Output = DaftResult<DataArray<Float64Type>>;

    fn approx_percentile(&self, q: &Series) -> Self::Output {
        let percentiles = convert_q_to_vec(q)?;
        let primitive_arr = self.as_arrow();
        let arrow_array = if primitive_arr.null_count() > 0 {
            let sketch = primitive_arr
                .iter()
                .fold(None, |acc, value| match (acc, value) {
                    (acc, None) => acc,
                    (None, Some(v)) => {
                        let mut sketch = Sketch::new();
                        sketch.add(*v);
                        Some(sketch)
                    }
                    (Some(mut acc), Some(v)) => {
                        acc.add(*v);
                        Some(acc)
                    }
                });
            let quantile = match sketch {
                Some(s) => s.quantile(percentiles[0])?,
                None => None,
            };

            Box::new(arrow2::array::PrimitiveArray::<f64>::from([quantile]))
        } else {
            let sketch = primitive_arr
                .values_iter()
                .fold(Sketch::new(), |mut acc, value| {
                    acc.add(*value);
                    acc
                });
            let quantile = sketch.quantile(percentiles[0])?.unwrap();

            Box::new(arrow2::array::PrimitiveArray::<f64>::from_slice([quantile]))
        };

        DataArray::new(self.field.clone(), arrow_array)
    }

    fn grouped_approx_percentile(&self, q: &Series, groups: &GroupIndices) -> Self::Output {
        let percentiles = convert_q_to_vec(q)?;
        let arrow_array = self.as_arrow();
        let sketch_per_group = if arrow_array.null_count() > 0 {
            let quantiles: Vec<Option<f64>> = groups
                .iter()
                .map(|g| {
                    let sketch = g.iter().fold(None, |acc, index| {
                        let idx = *index as usize;
                        match (acc, arrow_array.is_null(idx)) {
                            (acc, true) => acc,
                            (None, false) => {
                                let mut sketch = Sketch::new();
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
                        Some(s) => Ok(s.quantile(percentiles[0])?),
                        None => Ok(None),
                    }
                })
                .collect::<DaftResult<Vec<_>>>()?;

            Box::new(arrow2::array::PrimitiveArray::<f64>::from_trusted_len_iter(
                quantiles.into_iter(),
            ))
        } else {
            let quantiles: Vec<f64> = groups
                .iter()
                .map(|g| {
                    let sketch = g.iter().fold(Sketch::new(), |mut acc, index| {
                        let idx = *index as usize;
                        acc.add(arrow_array.value(idx));
                        acc
                    });

                    let quantile = sketch.quantile(percentiles[0])?;
                    Ok(quantile.unwrap())
                })
                .collect::<DaftResult<Vec<f64>>>()?;

            Box::new(
                arrow2::array::PrimitiveArray::<f64>::from_trusted_len_values_iter(
                    quantiles.into_iter(),
                ),
            )
        };

        DataArray::new(self.field.clone(), sketch_per_group)
    }
}

fn convert_q_to_vec(q: &Series) -> DaftResult<Vec<f64>> {
    match q.data_type() {
        DataType::Float64 => Ok(vec![q
            .f64()?
            .get(0)
            .expect("q parameter of approx_percentile must have one non-null element")]),
        other => Err(DaftError::TypeError(format!(
            "q parameter type must be a float 64, got {}",
            other
        ))),
    }
}
