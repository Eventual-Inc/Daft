use common_error::{DaftError, DaftResult};
use sketches_ddsketch::DDSketch;

use crate::array::ops::as_arrow::AsArrow;
use crate::{DataType, Series};

#[cfg(feature = "python")]
use crate::datatypes::PythonArray;
#[cfg(feature = "python")]
use pyo3::Python;

pub fn convert_q_to_vec(q: &Series) -> DaftResult<Vec<f64>> {
    match q.data_type() {
        DataType::Float64 => Ok(vec![q
            .f64()?
            .get(0)
            .expect("q parameter of approx_percentile must have one non-null element")]),
        DataType::List(_) => {
            let values = q
                .list()?
                .get(0)
                .expect("q parameter of approx_percentile must have one non-null element");
            match values.data_type() {
                DataType::Float64 => Ok(values.f64()?.into_iter().flatten().cloned().collect()),
                other => Err(DaftError::TypeError(format!(
                    "q parameter type must be a float 64 or an array of float 64, got {}",
                    other
                ))),
            }
        }
        #[cfg(feature = "python")]
        DataType::Python => {
            let downcasted = q.downcast::<PythonArray>()?;
            let py_objects = downcasted.as_arrow().to_pyobj_vec();
            let py_values = py_objects
                .get(0)
                .expect("q parameter of approx_percentile must have one non-null element");
            Python::with_gil(|py| {
                Ok(py_values
                    .extract::<Vec<f64>>(py)
                    .expect("q parameter of approx_percentile must contain float 64 values"))
            })
        }
        other => Err(DaftError::TypeError(format!(
            "q parameter type must be a float 64 or an array of float 64, got {}",
            other
        ))),
    }
}

pub fn compute_percentiles(sketch: &DDSketch, percentiles: &[f64]) -> DaftResult<Vec<Option<f64>>> {
    percentiles
        .iter()
        .map(|q| Ok(sketch.quantile(*q)?))
        .collect::<DaftResult<Vec<Option<f64>>>>()
}
