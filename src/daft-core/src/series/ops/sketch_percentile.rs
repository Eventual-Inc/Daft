use crate::series::Series;
use common_error::DaftError;
use common_error::DaftResult;

impl Series {
    pub fn sketch_percentile(&self, q: &Series) -> DaftResult<Series> {
        use crate::datatypes::DataType::*;
        use crate::series::IntoSeries;

        match self.data_type() {
            Binary => {
                let casted = self.cast(&Binary)?;
                Ok(casted
                    .binary()
                    .unwrap()
                    .sketch_percentile(q.f64()?)?
                    .into_series())
            }
            other => Err(DaftError::TypeError(format!(
                "sketch_percentile is not implemented for type {}",
                other
            ))),
        }
    }
}
