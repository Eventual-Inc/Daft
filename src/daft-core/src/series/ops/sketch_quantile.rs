use crate::series::Series;
use common_error::DaftError;
use common_error::DaftResult;

impl Series {
    pub fn sketch_quantile(&self, q: &Series) -> DaftResult<Series> {
        use crate::datatypes::DataType::*;
        use crate::series::IntoSeries;

        match self.data_type() {
            Binary => {
                let casted = self.cast(&Binary)?;
                Ok(casted
                    .binary()
                    .unwrap()
                    .sketch_quantile(q.f64()?)?
                    .into_series())
            }
            other => Err(DaftError::TypeError(format!(
                "sketch_quantile is not implemented for type {}",
                other
            ))),
        }
    }
}
