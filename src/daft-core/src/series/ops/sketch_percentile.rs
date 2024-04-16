use crate::series::Series;
use crate::utils::approx_percentile::convert_q_to_vec;
use crate::IntoSeries;
use common_error::DaftError;
use common_error::DaftResult;

impl Series {
    pub fn sketch_percentile(&self, q: &Series) -> DaftResult<Series> {
        use crate::datatypes::DataType::*;

        match self.data_type() {
            Struct(_) => Ok(self
                .struct_()?
                .sketch_percentile(&convert_q_to_vec(q)?)?
                .into_series()),
            other => Err(DaftError::TypeError(format!(
                "sketch_percentile is not implemented for type {}",
                other
            ))),
        }
    }
}
