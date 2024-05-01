use crate::series::Series;
use common_error::DaftError;
use common_error::DaftResult;

impl Series {
    pub fn sketch_percentile(
        &self,
        percentiles: &[f64],
        force_list_output: bool,
    ) -> DaftResult<Series> {
        use crate::datatypes::DataType::*;

        match self.data_type() {
            Struct(_) => Ok(self
                .struct_()?
                .sketch_percentile(percentiles, force_list_output)?),
            other => Err(DaftError::TypeError(format!(
                "sketch_percentile is not implemented for type {}",
                other
            ))),
        }
    }
}
