use crate::datatypes::DataType;
use crate::series::Series;
use crate::IntoSeries;
use common_error::DaftError;
use common_error::DaftResult;

impl Series {
    pub fn json_query(&self, query: &str) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Utf8 => Ok(self.utf8()?.json_query(query)?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "json query not implemented for {}",
                dt
            ))),
        }
    }
}
