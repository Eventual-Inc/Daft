use crate::{
    datatypes::DataType,
    error::{DaftError, DaftResult},
    series::Series,
};

use crate::series::array_impl::IntoSeries;

impl Series {
    pub fn url_download(
        &self,
        max_connections: usize,
        raise_error_on_failure: bool,
    ) -> DaftResult<Series> {
        match self.data_type() {
            DataType::Utf8 => Ok(self
                .utf8()?
                .url_download(max_connections, raise_error_on_failure)?
                .into_series()),
            dt => Err(DaftError::TypeError(format!(
                "url download not implemented for type {dt}"
            ))),
        }
    }
}
