use crate::{datatypes::DataType, series::Series};
use common_error::{DaftError, DaftResult};

use crate::series::array_impl::IntoSeries;

impl Series {
    pub fn url_download(
        &self,
        max_connections: usize,
        raise_error_on_failure: bool,
        multi_thread: bool,
    ) -> DaftResult<Series> {
        match self.data_type() {
            // DataType::Utf8 => Ok(self
            //     .utf8()?
            //     .url_download(max_connections, raise_error_on_failure, multi_thread)?
            //     .into_series()),
            dt => Err(DaftError::TypeError(format!(
                "url download not implemented for type {dt}"
            ))),
        }
    }
}
