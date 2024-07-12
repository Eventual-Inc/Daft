use common_error::{DaftError, DaftResult};
use daft_core::{DataType, IntoSeries, Series};

use crate::array::{tokenize_decode_array, tokenize_encode_array};

pub fn tokenize_encode(series: &Series, tokens_path: &str) -> DaftResult<Series> {
    series.with_utf8_array(|arr| Ok(tokenize_encode_array(arr, tokens_path)?.into_series()))
}

pub fn tokenize_decode(series: &Series, tokens_path: &str) -> DaftResult<Series> {
    match series.data_type() {
        DataType::List(_) => Ok(tokenize_decode_array(series.list()?, tokens_path)?.into_series()),
        dt => Err(DaftError::TypeError(format!(
            "Tokenize decode not implemented for type {}",
            dt
        ))),
    }
}
