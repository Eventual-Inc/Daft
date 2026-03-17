use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, UInt64Array, Utf8Array},
    series::{IntoSeries, Series},
};
use h3o::CellIndex;

/// Parses a Series (UInt64 or Utf8) into a Vec of Option<CellIndex>.
pub fn series_to_cell_indices(series: &Series) -> DaftResult<Vec<Option<CellIndex>>> {
    match series.data_type() {
        DataType::UInt64 => {
            let arr = series.u64()?;
            Ok(arr
                .into_iter()
                .map(|opt| opt.and_then(|v| CellIndex::try_from(v).ok()))
                .collect())
        }
        DataType::Utf8 => {
            let arr = series.utf8()?;
            Ok(arr
                .into_iter()
                .map(|opt| opt.and_then(|s| s.parse::<CellIndex>().ok()))
                .collect())
        }
        dt => Err(common_error::DaftError::TypeError(format!(
            "expected UInt64 or Utf8 for H3 cell index, got {dt}"
        ))),
    }
}

/// Converts cell indices back to a Series, matching the input dtype.
/// If input was Utf8, returns hex strings. If UInt64, returns u64 values.
pub fn cell_indices_to_series(
    name: &str,
    cells: impl Iterator<Item = Option<CellIndex>>,
    input_dtype: &DataType,
) -> Series {
    match input_dtype {
        DataType::Utf8 => {
            let result: Utf8Array = cells.map(|opt| opt.map(|c| c.to_string())).collect();
            result.rename(name).into_series()
        }
        _ => {
            let result: UInt64Array = cells.map(|opt| opt.map(u64::from)).collect();
            result.rename(name).into_series()
        }
    }
}

/// Validates that a field's dtype is UInt64 or Utf8 (valid H3 cell representations).
pub fn ensure_cell_dtype(func_name: &str, field: &daft_core::prelude::Field) -> DaftResult<()> {
    if field.dtype != DataType::UInt64 && field.dtype != DataType::Utf8 {
        return Err(common_error::DaftError::TypeError(format!(
            "{func_name}: cell must be UInt64 or Utf8, got {}",
            field.dtype
        )));
    }
    Ok(())
}
