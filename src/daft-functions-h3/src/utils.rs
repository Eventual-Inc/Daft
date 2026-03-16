use common_error::DaftResult;
use daft_core::{prelude::DataType, series::Series};
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
