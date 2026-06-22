//! Parquet round-trip codec for per-checkpoint key files.
//!
//! One file per `stage_keys()` call, one batch per file, one column
//! (the keys column).

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use bytes::Bytes;
use daft_core::series::Series;
use daft_schema::field::Field;
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};

use crate::error::{CheckpointError, CheckpointResult};

/// Serialize a single-column [`Series`] of keys as Parquet bytes.
pub(super) fn write_series_as_parquet(series: &Series) -> CheckpointResult<Vec<u8>> {
    let arrow_array = series.to_arrow().map_err(|e| CheckpointError::Internal {
        message: format!("failed to convert series to arrow: {e}"),
    })?;
    let arrow_field = series
        .field()
        .to_arrow()
        .map_err(|e| CheckpointError::Internal {
            message: format!("failed to convert field to arrow: {e}"),
        })?;
    let schema = Arc::new(Schema::new(vec![arrow_field]));
    let batch = RecordBatch::try_new(schema.clone(), vec![arrow_array]).map_err(|e| {
        CheckpointError::Internal {
            message: format!("failed to create record batch: {e}"),
        }
    })?;

    let mut buf = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None).map_err(|e| {
            CheckpointError::Internal {
                message: format!("failed to create parquet writer: {e}"),
            }
        })?;
        writer
            .write(&batch)
            .map_err(|e| CheckpointError::Internal {
                message: format!("failed to write parquet batch: {e}"),
            })?;
        writer.close().map_err(|e| CheckpointError::Internal {
            message: format!("failed to close parquet writer: {e}"),
        })?;
    }
    Ok(buf)
}

/// Deserialize Parquet bytes produced by [`write_series_as_parquet`] back
/// into a [`Series`]. Expects exactly one column; returns an empty series
/// if the parquet file has zero row groups (e.g. `stage_keys` was called
/// with an empty input).
pub(super) fn read_series_from_parquet(data: &[u8]) -> CheckpointResult<Series> {
    let bytes = Bytes::copy_from_slice(data);
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(|e| CheckpointError::Internal {
            message: format!("failed to open parquet reader: {e}"),
        })?;

    let parquet_schema = builder.schema().clone();
    let reader = builder.build().map_err(|e| CheckpointError::Internal {
        message: format!("failed to build parquet reader: {e}"),
    })?;

    let batches: Vec<RecordBatch> =
        reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| CheckpointError::Internal {
                message: format!("failed to read parquet batches: {e}"),
            })?;

    // Recover the daft Field from the schema — works regardless of whether
    // there are any row groups (empty series on the write side produces zero
    // row groups, but the schema is always written).
    let arrow_field = parquet_schema.fields()[0].clone();
    let daft_field =
        Arc::new(
            Field::try_from(arrow_field.as_ref()).map_err(|e| CheckpointError::Internal {
                message: format!("failed to convert arrow field to daft field: {e}"),
            })?,
        );

    let array = match batches.into_iter().next() {
        Some(batch) => batch.column(0).clone(),
        // No row groups → return an empty array of the right type.
        None => arrow_array::new_empty_array(arrow_field.data_type()),
    };
    Series::from_arrow(daft_field, array).map_err(|e| CheckpointError::Internal {
        message: format!("failed to create series from arrow: {e}"),
    })
}
