//! Arrow-rs based parquet reader.
//!
//! This module provides a parquet reader built on the arrow-rs `parquet` crate,
//! replacing the parquet2/arrow2 decode pipeline. It uses [`DaftAsyncFileReader`]
//! as the IO bridge for remote reads, and the sync `ParquetRecordBatchReaderBuilder`
//! with `std::fs::File` for local reads (avoiding IOClient overhead).

use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::*;
use daft_dsl::{ExprRef, expr::bound_expr::BoundExpr};
use daft_io::{IOClient, IOStatsRef};
use daft_recordbatch::RecordBatch;
use daft_stats::TruthValue;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
        async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder},
    },
    file::metadata::ParquetMetaData,
};
use rayon::prelude::*;

use crate::{
    async_reader::DaftAsyncFileReader,
    read::ParquetSchemaInferenceOptions,
    schema_inference::{arrow_schema_to_daft_schema, infer_schema_from_parquet_metadata_arrowrs},
    statistics::arrowrs_row_group_metadata_to_table_stats,
};

/// Default batch size for the arrow-rs reader (number of rows per batch).
const DEFAULT_BATCH_SIZE: usize = 8192;

/// Convert a parquet error to a DaftError.
fn parquet_err(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> common_error::DaftError {
    common_error::DaftError::External(e.into())
}

/// Build a `ProjectionMask` from the given column names and arrow schema.
fn build_projection_mask(
    col_set: &std::collections::HashSet<&str>,
    arrow_schema: &arrow::datatypes::Schema,
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
) -> ProjectionMask {
    ProjectionMask::roots(
        parquet_schema,
        arrow_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| col_set.contains(f.name().as_str()))
            .map(|(i, _)| i),
    )
}

/// Project a Daft schema to only the columns in `col_set`.
fn project_daft_schema(schema: &Schema, col_set: &std::collections::HashSet<&str>) -> Schema {
    Schema::new(
        schema
            .into_iter()
            .filter(|f| col_set.contains(f.name.as_str()))
            .cloned(),
    )
}

/// Infer the arrow and Daft schemas from parquet metadata with Daft options.
fn infer_schemas(
    parquet_metadata: &ParquetMetaData,
    schema_infer_options: &ParquetSchemaInferenceOptions,
) -> DaftResult<(arrow::datatypes::Schema, Schema)> {
    let arrow_schema = infer_schema_from_parquet_metadata_arrowrs(
        parquet_metadata,
        Some(schema_infer_options.coerce_int96_timestamp_unit),
        schema_infer_options.string_encoding
            == daft_arrow::io::parquet::read::schema::StringEncoding::Raw,
    )
    .map_err(parquet_err)?;
    let daft_schema = arrow_schema_to_daft_schema(&arrow_schema)?;
    Ok((arrow_schema, daft_schema))
}

/// Read a single parquet file into a Daft [`RecordBatch`] using the arrow-rs reader.
///
/// This is the arrow-rs equivalent of the parquet2-based `read_parquet_single`.
#[allow(clippy::too_many_arguments)]
pub async fn read_parquet_single_arrowrs(
    uri: &str,
    columns: Option<&[&str]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<&[i64]>,
    predicate: Option<ExprRef>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    metadata: Option<Arc<ParquetMetaData>>,
    batch_size: Option<usize>,
) -> DaftResult<RecordBatch> {
    // 1. Create the async file reader and fetch metadata.
    let mut reader = DaftAsyncFileReader::new(uri.to_string(), io_client, io_stats, metadata, None);
    let parquet_metadata = reader.get_metadata(None).await.map_err(parquet_err)?;

    // 2. Infer schema with Daft options (INT96 coercion, string encoding).
    let (arrow_schema, daft_schema) = infer_schemas(&parquet_metadata, &schema_infer_options)?;

    // 3. Build the stream builder with our custom schema.
    let options = ArrowReaderOptions::new().with_schema(Arc::new(arrow_schema.clone()));
    let arrow_reader_metadata =
        ArrowReaderMetadata::try_new(parquet_metadata.clone(), options).map_err(parquet_err)?;
    let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arrow_reader_metadata);

    // 4. Apply column projection.
    let (projected_daft_schema, builder) = if let Some(cols) = columns {
        let col_set: std::collections::HashSet<&str> = cols.iter().copied().collect();
        let mask = build_projection_mask(&col_set, &arrow_schema, builder.parquet_schema());
        (
            project_daft_schema(&daft_schema, &col_set),
            builder.with_projection(mask),
        )
    } else {
        (daft_schema, builder)
    };

    // 5. Determine which row groups to read (with predicate-based pruning).
    let rg_indices = prune_row_groups(
        &parquet_metadata,
        row_groups,
        predicate.as_ref(),
        &projected_daft_schema,
        uri,
    )?;

    if rg_indices.is_empty() {
        return Ok(RecordBatch::empty(Some(Arc::new(projected_daft_schema))));
    }

    // 6. Apply row groups, offset, limit, and batch size.
    let mut builder = builder.with_row_groups(rg_indices);
    if let Some(offset) = start_offset {
        builder = builder.with_offset(offset);
    }
    if let Some(limit) = num_rows {
        builder = builder.with_limit(limit);
    }
    builder = builder.with_batch_size(batch_size.unwrap_or(DEFAULT_BATCH_SIZE));

    // 7. Build the stream and collect all batches.
    let stream = builder.build().map_err(parquet_err)?;
    let arrow_batches: Vec<arrow::array::RecordBatch> =
        stream.try_collect().await.map_err(parquet_err)?;

    // 8. Convert to Daft RecordBatch.
    crate::arrow_bridge::arrowrs_batches_to_daft_recordbatch(&arrow_batches, &projected_daft_schema)
}

/// Read a local parquet file using the sync arrow-rs reader with parallel row group decode.
///
/// This avoids the overhead of `DaftAsyncFileReader` + `IOClient` for local files
/// by using `std::fs::File` directly with `ParquetRecordBatchReaderBuilder`.
/// Row groups are decoded in parallel using rayon, matching the parquet2 reader's
/// parallelism strategy.
#[allow(clippy::too_many_arguments)]
pub fn local_parquet_read_arrowrs(
    path: &str,
    columns: Option<&[&str]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<&[i64]>,
    predicate: Option<ExprRef>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    batch_size: Option<usize>,
) -> DaftResult<RecordBatch> {
    // 1. Open the file and read metadata once.
    let file = std::fs::File::open(path).map_err(|e| {
        parquet_err(format!(
            "Failed to open local parquet file '{}': {}",
            path, e
        ))
    })?;

    let arrow_reader_metadata =
        ArrowReaderMetadata::load(&file, ArrowReaderOptions::new()).map_err(parquet_err)?;
    let parquet_metadata = arrow_reader_metadata.metadata().clone();

    // 2. Infer schema with Daft options.
    let (arrow_schema, daft_schema) = infer_schemas(&parquet_metadata, &schema_infer_options)?;

    // 3. Rebuild metadata with our custom schema.
    let options = ArrowReaderOptions::new().with_schema(Arc::new(arrow_schema.clone()));
    let arrow_reader_metadata =
        ArrowReaderMetadata::try_new(parquet_metadata.clone(), options).map_err(parquet_err)?;

    // 4. Compute column projection.
    let col_set: Option<std::collections::HashSet<&str>> =
        columns.map(|cols| cols.iter().copied().collect());

    let projected_daft_schema = if let Some(ref col_set) = col_set {
        project_daft_schema(&daft_schema, col_set)
    } else {
        daft_schema
    };

    // 5. Row group pruning.
    let rg_indices = prune_row_groups(
        &parquet_metadata,
        row_groups,
        predicate.as_ref(),
        &projected_daft_schema,
        path,
    )?;

    if rg_indices.is_empty() {
        return Ok(RecordBatch::empty(Some(Arc::new(projected_daft_schema))));
    }

    // 6. Apply offset and limit: trim row groups to only those we need.
    let start = start_offset.unwrap_or(0);
    let limit = num_rows;
    // Use a large batch size so each row group produces a single batch,
    // matching parquet2's behavior and avoiding per-batch concat overhead.
    // Default to reading each row group as a single batch.
    let batch_size = batch_size.unwrap_or_else(|| {
        // Calculate the max row group size to use as batch size.
        rg_indices
            .iter()
            .map(|&idx| parquet_metadata.row_group(idx).num_rows() as usize)
            .max()
            .unwrap_or(256 * 1024)
    });

    // Trim the row group list if we have an offset/limit so we don't decode
    // row groups we'll never use.
    let rg_indices = if start > 0 || limit.is_some() {
        let mut trimmed = Vec::new();
        let mut cumulative_rows = 0usize;
        let rows_needed = start + limit.unwrap_or(usize::MAX - start);
        for &rg_idx in &rg_indices {
            let rg_rows = parquet_metadata.row_group(rg_idx).num_rows() as usize;
            if cumulative_rows >= rows_needed {
                break;
            }
            trimmed.push(rg_idx);
            cumulative_rows += rg_rows;
        }
        trimmed
    } else {
        rg_indices
    };

    // 7a. Fast path for single row group: skip rayon dispatch, use limit/offset directly.
    if rg_indices.len() == 1 {
        let mut rg_builder =
            ParquetRecordBatchReaderBuilder::new_with_metadata(file, arrow_reader_metadata);
        if let Some(ref col_set) = col_set {
            let mask = build_projection_mask(col_set, &arrow_schema, rg_builder.parquet_schema());
            rg_builder = rg_builder.with_projection(mask);
        }
        rg_builder = rg_builder
            .with_row_groups(rg_indices)
            .with_batch_size(batch_size);
        if let Some(off) = start_offset {
            rg_builder = rg_builder.with_offset(off);
        }
        if let Some(lim) = limit {
            rg_builder = rg_builder.with_limit(lim);
        }
        let reader = rg_builder.build().map_err(parquet_err)?;
        let arrow_batches: Vec<arrow::array::RecordBatch> =
            reader.collect::<Result<Vec<_>, _>>().map_err(parquet_err)?;
        return crate::arrow_bridge::arrowrs_batches_to_daft_recordbatch(
            &arrow_batches,
            &projected_daft_schema,
        );
    }

    // 7b. Decode row groups in parallel using rayon.
    //    Each thread opens its own file handle (cheap syscall). This is better than
    //    reading the entire file into memory because the arrow-rs reader uses seeks
    //    to read only the projected column chunks, which is critical for projection
    //    performance on wide tables.
    let path_owned = path.to_string();
    let arrow_reader_metadata = Arc::new(arrow_reader_metadata);

    let rg_batches: Vec<DaftResult<Vec<arrow::array::RecordBatch>>> = rg_indices
        .par_iter()
        .map(|&rg_idx| {
            let file = std::fs::File::open(&path_owned)
                .map_err(|e| parquet_err(format!("Failed to open '{}': {}", path_owned, e)))?;
            let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
                file,
                (*arrow_reader_metadata).clone(),
            );
            if let Some(ref col_set) = col_set {
                let mask = build_projection_mask(col_set, &arrow_schema, builder.parquet_schema());
                builder = builder.with_projection(mask);
            }
            let reader = builder
                .with_row_groups(vec![rg_idx])
                .with_batch_size(batch_size)
                .build()
                .map_err(parquet_err)?;
            reader.collect::<Result<Vec<_>, _>>().map_err(parquet_err)
        })
        .collect();

    // 8. Flatten batches from all row groups, applying offset/limit.
    let mut all_batches = Vec::new();
    let mut rows_skipped = 0usize;
    let mut rows_collected = 0usize;

    for rg_result in rg_batches {
        let batches = rg_result?;
        for batch in batches {
            let batch_rows = batch.num_rows();

            // Handle offset: skip initial rows.
            if rows_skipped < start {
                let skip_in_batch = (start - rows_skipped).min(batch_rows);
                rows_skipped += skip_in_batch;
                if skip_in_batch == batch_rows {
                    continue;
                }
                // Partially skip this batch.
                let remaining = batch.slice(skip_in_batch, batch_rows - skip_in_batch);
                let take = if let Some(lim) = limit {
                    remaining.num_rows().min(lim - rows_collected)
                } else {
                    remaining.num_rows()
                };
                all_batches.push(remaining.slice(0, take));
                rows_collected += take;
            } else {
                let take = if let Some(lim) = limit {
                    batch_rows.min(lim - rows_collected)
                } else {
                    batch_rows
                };
                if take < batch_rows {
                    all_batches.push(batch.slice(0, take));
                } else {
                    all_batches.push(batch);
                }
                rows_collected += take;
            }

            if limit.is_some_and(|lim| rows_collected >= lim) {
                break;
            }
        }
        if limit.is_some_and(|lim| rows_collected >= lim) {
            break;
        }
    }

    // 9. Convert to Daft RecordBatch.
    crate::arrow_bridge::arrowrs_batches_to_daft_recordbatch(&all_batches, &projected_daft_schema)
}

/// Stream a single parquet file as Daft [`RecordBatch`]es using the arrow-rs reader.
///
/// This is the arrow-rs equivalent of the parquet2-based `stream_parquet_single`.
#[allow(clippy::too_many_arguments)]
pub async fn stream_parquet_single_arrowrs(
    uri: &str,
    columns: Option<&[&str]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<&[i64]>,
    predicate: Option<ExprRef>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    metadata: Option<Arc<ParquetMetaData>>,
    batch_size: Option<usize>,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    // 1. Create the async file reader and fetch metadata.
    let mut reader = DaftAsyncFileReader::new(uri.to_string(), io_client, io_stats, metadata, None);
    let parquet_metadata = reader.get_metadata(None).await.map_err(parquet_err)?;

    // 2. Infer schema with Daft options.
    let (arrow_schema, daft_schema) = infer_schemas(&parquet_metadata, &schema_infer_options)?;

    // 3. Build the stream builder with our custom schema.
    let options = ArrowReaderOptions::new().with_schema(Arc::new(arrow_schema.clone()));
    let arrow_reader_metadata =
        ArrowReaderMetadata::try_new(parquet_metadata.clone(), options).map_err(parquet_err)?;
    let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arrow_reader_metadata);

    // 4. Apply column projection.
    let (projected_daft_schema, builder) = if let Some(cols) = columns {
        let col_set: std::collections::HashSet<&str> = cols.iter().copied().collect();
        let mask = build_projection_mask(&col_set, &arrow_schema, builder.parquet_schema());
        (
            project_daft_schema(&daft_schema, &col_set),
            builder.with_projection(mask),
        )
    } else {
        (daft_schema, builder)
    };

    // 5. Row group pruning.
    let rg_indices = prune_row_groups(
        &parquet_metadata,
        row_groups,
        predicate.as_ref(),
        &projected_daft_schema,
        uri,
    )?;

    if rg_indices.is_empty() {
        return Ok(futures::stream::empty().boxed());
    }

    // 6. Apply row groups, offset, limit, and batch size.
    let mut builder = builder.with_row_groups(rg_indices);
    if let Some(offset) = start_offset {
        builder = builder.with_offset(offset);
    }
    if let Some(limit) = num_rows {
        builder = builder.with_limit(limit);
    }
    builder = builder.with_batch_size(batch_size.unwrap_or(DEFAULT_BATCH_SIZE));

    // 7. Build the stream, mapping each arrow-rs batch to a Daft RecordBatch.
    let stream = builder.build().map_err(parquet_err)?;
    let daft_schema = Arc::new(projected_daft_schema);
    let mapped = stream.map(move |result| {
        let arrow_batch = result.map_err(parquet_err)?;
        crate::arrow_bridge::arrowrs_to_daft_recordbatch(&arrow_batch, &daft_schema)
    });

    Ok(mapped.boxed())
}

/// Determine which row groups to read, applying predicate-based pruning.
///
/// Returns the list of row group indices to read. If `requested_row_groups` is Some,
/// only those are considered; otherwise all row groups are candidates.
fn prune_row_groups(
    metadata: &ParquetMetaData,
    requested_row_groups: Option<&[i64]>,
    predicate: Option<&ExprRef>,
    schema: &Schema,
    uri: &str,
) -> DaftResult<Vec<usize>> {
    let num_row_groups = metadata.num_row_groups();

    // Determine candidate row groups.
    let candidates: Vec<usize> = if let Some(rgs) = requested_row_groups {
        rgs.iter()
            .map(|&i| {
                let idx = i as usize;
                if idx >= num_row_groups {
                    Err(common_error::DaftError::ValueError(format!(
                        "Row group index {} out of bounds for '{}' (has {} row groups)",
                        i, uri, num_row_groups
                    )))
                } else {
                    Ok(idx)
                }
            })
            .collect::<DaftResult<Vec<_>>>()?
    } else {
        (0..num_row_groups).collect()
    };

    // If no predicate, return all candidates.
    let predicate = match predicate {
        Some(p) => p,
        None => return Ok(candidates),
    };

    // Bind the predicate expression to the schema.
    let bound_pred = BoundExpr::try_new((*predicate).clone(), schema).map_err(|e| {
        common_error::DaftError::ValueError(format!(
            "Failed to bind predicate for row group pruning on '{}': {}",
            uri, e
        ))
    })?;

    // Evaluate the predicate against each row group's statistics.
    let mut result = Vec::with_capacity(candidates.len());
    for rg_idx in candidates {
        let rg_meta = metadata.row_group(rg_idx);
        match arrowrs_row_group_metadata_to_table_stats(rg_meta, schema) {
            Ok(stats) => {
                let evaled = stats.eval_expression(&bound_pred)?;
                if evaled.to_truth_value() != TruthValue::False {
                    result.push(rg_idx);
                }
                // else: predicate definitively false for this RG, skip it
            }
            Err(_) => {
                // If stats are unavailable/unparsable, include the RG (be conservative).
                result.push(rg_idx);
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::Int32Array,
        datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
    };
    use parquet::{arrow::ArrowWriter, file::reader::FileReader};

    use super::*;

    /// Create parquet metadata from an in-memory parquet file.
    fn create_test_parquet_metadata() -> Arc<parquet::file::metadata::ParquetMetaData> {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("a", ArrowDataType::Int32, true),
            ArrowField::new("b", ArrowDataType::Int32, true),
        ]));

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None).unwrap();

        let batch = arrow::array::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Read back the metadata from the in-memory buffer.
        let reader =
            parquet::file::serialized_reader::SerializedFileReader::new(bytes::Bytes::from(buf))
                .unwrap();
        Arc::new(reader.metadata().clone())
    }

    #[test]
    fn test_prune_row_groups_no_predicate() {
        let metadata = create_test_parquet_metadata();
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32),
            Field::new("b", DataType::Int32),
        ]);

        let result = prune_row_groups(&metadata, None, None, &schema, "test.parquet").unwrap();
        // Should return all row groups.
        assert_eq!(result.len(), metadata.num_row_groups());
    }

    #[test]
    fn test_prune_row_groups_with_selection() {
        let metadata = create_test_parquet_metadata();
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32),
            Field::new("b", DataType::Int32),
        ]);

        // Request specific row group 0.
        let result =
            prune_row_groups(&metadata, Some(&[0]), None, &schema, "test.parquet").unwrap();
        assert_eq!(result, vec![0]);
    }

    #[test]
    fn test_prune_row_groups_out_of_bounds() {
        let metadata = create_test_parquet_metadata();
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32),
            Field::new("b", DataType::Int32),
        ]);

        // Request out-of-bounds row group.
        let result = prune_row_groups(&metadata, Some(&[999]), None, &schema, "test.parquet");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_read_parquet_single_arrowrs_from_local_file() {
        use daft_io::IOConfig;

        // Write a test parquet file to a temp directory.
        let dir = std::env::temp_dir().join("daft_parquet_test_arrowrs");
        std::fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("test.parquet");

        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("x", ArrowDataType::Int32, true),
            ArrowField::new("y", ArrowDataType::Utf8, true),
        ]));

        let file = std::fs::File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
        let batch = arrow::array::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![10, 20, 30])),
                Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Read through the new reader.
        let uri = file_path.to_str().unwrap();
        let io_client = Arc::new(IOClient::new(IOConfig::default().into()).unwrap());

        let result = read_parquet_single_arrowrs(
            uri,
            None, // all columns
            None, // no offset
            None, // no limit
            None, // all row groups
            None, // no predicate
            io_client,
            None, // no io_stats
            ParquetSchemaInferenceOptions::default(),
            None, // no cached metadata
            None, // default batch size
        )
        .await
        .unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.num_columns(), 2);

        // Verify column names.
        assert_eq!(result.schema.fields()[0].name, "x");
        assert_eq!(result.schema.fields()[1].name, "y");

        // Clean up.
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_read_parquet_single_arrowrs_with_column_projection() {
        use daft_io::IOConfig;

        let dir = std::env::temp_dir().join("daft_parquet_test_arrowrs_proj");
        std::fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("test.parquet");

        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("x", ArrowDataType::Int32, true),
            ArrowField::new("y", ArrowDataType::Utf8, true),
            ArrowField::new("z", ArrowDataType::Float64, true),
        ]));

        let file = std::fs::File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
        let batch = arrow::array::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "a", "b", "c", "d", "e",
                ])),
                Arc::new(arrow::array::Float64Array::from(vec![
                    1.0, 2.0, 3.0, 4.0, 5.0,
                ])),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let uri = file_path.to_str().unwrap();
        let io_client = Arc::new(IOClient::new(IOConfig::default().into()).unwrap());

        // Read only columns "x" and "z".
        let result = read_parquet_single_arrowrs(
            uri,
            Some(&["x", "z"]),
            None,
            None,
            None,
            None,
            io_client,
            None,
            ParquetSchemaInferenceOptions::default(),
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(result.len(), 5);
        assert_eq!(result.num_columns(), 2);
        assert_eq!(result.schema.fields()[0].name, "x");
        assert_eq!(result.schema.fields()[1].name, "z");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_read_parquet_single_arrowrs_with_limit() {
        use daft_io::IOConfig;

        let dir = std::env::temp_dir().join("daft_parquet_test_arrowrs_limit");
        std::fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("test.parquet");

        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "val",
            ArrowDataType::Int32,
            true,
        )]));

        let file = std::fs::File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
        let batch = arrow::array::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
            ]))],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let uri = file_path.to_str().unwrap();
        let io_client = Arc::new(IOClient::new(IOConfig::default().into()).unwrap());

        // Read with limit of 3.
        let result = read_parquet_single_arrowrs(
            uri,
            None,
            None,
            Some(3), // limit
            None,
            None,
            io_client,
            None,
            ParquetSchemaInferenceOptions::default(),
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(result.len(), 3);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_stream_parquet_single_arrowrs() {
        use daft_io::IOConfig;

        let dir = std::env::temp_dir().join("daft_parquet_test_arrowrs_stream");
        std::fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("test.parquet");

        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int32,
            true,
        )]));

        let file = std::fs::File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
        let batch = arrow::array::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let uri = file_path.to_str().unwrap();
        let io_client = Arc::new(IOClient::new(IOConfig::default().into()).unwrap());

        let mut stream = stream_parquet_single_arrowrs(
            uri,
            None,
            None,
            None,
            None,
            None,
            io_client,
            None,
            ParquetSchemaInferenceOptions::default(),
            None,
            None,
        )
        .await
        .unwrap();

        let mut total_rows = 0;
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            total_rows += batch.len();
        }
        assert_eq!(total_rows, 5);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_read_existing_mvp_parquet() {
        use daft_io::IOConfig;

        // Read the existing MVP test fixture through the new reader.
        let mvp_path = path_macro::path!(
            env!("CARGO_MANIFEST_DIR")
                / ".."
                / ".."
                / "tests"
                / "assets"
                / "parquet-data"
                / "mvp.parquet"
        );
        let uri = mvp_path.to_str().unwrap();
        let io_client = Arc::new(IOClient::new(IOConfig::default().into()).unwrap());

        let result = read_parquet_single_arrowrs(
            uri,
            None,
            None,
            None,
            None,
            None,
            io_client,
            None,
            ParquetSchemaInferenceOptions::default(),
            None,
            None,
        )
        .await
        .unwrap();

        // The MVP parquet file should have some rows and columns.
        assert!(result.len() > 0, "MVP parquet should have rows");
        assert!(result.num_columns() > 0, "MVP parquet should have columns");
    }
}
