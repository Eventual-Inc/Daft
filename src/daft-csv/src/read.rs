use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow2::{
    datatypes::Field,
    io::csv::read_async::{
        deserialize_batch, deserialize_column, infer, infer_schema, read_rows, AsyncReaderBuilder,
        ByteRecord,
    },
};
use async_compat::CompatExt;
use common_error::DaftResult;
use daft_core::{
    schema::{Schema, SchemaRef},
    utils::arrow::cast_array_for_daft_if_needed,
    Series,
};
use daft_io::{get_runtime, GetResult, IOClient, IOStatsRef};
use daft_table::Table;
use futures::{AsyncRead, AsyncSeek};
use rayon::prelude::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};
use snafu::ResultExt;
use tokio::fs::File;
use tokio_util::io::StreamReader;

use crate::metadata::read_csv_schema_single;

#[allow(clippy::too_many_arguments)]
pub fn read_csv(
    uri: &str,
    column_names: Option<Vec<&str>>,
    include_columns: Option<Vec<&str>>,
    num_rows: Option<usize>,
    has_header: bool,
    delimiter: Option<u8>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    multithreaded_io: bool,
    schema: Option<SchemaRef>,
    buffer_size: Option<usize>,
    chunk_size: Option<usize>,
    max_chunks_in_flight: Option<usize>,
    estimated_mean_row_size: Option<usize>,
) -> DaftResult<Table> {
    let runtime_handle = get_runtime(multithreaded_io)?;
    let _rt_guard = runtime_handle.enter();
    runtime_handle.block_on(async {
        read_csv_single(
            uri,
            column_names,
            include_columns,
            num_rows,
            has_header,
            delimiter,
            io_client,
            io_stats,
            schema,
            buffer_size,
            chunk_size,
            max_chunks_in_flight,
            estimated_mean_row_size,
        )
        .await
    })
}

#[allow(clippy::too_many_arguments)]
async fn read_csv_single(
    uri: &str,
    column_names: Option<Vec<&str>>,
    include_columns: Option<Vec<&str>>,
    num_rows: Option<usize>,
    has_header: bool,
    delimiter: Option<u8>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    schema: Option<SchemaRef>,
    buffer_size: Option<usize>,
    chunk_size: Option<usize>,
    max_chunks_in_flight: Option<usize>,
    estimated_mean_row_size: Option<usize>,
) -> DaftResult<Table> {
    let (schema, estimated_mean_row_size) = match schema {
        Some(schema) => (schema.to_arrow()?, estimated_mean_row_size),
        None => {
            let (schema, estimated_mean_row_size) = read_csv_schema_single(
                uri,
                has_header,
                delimiter,
                Some(1 << 20),
                io_client.clone(),
                io_stats.clone(),
            )
            .await?;
            (schema.to_arrow()?, Some(estimated_mean_row_size))
        }
    };
    match io_client
        .single_url_get(uri.to_string(), None, io_stats)
        .await?
    {
        GetResult::File(file) => {
            read_csv_single_from_reader(
                File::open(file.path).await?.compat(),
                column_names,
                include_columns,
                num_rows,
                has_header,
                delimiter,
            )
            .await
        }
        GetResult::Stream(stream, _, _) => {
            let stream_reader = StreamReader::new(stream).compat();
            let mut reader = AsyncReaderBuilder::new()
                .has_headers(has_header)
                .delimiter(delimiter.unwrap_or(b','))
                // Default buffer capacity of 512 KiB.
                .buffer_capacity(buffer_size.unwrap_or(512 * 1024))
                .create_reader(stream_reader);
            let mut fields = schema.fields;
            if let Some(column_names) = column_names {
                fields = fields
                    .into_iter()
                    .zip(column_names.iter())
                    .map(|(field, name)| {
                        Field::new(*name, field.data_type, field.is_nullable)
                            .with_metadata(field.metadata)
                    })
                    .collect();
            }
            let field_name_to_idx = fields
                .iter()
                .enumerate()
                .map(|(idx, f)| (f.name.as_ref(), idx))
                .collect::<HashMap<&str, usize>>();
            let projection_indices = Arc::new(include_columns.as_ref().map_or_else(
                || (0..fields.len()).collect(),
                |cols| {
                    cols.iter()
                        .map(|c| field_name_to_idx[c])
                        .collect::<Vec<_>>()
                },
            ));
            let num_projected_fields = projection_indices.len();
            let unpruned_fields = Arc::new(fields.clone());
            let num_rows = num_rows.unwrap_or(usize::MAX);
            // Default of 64 KiB per chunk.
            let chunk_size = chunk_size.unwrap_or(64 * 1024);
            let chunk_size_rows = {
                // If no estimated row size information from schema inference, we assume 200 bytes per row.
                // With an 64 KiB targeted chunk size, this would result in a chunk size of ~328 rows.
                let estimated_rows_per_desired_chunk =
                    chunk_size / estimated_mean_row_size.unwrap_or(200);
                // Process at least 8 rows in a chunk, even if the rows are pretty large.
                estimated_rows_per_desired_chunk.max(8).min(num_rows)
            };
            // Default max chunks in flight would result in 2 * 8 * 1024 * chunk_size bytes, or 1 GiB with the default chunk size.
            let max_chunks_in_flight = max_chunks_in_flight.unwrap_or(8 * 1024);
            let semaphore = Arc::new(tokio::sync::Semaphore::new(max_chunks_in_flight));
            // Number of rows read in last read.
            let mut rows_read = 1;
            // Total number of rows read across all reads.
            let mut total_rows_read = 0;
            let mut chunk_idx = 0;
            let mut handles = vec![];
            while rows_read > 0 && total_rows_read < num_rows {
                let mut buffer = vec![
                    ByteRecord::with_capacity(
                        estimated_mean_row_size.unwrap_or(0),
                        fields.len()
                    );
                    chunk_size_rows
                ];
                let mut rows = buffer.as_mut_slice();
                if rows.len() > num_rows - total_rows_read {
                    // If we need to read less than the entire row buffer, truncate the buffer to the number
                    // of rows that we actually want to read.
                    rows = &mut rows[..num_rows - total_rows_read + 1]
                }
                let permit = semaphore.clone().acquire_owned().await.unwrap();
                rows_read = read_rows(&mut reader, 0, rows).await?;
                total_rows_read += rows_read;
                let async_closure = {
                    let (unpruned_fields, projection_indices, chunk_idx) = (
                        unpruned_fields.clone(),
                        projection_indices.clone(),
                        chunk_idx,
                    );
                    async move {
                        let (send, recv) = tokio::sync::oneshot::channel();
                        rayon::spawn(move || {
                            let result = (move || {
                                let chunk = projection_indices
                                    .par_iter()
                                    .map(|idx| {
                                        let column = *idx;
                                        deserialize_column(
                                            &buffer.as_slice()[..rows_read],
                                            column,
                                            unpruned_fields[column].data_type().clone(),
                                            0,
                                        )
                                    })
                                    .collect::<arrow2::error::Result<Vec<Box<dyn arrow2::array::Array>>>>()?;
                                DaftResult::Ok((chunk_idx, chunk))
                            })();
                            let _ = send.send(result);
                        });
                        let result = recv.await.context(super::OneShotRecvSnafu {})?;
                        drop(permit);
                        result
                    }
                };
                let handle = tokio::spawn(async_closure);
                handles.push(handle);
                chunk_idx += 1;
            }
            let mut chunks = futures::future::try_join_all(handles)
                .await
                .context(super::JoinSnafu {})?
                .into_iter()
                .collect::<DaftResult<Vec<_>>>()?;
            chunks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            let mut column_arrays = vec![vec![]; num_projected_fields];
            for chunk in chunks.into_iter() {
                for (idx, col) in chunk.1.into_iter().enumerate() {
                    column_arrays[idx].push(col);
                }
            }
            if let Some(include_columns) = include_columns {
                // Truncate fields to only contain projected columns.
                let include_columns: HashSet<&str> = include_columns.into_iter().collect();
                fields.retain(|f| include_columns.contains(f.name.as_str()))
            }
            let columns_series = column_arrays
                .into_par_iter()
                .zip(fields.clone())
                .map(|(mut arrays, field)| {
                    let array = if arrays.len() > 1 {
                        // Concatenate all array chunks.
                        let unboxed_arrays = arrays.iter().map(Box::as_ref).collect::<Vec<_>>();
                        arrow2::compute::concatenate::concatenate(unboxed_arrays.as_slice())?
                    } else {
                        // Return single array chunk directly.
                        arrays.pop().unwrap()
                    };
                    Series::try_from((field.name.as_ref(), cast_array_for_daft_if_needed(array)))
                })
                .collect::<DaftResult<Vec<Series>>>()?;
            let schema: arrow2::datatypes::Schema = fields.into();
            let daft_schema = Schema::try_from(&schema)?;
            Table::new(daft_schema, columns_series)
        }
    }
}

async fn read_csv_single_from_reader<R>(
    reader: R,
    column_names: Option<Vec<&str>>,
    include_columns: Option<Vec<&str>>,
    num_rows: Option<usize>,
    has_header: bool,
    delimiter: Option<u8>,
) -> DaftResult<Table>
where
    R: AsyncRead + AsyncSeek + Unpin + Sync + Send,
{
    let mut reader = AsyncReaderBuilder::new()
        .has_headers(has_header)
        .delimiter(delimiter.unwrap_or(b','))
        .create_reader(reader);
    let (mut fields, _) = infer_schema(&mut reader, None, has_header, &infer).await?;
    if let Some(column_names) = column_names {
        fields = fields
            .into_iter()
            .zip(column_names.iter())
            .map(|(field, name)| {
                Field::new(*name, field.data_type, field.is_nullable).with_metadata(field.metadata)
            })
            .collect();
    }
    let field_name_to_idx = fields
        .iter()
        .enumerate()
        .map(|(idx, f)| (f.name.as_ref(), idx))
        .collect::<HashMap<&str, usize>>();
    let projection_indices = include_columns.as_ref().map(|cols| {
        cols.iter()
            .map(|c| field_name_to_idx[c])
            .collect::<Vec<_>>()
    });
    let num_rows = num_rows.unwrap_or(usize::MAX);
    // TODO(Clark): Make batch size configurable.
    // TODO(Clark): Get estimated average row size in bytes during schema inference and use it to:
    //   1. Set a reasonable batch size.
    //   2. Preallocate per-column batch vecs.
    //   3. Preallocate column Arrow array buffers.
    let batch_size = 1024.min(num_rows);
    // TODO(Clark): Instead of allocating an array-per-column-batch and concatenating at the end,
    // progressively grow a single array per column (with the above preallocation based on estimated
    // number of rows).
    let mut column_arrays = vec![
        vec![];
        projection_indices
            .as_ref()
            .map(|p| p.len())
            .unwrap_or(fields.len())
    ];
    let mut buffer = vec![ByteRecord::with_capacity(0, fields.len()); batch_size];
    let mut rows = buffer.as_mut_slice();
    // Number of rows read in last read.
    let mut rows_read = 1;
    // Total number of rows read across all reads.
    let mut total_rows_read = 0;
    while rows_read > 0 && total_rows_read < num_rows {
        if rows.len() > num_rows - total_rows_read {
            // If we need to read less than the entire row buffer, truncate the buffer to the number
            // of rows that we actually want to read.
            rows = &mut rows[..num_rows - total_rows_read + 1]
        }
        rows_read = read_rows(&mut reader, 0, rows).await?;
        total_rows_read += rows_read;
        // TODO(Clark): Parallelize column deserialization over a rayon threadpool.
        for (idx, array) in deserialize_batch(
            &rows[..rows_read],
            &fields,
            projection_indices.as_deref(),
            0,
            deserialize_column,
        )?
        .into_arrays()
        .into_iter()
        .enumerate()
        {
            column_arrays[idx].push(array);
        }
    }
    if let Some(include_columns) = include_columns {
        // Truncate fields to only contain projected columns.
        let include_columns: HashSet<&str> = include_columns.into_iter().collect();
        fields.retain(|f| include_columns.contains(f.name.as_str()))
    }
    let columns_series = column_arrays
        .into_iter()
        .zip(fields.iter())
        .map(|(mut arrays, field)| {
            let array = if arrays.len() > 1 {
                // Concatenate all array chunks.
                let unboxed_arrays = arrays.iter().map(Box::as_ref).collect::<Vec<_>>();
                arrow2::compute::concatenate::concatenate(unboxed_arrays.as_slice())?
            } else {
                // Return single array chunk directly.
                arrays.pop().unwrap()
            };
            Series::try_from((field.name.as_ref(), cast_array_for_daft_if_needed(array)))
        })
        .collect::<DaftResult<Vec<Series>>>()?;
    let schema: arrow2::datatypes::Schema = fields.into();
    let daft_schema = Schema::try_from(&schema)?;
    Table::new(daft_schema, columns_series)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;

    use daft_core::{datatypes::Field, schema::Schema, DataType};
    use daft_io::{IOClient, IOConfig};

    use super::read_csv;

    #[test]
    fn test_csv_read_from_s3() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/csv-dev/mvp.csv";

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(
            file, None, None, None, true, None, io_client, None, true, None, None, None, None, None,
        )?;
        assert_eq!(table.len(), 100);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("a", DataType::Int64),
                Field::new("b", DataType::Utf8)
            ])?
            .into(),
        );

        Ok(())
    }

    #[test]
    fn test_csv_read_from_s3_larger_than_batch_size() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/csv-dev/medium.csv";

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(
            file, None, None, None, true, None, io_client, None, true, None, None, None, None, None,
        )?;
        assert_eq!(table.len(), 5000);

        Ok(())
    }

    #[test]
    fn test_csv_read_from_s3_limit() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/csv-dev/mvp.csv";

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(
            file,
            None,
            None,
            Some(10),
            true,
            None,
            io_client,
            None,
            true,
            None,
            None,
            None,
            None,
            None,
        )?;
        assert_eq!(table.len(), 10);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("a", DataType::Int64),
                Field::new("b", DataType::Utf8)
            ])?
            .into(),
        );

        Ok(())
    }

    #[test]
    fn test_csv_read_from_s3_projection() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/csv-dev/mvp.csv";

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(
            file,
            None,
            Some(vec!["b"]),
            None,
            true,
            None,
            io_client,
            None,
            true,
            None,
            None,
            None,
            None,
            None,
        )?;
        assert_eq!(table.len(), 100);
        assert_eq!(
            table.schema,
            Schema::new(vec![Field::new("b", DataType::Utf8)])?.into(),
        );

        Ok(())
    }
}
