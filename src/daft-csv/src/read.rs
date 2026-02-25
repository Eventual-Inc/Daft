use core::str;
use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};

use arrow_schema::Field as ArrowField;
use async_compat::{Compat, CompatExt};
use common_error::{DaftError, DaftResult};
use common_runtime::get_io_runtime;
use csv_async::{AsyncReader, AsyncReaderBuilder};
use daft_compression::CompressionCodec;
use daft_core::prelude::*;
use daft_decoding::deserialize::deserialize_column;
use daft_dsl::{expr::bound_expr::BoundExpr, optimization::get_required_columns};
use daft_io::{GetResult, IOClient, IOStatsRef, SourceType, parse_url};
use daft_recordbatch::RecordBatch;
use futures::{Stream, StreamExt, TryStreamExt, stream::BoxStream};
use rayon::{
    iter::{IndexedParallelIterator, IntoParallelIterator},
    prelude::{IntoParallelRefIterator, ParallelIterator},
};
use snafu::{
    ResultExt,
    futures::{TryFutureExt, try_future::Context},
};
use tokio::{
    fs::File,
    io::{AsyncBufRead, AsyncRead, BufReader},
    task::JoinHandle,
};
use tokio_util::io::StreamReader;

use crate::{CsvConvertOptions, CsvParseOptions, CsvReadOptions, metadata::read_csv_schema_single};

trait ByteRecordChunkStream: Stream<Item = super::Result<Vec<csv_async::ByteRecord>>> {}
impl<S> ByteRecordChunkStream for S where S: Stream<Item = super::Result<Vec<csv_async::ByteRecord>>>
{}

use crate::local::{read_csv_local, stream_csv_local};

type TableChunkResult =
    super::Result<Context<JoinHandle<DaftResult<RecordBatch>>, super::JoinSnafu, super::Error>>;
trait TableStream: Stream<Item = TableChunkResult> {}
impl<S> TableStream for S where S: Stream<Item = TableChunkResult> {}

#[allow(clippy::too_many_arguments)]
pub fn read_csv(
    uri: &str,
    convert_options: Option<CsvConvertOptions>,
    parse_options: Option<CsvParseOptions>,
    read_options: Option<CsvReadOptions>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    multithreaded_io: bool,
    max_chunks_in_flight: Option<usize>,
) -> DaftResult<RecordBatch> {
    let runtime_handle = get_io_runtime(multithreaded_io);
    runtime_handle.block_on_current_thread(async {
        read_csv_single_into_table(
            uri,
            convert_options,
            parse_options,
            read_options,
            io_client,
            io_stats,
            max_chunks_in_flight,
        )
        .await
    })
}

#[allow(clippy::too_many_arguments)]
pub fn read_csv_bulk(
    uris: &[&str],
    convert_options: Option<CsvConvertOptions>,
    parse_options: Option<CsvParseOptions>,
    read_options: Option<CsvReadOptions>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    multithreaded_io: bool,
    max_chunks_in_flight: Option<usize>,
    num_parallel_tasks: usize,
) -> DaftResult<Vec<RecordBatch>> {
    let runtime_handle = get_io_runtime(multithreaded_io);
    let tables = runtime_handle.block_on_current_thread(async move {
        // Launch a read task per URI, throttling the number of concurrent file reads to num_parallel tasks.
        let task_stream = futures::stream::iter(uris.iter().map(|uri| {
            let (uri, convert_options, parse_options, read_options, io_client, io_stats) = (
                (*uri).to_string(),
                convert_options.clone(),
                parse_options.clone(),
                read_options.clone(),
                io_client.clone(),
                io_stats.clone(),
            );
            tokio::task::spawn(async move {
                read_csv_single_into_table(
                    uri.as_str(),
                    convert_options,
                    parse_options,
                    read_options,
                    io_client,
                    io_stats,
                    max_chunks_in_flight,
                )
                .await
            })
            .context(super::JoinSnafu {})
        }));
        let mut remaining_rows = convert_options
            .as_ref()
            .and_then(|opts| opts.limit.map(|limit| limit as i64));
        task_stream
            // Limit the number of file reads we have in flight at any given time.
            .buffered(num_parallel_tasks)
            // Terminate the stream if we have already reached the row limit. With the upstream buffering, we will still read up to
            // num_parallel_tasks redundant files.
            .try_take_while(|result| {
                match (result, remaining_rows) {
                    // Limit has been met, early-terminate.
                    (_, Some(rows_left)) if rows_left <= 0 => futures::future::ready(Ok(false)),
                    // Limit has not yet been met, update remaining limit slack and continue.
                    (Ok(table), Some(rows_left)) => {
                        remaining_rows = Some(rows_left - table.len() as i64);
                        futures::future::ready(Ok(true))
                    }
                    // (1) No limit, never early-terminate.
                    // (2) Encountered error, propagate error to try_collect to allow it to short-circuit.
                    (_, None) | (Err(_), _) => futures::future::ready(Ok(true)),
                }
            })
            .try_collect::<Vec<_>>()
            .await
    })?;

    tables.into_iter().collect::<DaftResult<Vec<_>>>()
}

#[allow(clippy::too_many_arguments)]
pub async fn stream_csv(
    uri: String,
    convert_options: Option<CsvConvertOptions>,
    parse_options: Option<CsvParseOptions>,
    read_options: Option<CsvReadOptions>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    max_chunks_in_flight: Option<usize>,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let (source_type, _) = parse_url(&uri)?;
    let is_compressed = CompressionCodec::from_uri(&uri).is_some();
    if matches!(source_type, SourceType::File) && !is_compressed {
        let stream = stream_csv_local(
            uri,
            convert_options,
            parse_options.unwrap_or_default(),
            read_options,
            io_client,
            io_stats,
            max_chunks_in_flight,
        )
        .await?;
        Ok(Box::pin(stream))
    } else {
        let stream = stream_csv_single(
            uri,
            convert_options,
            parse_options,
            read_options,
            io_client,
            io_stats,
            max_chunks_in_flight,
        )
        .await?;
        Ok(Box::pin(stream))
    }
}

pub fn tables_concat(mut tables: Vec<RecordBatch>) -> DaftResult<RecordBatch> {
    if tables.is_empty() {
        return Err(DaftError::ValueError(
            "Need at least 1 Table to perform concat".to_string(),
        ));
    }
    if tables.len() == 1 {
        return Ok(tables.pop().unwrap());
    }
    let first_table = tables.as_slice().first().unwrap();

    let first_schema = &first_table.schema;
    for tab in tables.iter().skip(1) {
        if tab.schema.as_ref() != first_schema.as_ref() {
            return Err(DaftError::SchemaMismatch(format!(
                "Table concat requires all schemas to match, {} vs {}",
                first_schema, tab.schema
            )));
        }
    }
    let num_columns = first_table.num_columns();
    let new_series = (0..num_columns)
        .into_par_iter()
        .map(|i| {
            let series_to_cat: Vec<&Series> =
                tables.iter().map(|s| s.as_ref().get_column(i)).collect();
            Series::concat(series_to_cat.as_slice())
        })
        .collect::<DaftResult<Vec<_>>>()?;
    RecordBatch::new_with_size(
        first_table.schema.clone(),
        new_series,
        tables.iter().map(daft_recordbatch::RecordBatch::len).sum(),
    )
}

#[allow(clippy::too_many_arguments)]
async fn read_csv_single_into_table(
    uri: &str,
    convert_options: Option<CsvConvertOptions>,
    parse_options: Option<CsvParseOptions>,
    read_options: Option<CsvReadOptions>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    max_chunks_in_flight: Option<usize>,
) -> DaftResult<RecordBatch> {
    let (source_type, _) = parse_url(uri)?;
    let is_compressed = CompressionCodec::from_uri(uri).is_some();
    if matches!(source_type, SourceType::File) && !is_compressed {
        return read_csv_local(
            uri,
            convert_options,
            parse_options.unwrap_or_default(),
            read_options,
            io_client,
            io_stats,
            max_chunks_in_flight,
        )
        .await;
    }

    let predicate = convert_options
        .as_ref()
        .and_then(|opts| opts.predicate.clone());

    let limit = convert_options.as_ref().and_then(|opts| opts.limit);

    let include_columns = convert_options
        .as_ref()
        .and_then(|opts| opts.include_columns.clone());

    let convert_options_with_predicate_columns = match (convert_options, &predicate) {
        (None, _) => None,
        (co, None) => co,
        (Some(mut co), Some(predicate)) => {
            if let Some(ref mut co_include_columns) = co.include_columns {
                let required_columns_for_predicate = get_required_columns(predicate);
                for rc in required_columns_for_predicate {
                    if co_include_columns.iter().all(|c| c.as_str() != rc.as_str()) {
                        co_include_columns.push(rc);
                    }
                }
            }
            // if we have a limit and a predicate, remove limit for stream
            co.limit = None;
            Some(co)
        }
    };

    let (chunk_stream, fields) = read_csv_single_into_stream(
        uri.to_string(),
        convert_options_with_predicate_columns.unwrap_or_default(),
        parse_options.unwrap_or_default(),
        read_options,
        io_client,
        io_stats,
    )
    .await?;

    // Default max chunks in flight is set to 2x the number of cores, which should ensure pipelining of reading chunks
    // with the parsing of chunks on the rayon threadpool.
    let max_chunks_in_flight = max_chunks_in_flight.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .unwrap_or(NonZeroUsize::new(2).unwrap())
            .checked_mul(2.try_into().unwrap())
            .unwrap()
            .into()
    });
    // Collect all chunks in chunk x column form.
    let tables = chunk_stream
        // Limit the number of chunks we have in flight at any given time.
        .try_buffered(max_chunks_in_flight);

    // Fields expected as the output. (removing fields that are only needed for predicate evaluation)
    let schema_fields = if let Some(include_columns) = &include_columns {
        let field_map = fields
            .iter()
            .map(|field| (field.name().as_str(), field))
            .collect::<HashMap<&str, &ArrowField>>();
        include_columns
            .iter()
            .map(|col| field_map[col.as_str()].clone())
            .collect::<Vec<_>>()
    } else {
        fields
    };

    let arrow_schema = arrow_schema::Schema::new(schema_fields);
    let schema: SchemaRef = Arc::new(Schema::try_from(&arrow_schema)?);

    let include_column_indices = include_columns
        .map(|include_columns| {
            include_columns
                .iter()
                .map(|name| schema.get_index(name))
                .collect::<DaftResult<Vec<_>>>()
        })
        .transpose()?;

    let filtered_tables = tables.map_ok(move |table| {
        if let Some(predicate) = &predicate {
            let table = table?;

            let predicate = BoundExpr::try_new(predicate.clone(), &table.schema)?;

            let filtered = table.filter(&[predicate])?;
            if let Some(include_column_indices) = &include_column_indices {
                Ok(filtered.get_columns(include_column_indices))
            } else {
                Ok(filtered)
            }
        } else {
            table
        }
    });
    let mut remaining_rows = limit.map(|limit| limit as i64);
    let collected_tables = filtered_tables
        .try_take_while(|result| {
            match (result, remaining_rows) {
                // Limit has been met, early-terminate.
                (_, Some(rows_left)) if rows_left <= 0 => futures::future::ready(Ok(false)),
                // Limit has not yet been met, update remaining limit slack and continue.
                (Ok(table), Some(rows_left)) => {
                    remaining_rows = Some(rows_left - table.len() as i64);
                    futures::future::ready(Ok(true))
                }
                // (1) No limit, never early-terminate.
                // (2) Encountered error, propagate error to try_collect to allow it to short-circuit.
                (_, None) | (Err(_), _) => futures::future::ready(Ok(true)),
            }
        })
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .collect::<DaftResult<Vec<_>>>()?;
    // Handle empty table case.
    if collected_tables.is_empty() {
        return Ok(RecordBatch::empty(Some(schema)));
    }

    // // TODO(Clark): Don't concatenate all chunks from a file into a single table, since MicroPartition is natively chunked.
    let concated_table = tables_concat(collected_tables)?;
    if let Some(limit) = limit
        && concated_table.len() > limit
    {
        // apply head in case that last chunk went over limit
        concated_table.head(limit)
    } else {
        Ok(concated_table)
    }
}

pub async fn stream_csv_single(
    uri: String,
    convert_options: Option<CsvConvertOptions>,
    parse_options: Option<CsvParseOptions>,
    read_options: Option<CsvReadOptions>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    max_chunks_in_flight: Option<usize>,
) -> DaftResult<impl Stream<Item = DaftResult<RecordBatch>> + Send> {
    let predicate = convert_options
        .as_ref()
        .and_then(|opts| opts.predicate.clone());

    let limit = convert_options.as_ref().and_then(|opts| opts.limit);

    let include_columns = convert_options
        .as_ref()
        .and_then(|opts| opts.include_columns.clone());

    let convert_options_with_predicate_columns = match (convert_options, &predicate) {
        (None, _) => None,
        (co, None) => co,
        (Some(mut co), Some(predicate)) => {
            if let Some(ref mut include_columns) = co.include_columns {
                let required_columns_for_predicate = get_required_columns(predicate);
                for rc in required_columns_for_predicate {
                    if include_columns.iter().all(|c| c.as_str() != rc.as_str()) {
                        include_columns.push(rc);
                    }
                }
            }
            // if we have a limit and a predicate, remove limit for stream
            co.limit = None;
            Some(co)
        }
    };

    let (chunk_stream, _fields) = read_csv_single_into_stream(
        uri,
        convert_options_with_predicate_columns.unwrap_or_default(),
        parse_options.unwrap_or_default(),
        read_options,
        io_client,
        io_stats,
    )
    .await?;
    // Default max chunks in flight is set to 2x the number of cores, which should ensure pipelining of reading chunks
    // with the parsing of chunks on the rayon threadpool.
    let max_chunks_in_flight = max_chunks_in_flight.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .unwrap_or(NonZeroUsize::new(2).unwrap())
            .checked_mul(2.try_into().unwrap())
            .unwrap()
            .into()
    });
    // Collect all chunks in chunk x column form.
    let tables = chunk_stream
        // Limit the number of chunks we have in flight at any given time.
        .try_buffered(max_chunks_in_flight);

    let filtered_tables = tables.map(move |table| {
        let table = table?;
        if let Some(predicate) = &predicate {
            let table = table?;
            let predicate = BoundExpr::try_new(predicate.clone(), &table.schema)?;

            let filtered = table.filter(&[predicate])?;
            if let Some(include_columns) = &include_columns {
                let include_column_indices = include_columns
                    .iter()
                    .map(|name| table.schema.get_index(name))
                    .collect::<DaftResult<Vec<_>>>()?;

                Ok(filtered.get_columns(&include_column_indices))
            } else {
                Ok(filtered)
            }
        } else {
            table
        }
    });

    let mut remaining_rows = limit.map(|limit| limit as i64);
    let tables = filtered_tables.try_take_while(move |table| {
        match remaining_rows {
            // Limit has been met, early-terminate.
            Some(rows_left) if rows_left <= 0 => futures::future::ready(Ok(false)),
            // Limit has not yet been met, update remaining limit slack and continue.
            Some(rows_left) => {
                remaining_rows = Some(rows_left - table.len() as i64);
                futures::future::ready(Ok(true))
            }
            // No limit, never early-terminate.
            None => futures::future::ready(Ok(true)),
        }
    });
    Ok(tables)
}

async fn read_csv_single_into_stream(
    uri: String,
    convert_options: CsvConvertOptions,
    parse_options: CsvParseOptions,
    read_options: Option<CsvReadOptions>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<(impl TableStream + Send, Vec<ArrowField>)> {
    let (mut schema, estimated_mean_row_size, estimated_std_row_size) =
        if let Some(schema) = convert_options.schema {
            (schema.to_arrow()?, None, None)
        } else {
            let (schema, read_stats) = read_csv_schema_single(
                &uri,
                parse_options.clone(),
                // Read at most 1 MiB when doing schema inference.
                Some(1024 * 1024),
                io_client.clone(),
                io_stats.clone(),
            )
            .await?;
            (
                schema.to_arrow()?,
                Some(read_stats.mean_record_size_bytes),
                Some(read_stats.stddev_record_size_bytes),
            )
        };
    // Rename fields, if necessary.
    if let Some(column_names) = convert_options.column_names {
        schema = arrow_schema::Schema::new(
            schema
                .fields()
                .iter()
                .zip(column_names.iter())
                .map(|(field, name)| {
                    ArrowField::new(name, field.data_type().clone(), field.is_nullable())
                        .with_metadata(field.metadata().clone())
                })
                .collect::<Vec<_>>(),
        );
    }
    let (reader, buffer_size, chunk_size): (Box<dyn AsyncBufRead + Unpin + Send>, usize, usize) =
        match io_client
            .single_url_get(uri.clone(), None, io_stats)
            .await?
        {
            GetResult::File(file) => {
                (
                    Box::new(BufReader::new(File::open(file.path).await?)),
                    // Use user-provided buffer size, otherwise falling back to 512 KiB as the default.
                    read_options
                        .as_ref()
                        .and_then(|opt| opt.buffer_size)
                        .unwrap_or(512 * 1024),
                    read_options
                        .as_ref()
                        .and_then(|opt| opt.chunk_size)
                        .unwrap_or(64 * 1024),
                )
            }
            GetResult::Stream(stream, ..) => (
                Box::new(StreamReader::new(stream)),
                read_options
                    .as_ref()
                    .and_then(|opt| opt.buffer_size)
                    .unwrap_or(512 * 1024),
                read_options
                    .as_ref()
                    .and_then(|opt| opt.chunk_size)
                    .unwrap_or(64 * 1024),
            ),
        };
    let reader: Box<dyn AsyncRead + Unpin + Send> = match CompressionCodec::from_uri(&uri) {
        Some(compression) => Box::new(compression.to_decoder(reader)),
        None => reader,
    };
    let reader = AsyncReaderBuilder::new()
        .has_headers(parse_options.has_header)
        .delimiter(parse_options.delimiter)
        .double_quote(parse_options.double_quote)
        .quote(parse_options.quote)
        .escape(parse_options.escape_char)
        .comment(parse_options.comment)
        .buffer_capacity(buffer_size)
        .flexible(parse_options.allow_variable_columns)
        .create_reader(reader.compat());
    let fields: Vec<ArrowField> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    let read_stream = read_into_byterecord_chunk_stream(
        reader,
        fields.len(),
        convert_options.limit,
        chunk_size,
        estimated_mean_row_size,
        estimated_std_row_size,
    );
    let projection_indices =
        fields_to_projection_indices(&fields, &convert_options.include_columns);

    let stream = parse_into_column_array_chunk_stream(
        read_stream,
        Arc::new(fields.clone()),
        projection_indices,
    )?;

    Ok((stream, fields))
}

/// Reads CSV rows from an async reader into the provided byte records buffer.
/// Returns the number of rows read.
async fn async_read_rows<R: futures::AsyncRead + Unpin + Send>(
    reader: &mut csv_async::AsyncReader<R>,
    skip: usize,
    rows: &mut [csv_async::ByteRecord],
) -> Result<usize, csv_async::Error> {
    // Skip the requested number of rows.
    let mut record = csv_async::ByteRecord::new();
    for _ in 0..skip {
        if !reader.read_byte_record(&mut record).await? {
            return Ok(0);
        }
    }
    // Read rows into the buffer.
    let mut row_count = 0;
    for row in rows.iter_mut() {
        if !reader.read_byte_record(row).await? {
            break;
        }
        row_count += 1;
    }
    Ok(row_count)
}

fn read_into_byterecord_chunk_stream<R>(
    mut reader: AsyncReader<Compat<R>>,
    num_fields: usize,
    num_rows: Option<usize>,
    chunk_size: usize,
    estimated_mean_row_size: Option<f64>,
    estimated_std_row_size: Option<f64>,
) -> impl ByteRecordChunkStream
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let num_rows = num_rows.unwrap_or(usize::MAX);
    let mut estimated_mean_row_size = estimated_mean_row_size.unwrap_or(200f64);
    let mut estimated_std_row_size = estimated_std_row_size.unwrap_or(20f64);
    // Stream of unparsed CSV byte record chunks.
    async_stream::try_stream! {
        // Number of rows read in last read.
        let mut rows_read = 1;
        // Total number of rows read across all reads.
        let mut total_rows_read = 0;
        let mut mean = 0f64;
        let mut m2 = 0f64;
        while rows_read > 0 && total_rows_read < num_rows {
            // Allocate a record buffer of size 1 standard above the observed mean record size.
            // If the record sizes are normally distributed, this should result in ~85% of the records not requiring
            // reallocation during reading.
            let record_buffer_size = (estimated_mean_row_size + estimated_std_row_size).ceil() as usize;
            // Process at least 8 rows in a chunk, even if the rows are pretty large.
            // Cap chunk size at the remaining number of rows we need to read before we reach the num_rows limit.
            let chunk_size_rows = chunk_size.max(8).min(num_rows - total_rows_read);
            let mut chunk_buffer = vec![
                csv_async::ByteRecord::with_capacity(record_buffer_size, num_fields);
                chunk_size_rows
            ];

            let byte_pos_before = reader.position().byte();
            rows_read = async_read_rows(&mut reader, 0, chunk_buffer.as_mut_slice()).await.context(super::CSVSnafu {})?;
            let bytes_read = reader.position().byte() - byte_pos_before;

            // Update stats.
            total_rows_read += rows_read;
            let delta = (bytes_read as f64) - mean;
            mean += delta / (total_rows_read as f64);
            let delta2 = (bytes_read as f64) - mean;
            m2 += delta * delta2;
            estimated_mean_row_size = mean;
            estimated_std_row_size = (m2 / ((total_rows_read - 1) as f64)).sqrt();

            chunk_buffer.truncate(rows_read);
            if rows_read > 0 {
                yield chunk_buffer;
            }
        }
    }
}

fn parse_into_column_array_chunk_stream(
    stream: impl ByteRecordChunkStream + Send,
    fields: Arc<Vec<ArrowField>>,
    projection_indices: Arc<Vec<usize>>,
) -> DaftResult<impl TableStream + Send> {
    // Parsing stream: we spawn background tokio + rayon tasks so we can pipeline chunk parsing with chunk reading, and
    // we further parse each chunk column in parallel on the rayon threadpool.

    let fields_subset = projection_indices
        .iter()
        .map(|i| {
            let f = fields.get(*i).unwrap();
            daft_core::datatypes::Field::new(
                f.name(),
                daft_schema::dtype::DataType::try_from(f.data_type()).unwrap(),
            )
        })
        .collect::<Vec<daft_core::datatypes::Field>>();
    let read_schema = Arc::new(daft_core::prelude::Schema::new(fields_subset));
    let read_daft_fields = Arc::new(
        read_schema
            .into_iter()
            .cloned()
            .map(Arc::new)
            .collect::<Vec<_>>(),
    );

    Ok(stream.map_ok(move |record| {
        let (fields, projection_indices) = (fields.clone(), projection_indices.clone());
        let read_schema = read_schema.clone();
        let read_daft_fields = read_daft_fields.clone();
        tokio::spawn(async move {
            let (send, recv) = tokio::sync::oneshot::channel();
            rayon::spawn(move || {
                let result = (move || {
                    let chunk = projection_indices
                        .par_iter()
                        .enumerate()
                        .map(|(i, proj_idx)| {
                            let deserialized_col = deserialize_column(
                                record.as_slice(),
                                *proj_idx,
                                fields[*proj_idx].data_type().clone(),
                                0,
                            );
                            Series::from_arrow(read_daft_fields[i].clone(), deserialized_col?)
                        })
                        .collect::<DaftResult<Vec<Series>>>()?;
                    let num_rows = chunk.first().map_or(0, daft_core::series::Series::len);
                    Ok(RecordBatch::new_unchecked(read_schema, chunk, num_rows))
                })();
                let _ = send.send(result);
            });
            recv.await.context(super::OneShotRecvSnafu {})?
        })
        .context(super::JoinSnafu {})
    }))
}

pub fn fields_to_projection_indices(
    fields: &[ArrowField],
    include_columns: &Option<Vec<String>>,
) -> Arc<Vec<usize>> {
    let field_name_to_idx = fields
        .iter()
        .enumerate()
        .map(|(idx, f)| (f.name().as_str(), idx))
        .collect::<HashMap<&str, usize>>();
    include_columns
        .as_ref()
        .map_or_else(
            || (0..fields.len()).collect(),
            |cols| {
                cols.iter()
                    .map(|c| field_name_to_idx[c.as_str()])
                    .collect::<Vec<_>>()
            },
        )
        .into()
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use arrow_schema::DataType as ArrowDataType;
    use common_error::{DaftError, DaftResult};
    use daft_core::prelude::*;
    use daft_decoding::{deserialize::deserialize_column, inference::infer};
    use daft_io::{IOClient, IOConfig};
    use daft_recordbatch::RecordBatch;
    use rstest::rstest;

    use super::read_csv;
    use crate::{
        CsvConvertOptions, CsvParseOptions, CsvReadOptions, char_to_byte, schema::merge_schema,
    };

    /// Reference implementation that reads a CSV file using the sync `csv` crate
    /// and `daft_decoding` directly, then compares the result against the output
    /// from Daft's async CSV reader. This ensures that Daft's CSV pipeline produces
    /// results consistent with a straightforward arrow-rs-based CSV read.
    #[allow(clippy::too_many_arguments)]
    fn check_equal_local_arrow_rs(
        path: &str,
        out: &RecordBatch,
        has_header: bool,
        delimiter: Option<char>,
        double_quote: bool,
        quote: Option<char>,
        escape_char: Option<char>,
        comment: Option<char>,
        column_names: Option<Vec<&str>>,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) {
        // Build a sync CSV reader with the same parse options.
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(has_header)
            .delimiter(char_to_byte(delimiter).unwrap_or(None).unwrap_or(b','))
            .double_quote(double_quote)
            .quote(char_to_byte(quote).unwrap_or(None).unwrap_or(b'"'))
            .escape(char_to_byte(escape_char).unwrap_or(Some(b'\\')))
            .comment(char_to_byte(comment).unwrap_or(Some(b'#')))
            .from_path(path)
            .unwrap();

        // Read headers.
        let mut first_record: Option<csv::ByteRecord> = None;
        let headers: Vec<String> = if has_header {
            reader
                .headers()
                .unwrap()
                .iter()
                .map(|s| s.to_string())
                .collect()
        } else {
            // Peek at first record to get column count.
            let mut record = csv::ByteRecord::new();
            let has_data = reader.read_byte_record(&mut record).unwrap();
            let count = if has_data { record.len() } else { 0 };
            if has_data {
                first_record = Some(record);
            }
            (0..count).map(|i| format!("column_{}", i + 1)).collect()
        };

        // Read all rows up to limit.
        let max_rows = limit.unwrap_or(usize::MAX);
        let mut rows: Vec<csv::ByteRecord> = Vec::new();
        // Include the first record we peeked at for no-header case.
        if let Some(rec) = first_record {
            rows.push(rec);
        }
        let mut record = csv::ByteRecord::new();
        while rows.len() < max_rows && reader.read_byte_record(&mut record).unwrap() {
            rows.push(record.clone());
        }

        // Infer schema from the rows.
        let num_cols = headers.len();
        let mut column_types: Vec<HashSet<ArrowDataType>> = vec![HashSet::new(); num_cols];
        for row in &rows {
            for (col_idx, types) in column_types.iter_mut().enumerate() {
                if let Some(bytes) = row.get(col_idx) {
                    types.insert(infer(bytes));
                } else {
                    types.insert(ArrowDataType::Null);
                }
            }
        }
        let mut fields = merge_schema(&headers, &mut column_types);

        // Apply column name overrides (for no-header case).
        if !has_header {
            if let Some(ref names) = column_names {
                fields = fields
                    .into_iter()
                    .zip(names.iter())
                    .map(|(field, name)| {
                        arrow_schema::Field::new(*name, field.data_type().clone(), true)
                    })
                    .collect();
            }
        }

        // Determine projection indices.
        let proj_indices: Vec<usize> = if let Some(ref proj) = projection {
            proj.clone()
        } else {
            (0..fields.len()).collect()
        };

        // Deserialize each projected column and convert to Daft Series for comparison
        // (this applies Daft's type coercion).
        let projected_fields: Vec<_> = proj_indices.iter().map(|&i| fields[i].clone()).collect();
        let reference_series: Vec<Series> = proj_indices
            .iter()
            .map(|&col_idx| {
                let arr =
                    deserialize_column(&rows, col_idx, fields[col_idx].data_type().clone(), 0)
                        .unwrap();
                let field = &fields[col_idx];
                let daft_dtype = daft_schema::dtype::DataType::try_from(field.data_type()).unwrap();
                let daft_field = daft_core::datatypes::Field::new(field.name(), daft_dtype);
                Series::from_arrow(daft_field, arr).unwrap()
            })
            .collect();

        // Compare schema.
        let reference_schema = Schema::new(
            projected_fields
                .iter()
                .map(|f| {
                    let daft_dtype = daft_schema::dtype::DataType::try_from(f.data_type()).unwrap();
                    daft_core::datatypes::Field::new(f.name(), daft_dtype)
                })
                .collect::<Vec<_>>(),
        );
        assert_eq!(
            out.schema.as_ref(),
            &reference_schema,
            "Schema mismatch between Daft CSV reader and reference arrow-rs reader"
        );

        // Compare columns.
        assert_eq!(out.num_columns(), reference_series.len());
        for (i, (out_col, ref_col)) in out
            .columns()
            .iter()
            .zip(reference_series.iter())
            .enumerate()
        {
            let out_arrow = out_col.to_arrow().unwrap();
            let ref_arrow = ref_col.to_arrow().unwrap();
            assert_eq!(
                out_arrow.as_ref(),
                ref_arrow.as_ref(),
                "Column {} ({}) mismatch between Daft CSV reader and reference arrow-rs reader",
                i,
                projected_fields[i].name(),
            );
        }
    }

    #[rstest]
    fn test_csv_read_local(
        #[values(
            // Uncompressed
            None,
            // brotli
            Some("br"),
            // bzip2
            Some("bz2"),
            // deflate
            Some("deflate"),
            // gzip
            Some("gz"),
            // lzma
            Some("lzma"),
            // xz
            Some("xz"),
            // zlib
            Some("zl"),
            // zstd
            Some("zst"),
        )]
        compression: Option<&str>,
    ) -> DaftResult<()> {
        let file = format!(
            "{}/test/iris_tiny.csv{}",
            env!("CARGO_MANIFEST_DIR"),
            compression.map_or(String::new(), |ext| format!(".{ext}"))
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(file.as_ref(), None, None, None, io_client, None, true, None)?;
        assert_eq!(table.len(), 20);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("sepal.length", DataType::Float64),
                Field::new("sepal.width", DataType::Float64),
                Field::new("petal.length", DataType::Float64),
                Field::new("petal.width", DataType::Float64),
                Field::new("variety", DataType::Utf8),
            ])
            .into(),
        );
        if compression.is_none() {
            check_equal_local_arrow_rs(
                file.as_ref(),
                &table,
                true,
                None,
                true,
                None,
                None,
                None,
                None,
                None,
                None,
            );
        }

        Ok(())
    }

    #[test]
    fn test_csv_read_local_no_headers() -> DaftResult<()> {
        let file = format!(
            "{}/test/iris_tiny_no_headers.csv",
            env!("CARGO_MANIFEST_DIR"),
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let column_names = vec![
            "sepal.length",
            "sepal.width",
            "petal.length",
            "petal.width",
            "variety",
        ];
        let table = read_csv(
            file.as_ref(),
            Some(CsvConvertOptions::default().with_column_names(Some(
                column_names.iter().map(|s| (*s).to_string()).collect(),
            ))),
            Some(CsvParseOptions::default().with_has_header(false)),
            None,
            io_client,
            None,
            true,
            None,
        )?;
        assert_eq!(table.len(), 20);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("sepal.length", DataType::Float64),
                Field::new("sepal.width", DataType::Float64),
                Field::new("petal.length", DataType::Float64),
                Field::new("petal.width", DataType::Float64),
                Field::new("variety", DataType::Utf8),
            ])
            .into(),
        );
        check_equal_local_arrow_rs(
            file.as_ref(),
            &table,
            false,
            None,
            true,
            None,
            None,
            None,
            Some(column_names),
            None,
            None,
        );

        Ok(())
    }

    #[test]
    fn test_csv_read_local_delimiter() -> DaftResult<()> {
        let file = format!(
            "{}/test/iris_tiny_bar_delimiter.csv",
            env!("CARGO_MANIFEST_DIR"),
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(
            file.as_ref(),
            None,
            Some(CsvParseOptions::default().with_delimiter(b'|')),
            None,
            io_client,
            None,
            true,
            None,
        )?;
        assert_eq!(table.len(), 20);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("sepal.length", DataType::Float64),
                Field::new("sepal.width", DataType::Float64),
                Field::new("petal.length", DataType::Float64),
                Field::new("petal.width", DataType::Float64),
                Field::new("variety", DataType::Utf8),
            ])
            .into(),
        );
        check_equal_local_arrow_rs(
            file.as_ref(),
            &table,
            true,
            Some('|'),
            true,
            None,
            None,
            None,
            None,
            None,
            None,
        );

        Ok(())
    }

    #[test]
    fn test_csv_read_local_double_quote() -> DaftResult<()> {
        let file = format!(
            "{}/test/iris_tiny_double_quote.csv",
            env!("CARGO_MANIFEST_DIR"),
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(
            file.as_ref(),
            None,
            Some(CsvParseOptions::default().with_double_quote(false)),
            None,
            io_client,
            None,
            true,
            None,
        )?;
        assert_eq!(table.len(), 19);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("\"sepal.\"\"length\"", DataType::Float64),
                Field::new("sepal.width", DataType::Float64),
                Field::new("petal.length", DataType::Float64),
                Field::new("petal.width", DataType::Float64),
                Field::new("variety", DataType::Utf8),
            ])
            .into(),
        );
        check_equal_local_arrow_rs(
            file.as_ref(),
            &table,
            true,
            None,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
        );

        Ok(())
    }
    #[test]
    fn test_csv_read_local_quote() -> DaftResult<()> {
        let file = format!(
            "{}/test/iris_tiny_single_quote.csv",
            env!("CARGO_MANIFEST_DIR"),
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(
            file.as_ref(),
            None,
            Some(CsvParseOptions::default().with_quote(b'\'')),
            None,
            io_client,
            None,
            true,
            None,
        )?;
        assert_eq!(table.len(), 20);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("sepal.length", DataType::Float64),
                Field::new("sepal.width", DataType::Float64),
                Field::new("petal.length", DataType::Float64),
                Field::new("petal.width", DataType::Float64),
                Field::new("variety", DataType::Utf8),
            ])
            .into(),
        );
        check_equal_local_arrow_rs(
            file.as_ref(),
            &table,
            true,
            None,
            true,
            Some('\''),
            None,
            None,
            None,
            None,
            None,
        );

        Ok(())
    }

    #[test]
    fn test_csv_read_local_escape() -> DaftResult<()> {
        let file = format!("{}/test/iris_tiny_escape.csv", env!("CARGO_MANIFEST_DIR"),);

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(
            file.as_ref(),
            None,
            Some(CsvParseOptions::default().with_escape_char(Some(b'\\'))),
            None,
            io_client,
            None,
            true,
            None,
        )?;
        assert_eq!(table.len(), 20);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("sepal.\"length\"", DataType::Float64),
                Field::new("sepal.width", DataType::Float64),
                Field::new("petal.length", DataType::Float64),
                Field::new("petal.width", DataType::Float64),
                Field::new("variety", DataType::Utf8),
            ])
            .into(),
        );
        check_equal_local_arrow_rs(
            file.as_ref(),
            &table,
            true,
            None,
            true,
            None,
            Some('\\'),
            None,
            None,
            None,
            None,
        );

        Ok(())
    }

    #[test]
    fn test_csv_read_local_comment() -> DaftResult<()> {
        let file = format!("{}/test/iris_tiny_comment.csv", env!("CARGO_MANIFEST_DIR"),);

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(
            file.as_ref(),
            None,
            Some(CsvParseOptions::default().with_comment(Some(b'#'))),
            None,
            io_client,
            None,
            true,
            None,
        )?;
        assert_eq!(table.len(), 19);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("sepal.length", DataType::Float64),
                Field::new("sepal.width", DataType::Float64),
                Field::new("petal.length", DataType::Float64),
                Field::new("petal.width", DataType::Float64),
                Field::new("variety", DataType::Utf8),
            ])
            .into(),
        );
        check_equal_local_arrow_rs(
            file.as_ref(),
            &table,
            true,
            None,
            true,
            None,
            None,
            Some('#'),
            None,
            None,
            None,
        );

        Ok(())
    }
    #[test]
    fn test_csv_read_local_limit() -> DaftResult<()> {
        let file = format!("{}/test/iris_tiny.csv", env!("CARGO_MANIFEST_DIR"),);

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(
            file.as_ref(),
            Some(CsvConvertOptions::default().with_limit(Some(5))),
            None,
            None,
            io_client,
            None,
            true,
            None,
        )?;
        assert_eq!(table.len(), 5);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("sepal.length", DataType::Float64),
                Field::new("sepal.width", DataType::Float64),
                Field::new("petal.length", DataType::Float64),
                Field::new("petal.width", DataType::Float64),
                Field::new("variety", DataType::Utf8),
            ])
            .into(),
        );
        check_equal_local_arrow_rs(
            file.as_ref(),
            &table,
            true,
            None,
            true,
            None,
            None,
            None,
            None,
            None,
            Some(5),
        );

        Ok(())
    }

    #[test]
    fn test_csv_read_local_projection() -> DaftResult<()> {
        let file = format!("{}/test/iris_tiny.csv", env!("CARGO_MANIFEST_DIR"),);

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(
            file.as_ref(),
            Some(CsvConvertOptions::default().with_include_columns(Some(vec![
                "petal.length".to_string(),
                "petal.width".to_string(),
            ]))),
            None,
            None,
            io_client,
            None,
            true,
            None,
        )?;
        assert_eq!(table.len(), 20);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("petal.length", DataType::Float64),
                Field::new("petal.width", DataType::Float64),
            ])
            .into(),
        );
        check_equal_local_arrow_rs(
            file.as_ref(),
            &table,
            true,
            None,
            true,
            None,
            None,
            None,
            None,
            Some(vec![2, 3]),
            None,
        );

        Ok(())
    }

    #[test]
    fn test_csv_read_local_no_headers_and_projection() -> DaftResult<()> {
        let file = format!(
            "{}/test/iris_tiny_no_headers.csv",
            env!("CARGO_MANIFEST_DIR"),
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let column_names = vec![
            "sepal.length",
            "sepal.width",
            "petal.length",
            "petal.width",
            "variety",
        ];
        let table = read_csv(
            file.as_ref(),
            Some(
                CsvConvertOptions::default()
                    .with_column_names(Some(
                        column_names.iter().map(|s| (*s).to_string()).collect(),
                    ))
                    .with_include_columns(Some(vec![
                        "petal.length".to_string(),
                        "petal.width".to_string(),
                    ])),
            ),
            Some(CsvParseOptions::default().with_has_header(false)),
            None,
            io_client,
            None,
            true,
            None,
        )?;
        assert_eq!(table.len(), 20);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("petal.length", DataType::Float64),
                Field::new("petal.width", DataType::Float64),
            ])
            .into(),
        );
        check_equal_local_arrow_rs(
            file.as_ref(),
            &table,
            false,
            None,
            true,
            None,
            None,
            None,
            Some(column_names),
            Some(vec![2, 3]),
            None,
        );

        Ok(())
    }

    #[test]
    fn test_csv_read_local_larger_than_buffer_size() -> DaftResult<()> {
        let file = format!("{}/test/iris_tiny.csv", env!("CARGO_MANIFEST_DIR"),);

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(
            file.as_ref(),
            None,
            None,
            Some(CsvReadOptions::default().with_buffer_size(Some(128))),
            io_client,
            None,
            true,
            None,
        )?;
        assert_eq!(table.len(), 20);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("sepal.length", DataType::Float64),
                Field::new("sepal.width", DataType::Float64),
                Field::new("petal.length", DataType::Float64),
                Field::new("petal.width", DataType::Float64),
                Field::new("variety", DataType::Utf8),
            ])
            .into(),
        );
        check_equal_local_arrow_rs(
            file.as_ref(),
            &table,
            true,
            None,
            true,
            None,
            None,
            None,
            None,
            None,
            None,
        );

        Ok(())
    }

    #[test]
    fn test_csv_read_local_larger_than_chunk_size() -> DaftResult<()> {
        let file = format!("{}/test/iris_tiny.csv", env!("CARGO_MANIFEST_DIR"),);

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(
            file.as_ref(),
            None,
            None,
            Some(CsvReadOptions::default().with_chunk_size(Some(2))),
            io_client,
            None,
            true,
            None,
        )?;
        assert_eq!(table.len(), 20);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("sepal.length", DataType::Float64),
                Field::new("sepal.width", DataType::Float64),
                Field::new("petal.length", DataType::Float64),
                Field::new("petal.width", DataType::Float64),
                Field::new("variety", DataType::Utf8),
            ])
            .into(),
        );
        check_equal_local_arrow_rs(
            file.as_ref(),
            &table,
            true,
            None,
            true,
            None,
            None,
            None,
            None,
            None,
            None,
        );

        Ok(())
    }

    #[test]
    fn test_csv_read_local_throttled_streaming() -> DaftResult<()> {
        let file = format!("{}/test/iris_tiny.csv", env!("CARGO_MANIFEST_DIR"),);

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(
            file.as_ref(),
            None,
            None,
            Some(CsvReadOptions::default().with_chunk_size(Some(5))),
            io_client,
            None,
            true,
            Some(2),
        )?;
        assert_eq!(table.len(), 20);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("sepal.length", DataType::Float64),
                Field::new("sepal.width", DataType::Float64),
                Field::new("petal.length", DataType::Float64),
                Field::new("petal.width", DataType::Float64),
                Field::new("variety", DataType::Utf8),
            ])
            .into(),
        );

        Ok(())
    }

    #[test]
    fn test_csv_read_local_nulls() -> DaftResult<()> {
        let file = format!("{}/test/iris_tiny_nulls.csv", env!("CARGO_MANIFEST_DIR"),);

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(file.as_ref(), None, None, None, io_client, None, true, None)?;
        assert_eq!(table.len(), 6);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("sepal.length", DataType::Float64),
                Field::new("sepal.width", DataType::Float64),
                Field::new("petal.length", DataType::Float64),
                Field::new("petal.width", DataType::Float64),
                Field::new("variety", DataType::Utf8),
            ])
            .into(),
        );

        Ok(())
    }

    #[test]
    fn test_csv_read_local_all_null_column() -> DaftResult<()> {
        let file = format!(
            "{}/test/iris_tiny_all_null_column.csv",
            env!("CARGO_MANIFEST_DIR"),
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(file.as_ref(), None, None, None, io_client, None, true, None)?;
        assert_eq!(table.len(), 6);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("sepal.length", DataType::Float64),
                Field::new("sepal.width", DataType::Float64),
                // All null column parsed as null dtype.
                Field::new("petal.length", DataType::Null),
                Field::new("petal.width", DataType::Float64),
                Field::new("variety", DataType::Utf8),
            ])
            .into(),
        );
        let null_column = table.get_column(2);
        assert_eq!(null_column.data_type(), &DataType::Null);
        assert_eq!(null_column.len(), 6);
        assert_eq!(
            null_column.null().unwrap(),
            &NullArray::full_null("petal.length", &DataType::Null, 6)
        );

        Ok(())
    }

    #[test]
    fn test_csv_read_local_all_null_column_with_schema() -> DaftResult<()> {
        let file = format!(
            "{}/test/iris_tiny_all_null_column.csv",
            env!("CARGO_MANIFEST_DIR"),
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);
        let schema = Schema::new(vec![
            Field::new("sepal.length", DataType::Float64),
            Field::new("sepal.width", DataType::Float64),
            Field::new("petal.length", DataType::Null),
            Field::new("petal.width", DataType::Float64),
            Field::new("variety", DataType::Utf8),
        ]);

        let table = read_csv(
            file.as_ref(),
            Some(CsvConvertOptions::default().with_schema(Some(schema.into()))),
            None,
            None,
            io_client,
            None,
            true,
            None,
        )?;
        assert_eq!(table.len(), 6);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("sepal.length", DataType::Float64),
                Field::new("sepal.width", DataType::Float64),
                // All null column parsed as null dtype.
                Field::new("petal.length", DataType::Null),
                Field::new("petal.width", DataType::Float64),
                Field::new("variety", DataType::Utf8),
            ])
            .into(),
        );
        let null_column = table.get_column(2);
        assert_eq!(null_column.data_type(), &DataType::Null);
        assert_eq!(null_column.len(), 6);
        assert_eq!(
            null_column.null().unwrap(),
            &NullArray::full_null("petal.length", &DataType::Null, 6)
        );
        Ok(())
    }

    #[test]
    fn test_csv_read_local_empty_lines_dropped() -> DaftResult<()> {
        let file = format!(
            "{}/test/iris_tiny_empty_lines.csv",
            env!("CARGO_MANIFEST_DIR"),
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(file.as_ref(), None, None, None, io_client, None, true, None)?;
        assert_eq!(table.len(), 3);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("sepal.length", DataType::Float64),
                Field::new("sepal.width", DataType::Float64),
                Field::new("petal.length", DataType::Float64),
                Field::new("petal.width", DataType::Float64),
                Field::new("variety", DataType::Utf8),
            ])
            .into(),
        );

        Ok(())
    }

    #[test]
    fn test_csv_read_local_wrong_type_yields_nulls() -> DaftResult<()> {
        let file = format!("{}/test/iris_tiny.csv", env!("CARGO_MANIFEST_DIR"),);

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let schema = Schema::new(vec![
            // Conversion to all of these types should fail, resulting in nulls.
            Field::new("sepal.length", DataType::Boolean),
            Field::new("sepal.width", DataType::Boolean),
            Field::new("petal.length", DataType::Boolean),
            Field::new("petal.width", DataType::Boolean),
            Field::new("variety", DataType::Int64),
        ]);
        let table = read_csv(
            file.as_ref(),
            Some(CsvConvertOptions::default().with_schema(Some(schema.into()))),
            None,
            None,
            io_client,
            None,
            true,
            None,
        )?;
        let num_rows = table.len();
        assert_eq!(num_rows, 20);
        // Check that all columns are all null.
        for idx in 0..table.num_columns() {
            let column = table.get_column(idx);
            assert_eq!(column.null_count(), num_rows);
        }

        Ok(())
    }

    #[test]
    fn test_csv_read_local_invalid_cols_header_mismatch() -> DaftResult<()> {
        let file = format!(
            "{}/test/iris_tiny_invalid_header_cols_mismatch.csv",
            env!("CARGO_MANIFEST_DIR"),
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let err = read_csv(file.as_ref(), None, None, None, io_client, None, true, None);
        assert!(err.is_err());
        let err = err.unwrap_err();
        assert!(matches!(err, DaftError::External(_)), "{}", err);
        assert!(
            err.to_string()
                .contains("found record with 4 fields, but the previous record has 5 fields"),
            "{}",
            err
        );

        Ok(())
    }

    #[test]
    fn test_csv_read_local_invalid_cols_header_mismatch_allow_variable_columns() -> DaftResult<()> {
        let file = format!(
            "{}/test/iris_tiny_invalid_header_cols_mismatch.csv", // 5 cols in header with 4 cols in data
            env!("CARGO_MANIFEST_DIR"),
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(
            file.as_ref(),
            None,
            Some(CsvParseOptions::default().with_variable_columns(true)),
            None,
            io_client,
            None,
            true,
            None,
        )?;

        assert_eq!(table.len(), 3);

        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("sepal.length", DataType::Float64),
                Field::new("sepal.width", DataType::Float64),
                Field::new("petal.length", DataType::Float64),
                Field::new("petal.width", DataType::Float64),
                Field::new("variety", DataType::Utf8),
            ])
            .into(),
        );

        // First 4 cols should have no nulls
        assert_eq!(table.get_column(0).null_count(), 0);
        assert_eq!(table.get_column(1).null_count(), 0);
        assert_eq!(table.get_column(2).null_count(), 0);
        assert_eq!(table.get_column(3).null_count(), 0);

        // Last col should have 3 nulls because of the missing data
        assert_eq!(table.get_column(4).null_count(), 3);

        Ok(())
    }

    #[test]
    fn test_csv_read_local_invalid_no_header_variable_num_cols() -> DaftResult<()> {
        let file = format!(
            "{}/test/iris_tiny_invalid_no_header_variable_num_cols.csv",
            env!("CARGO_MANIFEST_DIR"),
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let err = read_csv(
            file.as_ref(),
            None,
            Some(CsvParseOptions::default().with_has_header(false)),
            None,
            io_client,
            None,
            true,
            None,
        );
        assert!(err.is_err());
        let err = err.unwrap_err();
        assert!(matches!(err, DaftError::External(_)), "{}", err);
        assert!(
            err.to_string()
                .contains("found record with 5 fields, but the previous record has 4 fields"),
            "{}",
            err
        );

        Ok(())
    }

    #[test]
    fn test_csv_read_local_invalid_no_header_allow_variable_cols() -> DaftResult<()> {
        let file = format!(
            "{}/test/iris_tiny_invalid_no_header_variable_num_cols.csv", // first and third row have 4 cols, second row has 5 cols
            env!("CARGO_MANIFEST_DIR"),
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(
            file.as_ref(),
            None,
            Some(
                CsvParseOptions::default()
                    .with_has_header(false)
                    .with_variable_columns(true),
            ),
            None,
            io_client,
            None,
            true,
            None,
        )?;

        assert_eq!(table.len(), 3);

        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("column_1", DataType::Float64),
                Field::new("column_2", DataType::Float64),
                Field::new("column_3", DataType::Float64),
                Field::new("column_4", DataType::Float64),
            ])
            .into(),
        );

        Ok(())
    }

    #[test]
    fn test_csv_read_local_invalid_no_header_allow_variable_cols_with_schema() -> DaftResult<()> {
        let file = format!(
            "{}/test/iris_tiny_invalid_no_header_variable_num_cols.csv", // first and third row have 4 cols, second row has 5 cols
            env!("CARGO_MANIFEST_DIR"),
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let schema = Schema::new(vec![
            Field::new("sepal.length", DataType::Float64),
            Field::new("sepal.width", DataType::Float64),
            Field::new("petal.length", DataType::Float64),
            Field::new("petal.width", DataType::Float64),
            Field::new("variety", DataType::Utf8),
        ]);

        let table = read_csv(
            file.as_ref(),
            Some(CsvConvertOptions::default().with_schema(Some(schema.into()))),
            Some(
                CsvParseOptions::default()
                    .with_has_header(false)
                    .with_variable_columns(true),
            ),
            None,
            io_client,
            None,
            true,
            None,
        )?;

        assert_eq!(table.len(), 3);

        assert_eq!(
            table.get_column(4).utf8().unwrap(),
            &Utf8Array::from_iter("variety", vec![None, Some("Seratosa"), None,])
        );

        Ok(())
    }

    #[rstest]
    fn test_csv_read_s3_compression(
        #[values(
            // Uncompressed
            None,
            // brotli
            Some("br"),
            // bzip2
            Some("bz2"),
            // deflate
            Some("deflate"),
            // gzip
            Some("gz"),
            // lzma
            Some("lzma"),
            // xz
            Some("xz"),
            // zlib
            Some("zl"),
            // zstd
            Some("zst"),
        )]
        compression: Option<&str>,
    ) -> DaftResult<()> {
        let file = format!(
            "s3://daft-public-data/test_fixtures/csv-dev/mvp.csv{}",
            compression.map_or(String::new(), |ext| format!(".{ext}"))
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(file.as_ref(), None, None, None, io_client, None, true, None)?;
        assert_eq!(table.len(), 100);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("a", DataType::Int64),
                Field::new("b", DataType::Utf8)
            ])
            .into(),
        );

        Ok(())
    }

    #[test]
    fn test_csv_read_s3_no_headers() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/csv-dev/mvp_no_header.csv";

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let column_names = ["a", "b"];
        let table = read_csv(
            file,
            Some(CsvConvertOptions::default().with_column_names(Some(
                column_names.iter().map(|s| (*s).to_string()).collect(),
            ))),
            Some(CsvParseOptions::default().with_has_header(false)),
            None,
            io_client,
            None,
            true,
            None,
        )?;
        assert_eq!(table.len(), 100);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("a", DataType::Int64),
                Field::new("b", DataType::Utf8)
            ])
            .into(),
        );

        Ok(())
    }

    #[test]
    fn test_csv_read_s3_no_headers_and_projection() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/csv-dev/mvp_no_header.csv";

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let column_names = ["a", "b"];
        let table = read_csv(
            file,
            Some(
                CsvConvertOptions::default()
                    .with_column_names(Some(
                        column_names.iter().map(|s| (*s).to_string()).collect(),
                    ))
                    .with_include_columns(Some(vec!["b".to_string()])),
            ),
            Some(CsvParseOptions::default().with_has_header(false)),
            None,
            io_client,
            None,
            true,
            None,
        )?;
        assert_eq!(table.len(), 100);
        assert_eq!(
            table.schema,
            Schema::new(vec![Field::new("b", DataType::Utf8)]).into(),
        );

        Ok(())
    }

    #[test]
    fn test_csv_read_s3_limit() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/csv-dev/mvp.csv";

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(
            file,
            Some(CsvConvertOptions::default().with_limit(Some(10))),
            None,
            None,
            io_client,
            None,
            true,
            None,
        )?;
        assert_eq!(table.len(), 10);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("a", DataType::Int64),
                Field::new("b", DataType::Utf8)
            ])
            .into(),
        );

        Ok(())
    }

    #[test]
    fn test_csv_read_s3_projection() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/csv-dev/mvp.csv";

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(
            file,
            Some(CsvConvertOptions::default().with_include_columns(Some(vec!["b".to_string()]))),
            None,
            None,
            io_client,
            None,
            true,
            None,
        )?;
        assert_eq!(table.len(), 100);
        assert_eq!(
            table.schema,
            Schema::new(vec![Field::new("b", DataType::Utf8)]).into(),
        );

        Ok(())
    }

    #[test]
    fn test_csv_read_s3_larger_than_buffer_size() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/csv-dev/medium.csv";

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(
            file,
            None,
            None,
            Some(CsvReadOptions::default().with_buffer_size(Some(100))),
            io_client,
            None,
            true,
            None,
        )?;
        assert_eq!(table.len(), 5000);

        Ok(())
    }

    #[test]
    fn test_csv_read_s3_larger_than_chunk_size() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/csv-dev/medium.csv";

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(
            file,
            None,
            None,
            Some(CsvReadOptions::default().with_chunk_size(Some(100))),
            io_client,
            None,
            true,
            None,
        )?;
        assert_eq!(table.len(), 5000);

        Ok(())
    }

    #[test]
    fn test_csv_read_s3_throttled_streaming() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/csv-dev/medium.csv";

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(file, None, None, None, io_client, None, true, Some(5))?;
        assert_eq!(table.len(), 5000);

        Ok(())
    }
}
