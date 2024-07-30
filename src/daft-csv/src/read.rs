use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};

use arrow2::{
    datatypes::Field,
    io::csv::read_async::{read_rows, AsyncReaderBuilder, ByteRecord},
};
use async_compat::{Compat, CompatExt};
use common_error::{DaftError, DaftResult};
use csv_async::AsyncReader;
use daft_core::{schema::Schema, utils::arrow::cast_array_for_daft_if_needed, Series};
use daft_dsl::optimization::get_required_columns;
use daft_io::{get_runtime, GetResult, IOClient, IOStatsRef};
use daft_table::Table;
use futures::{stream::BoxStream, Stream, StreamExt, TryStreamExt};
use rayon::{
    iter::{IndexedParallelIterator, IntoParallelIterator},
    prelude::{IntoParallelRefIterator, ParallelIterator},
};
use snafu::{
    futures::{try_future::Context, TryFutureExt},
    ResultExt,
};
use tokio::{
    fs::File,
    io::{AsyncBufRead, AsyncRead, BufReader},
    task::JoinHandle,
};
use tokio_util::io::StreamReader;

use crate::ArrowSnafu;
use crate::{metadata::read_csv_schema_single, CsvConvertOptions, CsvParseOptions, CsvReadOptions};
use daft_compression::CompressionCodec;
use daft_decoding::deserialize::deserialize_column;

trait ByteRecordChunkStream: Stream<Item = super::Result<Vec<ByteRecord>>> {}
impl<S> ByteRecordChunkStream for S where S: Stream<Item = super::Result<Vec<ByteRecord>>> {}

type TableChunkResult =
    super::Result<Context<JoinHandle<DaftResult<Table>>, super::JoinSnafu, super::Error>>;
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
) -> DaftResult<Table> {
    let runtime_handle = get_runtime(multithreaded_io)?;
    let _rt_guard = runtime_handle.enter();
    runtime_handle.block_on(async {
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
) -> DaftResult<Vec<Table>> {
    let runtime_handle = get_runtime(multithreaded_io)?;
    let _rt_guard = runtime_handle.enter();
    let tables = runtime_handle.block_on(async move {
        // Launch a read task per URI, throttling the number of concurrent file reads to num_parallel tasks.
        let task_stream = futures::stream::iter(uris.iter().map(|uri| {
            let (uri, convert_options, parse_options, read_options, io_client, io_stats) = (
                uri.to_string(),
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
) -> DaftResult<BoxStream<'static, DaftResult<Vec<Table>>>> {
    let stream = stream_csv_single(
        &uri,
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

// Parallel version of table concat
// get rid of this once Table APIs are parallel
fn tables_concat(mut tables: Vec<Table>) -> DaftResult<Table> {
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
            let series_to_cat: Vec<&Series> = tables
                .iter()
                .map(|s| s.as_ref().get_column_by_index(i).unwrap())
                .collect();
            Series::concat(series_to_cat.as_slice())
        })
        .collect::<DaftResult<Vec<_>>>()?;
    Table::new_with_size(
        first_table.schema.clone(),
        new_series,
        tables.iter().map(|t| t.len()).sum(),
    )
}

async fn read_csv_single_into_table(
    uri: &str,
    convert_options: Option<CsvConvertOptions>,
    parse_options: Option<CsvParseOptions>,
    read_options: Option<CsvReadOptions>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    max_chunks_in_flight: Option<usize>,
) -> DaftResult<Table> {
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
                        include_columns.push(rc)
                    }
                }
            }
            // if we have a limit and a predicate, remove limit for stream
            co.limit = None;
            Some(co)
        }
    };

    let (chunk_stream, fields) = read_csv_single_into_stream(
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

    // Fields expected as the output. (removing fields that are only needed for predicate evaluation)
    let schema_fields = if let Some(include_columns) = &include_columns {
        let field_map = fields
            .iter()
            .map(|field| (field.name.as_str(), field))
            .collect::<HashMap<&str, &Field>>();
        include_columns
            .iter()
            .map(|col| field_map[col.as_str()].clone())
            .collect::<Vec<_>>()
    } else {
        fields
    };

    let schema: arrow2::datatypes::Schema = schema_fields.into();
    let schema = Arc::new(Schema::try_from(&schema)?);

    let filtered_tables = tables.map_ok(move |table| {
        if let Some(predicate) = &predicate {
            let filtered = table?.filter(&[predicate.clone()])?;
            if let Some(include_columns) = &include_columns {
                filtered.get_columns(include_columns.as_slice())
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
        return Table::empty(Some(schema));
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

async fn stream_csv_single(
    uri: &str,
    convert_options: Option<CsvConvertOptions>,
    parse_options: Option<CsvParseOptions>,
    read_options: Option<CsvReadOptions>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    max_chunks_in_flight: Option<usize>,
) -> DaftResult<impl Stream<Item = DaftResult<Vec<Table>>> + Send> {
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
                        include_columns.push(rc)
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

    let filtered_tables = tables.map_ok(move |table| {
        if let Some(predicate) = &predicate {
            let filtered = table?.filter(&[predicate.clone()])?;
            if let Some(include_columns) = &include_columns {
                filtered.get_columns(include_columns.as_slice())
            } else {
                Ok(filtered)
            }
        } else {
            table
        }
    });

    let mut remaining_rows = limit.map(|limit| limit as i64);
    let tables = filtered_tables
        .try_take_while(move |result| {
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
        .map(|r| match r {
            Ok(table) => table,
            Err(e) => Err(e.into()),
        })
        // Chunk the tables into chunks of size max_chunks_in_flight.
        .try_ready_chunks(max_chunks_in_flight)
        .map_err(|e| DaftError::ComputeError(e.to_string()));
    Ok(tables)
}

async fn read_csv_single_into_stream(
    uri: &str,
    convert_options: CsvConvertOptions,
    parse_options: CsvParseOptions,
    read_options: Option<CsvReadOptions>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<(impl TableStream + Send, Vec<Field>)> {
    let (mut schema, estimated_mean_row_size, estimated_std_row_size) = match convert_options.schema
    {
        Some(schema) => (schema.to_arrow()?, None, None),
        None => {
            let (schema, read_stats) = read_csv_schema_single(
                uri,
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
        }
    };
    // Rename fields, if necessary.
    if let Some(column_names) = convert_options.column_names {
        schema = schema
            .fields
            .into_iter()
            .zip(column_names.iter())
            .map(|(field, name)| {
                Field::new(name, field.data_type, field.is_nullable).with_metadata(field.metadata)
            })
            .collect::<Vec<_>>()
            .into();
    }
    let (reader, buffer_size, chunk_size): (Box<dyn AsyncBufRead + Unpin + Send>, usize, usize) =
        match io_client
            .single_url_get(uri.to_string(), None, io_stats)
            .await?
        {
            GetResult::File(file) => {
                (
                    Box::new(BufReader::new(File::open(file.path).await?)),
                    // Use user-provided buffer size, falling back to 8 * the user-provided chunk size if that exists, otherwise falling back to 512 KiB as the default.
                    read_options
                        .as_ref()
                        .and_then(|opt| opt.buffer_size.or_else(|| opt.chunk_size.map(|cs| 8 * cs)))
                        .unwrap_or(512 * 1024),
                    read_options
                        .as_ref()
                        .and_then(|opt| opt.chunk_size.or_else(|| opt.buffer_size.map(|bs| bs / 8)))
                        .unwrap_or(64 * 1024),
                )
            }
            GetResult::Stream(stream, ..) => (
                Box::new(StreamReader::new(stream)),
                read_options
                    .as_ref()
                    .and_then(|opt| opt.buffer_size.or_else(|| opt.chunk_size.map(|cs| 8 * cs)))
                    .unwrap_or(512 * 1024),
                read_options
                    .as_ref()
                    .and_then(|opt| opt.chunk_size.or_else(|| opt.buffer_size.map(|bs| bs / 8)))
                    .unwrap_or(64 * 1024),
            ),
        };
    let reader: Box<dyn AsyncRead + Unpin + Send> = match CompressionCodec::from_uri(uri) {
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
    let read_stream = read_into_byterecord_chunk_stream(
        reader,
        schema.fields.len(),
        convert_options.limit,
        chunk_size,
        estimated_mean_row_size,
        estimated_std_row_size,
    );
    let projection_indices =
        fields_to_projection_indices(&schema.fields, &convert_options.include_columns);

    let fields = schema.fields;
    let stream = parse_into_column_array_chunk_stream(
        read_stream,
        Arc::new(fields.clone()),
        projection_indices,
    )?;

    Ok((stream, fields))
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
            // Get chunk size in # of rows, using the estimated mean row size in bytes.
            let chunk_size_rows = {
                let estimated_rows_per_desired_chunk = chunk_size / (estimated_mean_row_size.ceil() as usize);
                // Process at least 8 rows in a chunk, even if the rows are pretty large.
                // Cap chunk size at the remaining number of rows we need to read before we reach the num_rows limit.
                estimated_rows_per_desired_chunk.max(8).min(num_rows - total_rows_read)
            };
            let mut chunk_buffer = vec![
                ByteRecord::with_capacity(record_buffer_size, num_fields);
                chunk_size_rows
            ];

            let byte_pos_before = reader.position().byte();
            rows_read = read_rows(&mut reader, 0, chunk_buffer.as_mut_slice()).await.context(ArrowSnafu {})?;
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
                yield chunk_buffer
            }
        }
    }
}

fn parse_into_column_array_chunk_stream(
    stream: impl ByteRecordChunkStream + Send,
    fields: Arc<Vec<arrow2::datatypes::Field>>,
    projection_indices: Arc<Vec<usize>>,
) -> DaftResult<impl TableStream + Send> {
    // Parsing stream: we spawn background tokio + rayon tasks so we can pipeline chunk parsing with chunk reading, and
    // we further parse each chunk column in parallel on the rayon threadpool.

    let fields_subset = projection_indices
        .iter()
        .map(|i| fields.get(*i).unwrap().into())
        .collect::<Vec<daft_core::datatypes::Field>>();
    let read_schema = Arc::new(daft_core::schema::Schema::new(fields_subset)?);
    let read_daft_fields = Arc::new(
        read_schema
            .fields
            .values()
            .map(|f| Arc::new(f.clone()))
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
                            Series::try_from_field_and_arrow_array(
                                read_daft_fields[i].clone(),
                                cast_array_for_daft_if_needed(deserialized_col?),
                            )
                        })
                        .collect::<DaftResult<Vec<Series>>>()?;
                    let num_rows = chunk.first().map(|s| s.len()).unwrap_or(0);
                    Ok(Table::new_unchecked(read_schema, chunk, num_rows))
                })();
                let _ = send.send(result);
            });
            recv.await.context(super::OneShotRecvSnafu {})?
        })
        .context(super::JoinSnafu {})
    }))
}

fn fields_to_projection_indices(
    fields: &[arrow2::datatypes::Field],
    include_columns: &Option<Vec<String>>,
) -> Arc<Vec<usize>> {
    let field_name_to_idx = fields
        .iter()
        .enumerate()
        .map(|(idx, f)| (f.name.as_ref(), idx))
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
    use std::sync::Arc;

    use common_error::{DaftError, DaftResult};

    use arrow2::io::csv::read::{
        deserialize_batch, deserialize_column, infer, infer_schema, read_rows, ByteRecord,
        ReaderBuilder,
    };
    use daft_core::{
        datatypes::Field,
        schema::Schema,
        utils::arrow::{cast_array_for_daft_if_needed, cast_array_from_daft_if_needed},
        DataType,
    };
    use daft_io::{IOClient, IOConfig};
    use daft_table::Table;
    use rstest::rstest;

    use crate::{char_to_byte, CsvConvertOptions, CsvParseOptions, CsvReadOptions};

    use super::read_csv;

    #[allow(clippy::too_many_arguments)]
    fn check_equal_local_arrow2(
        path: &str,
        out: &Table,
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
        let mut reader = ReaderBuilder::new()
            .delimiter(char_to_byte(delimiter).unwrap_or(None).unwrap_or(b','))
            .double_quote(double_quote)
            .quote(char_to_byte(quote).unwrap_or(None).unwrap_or(b'"'))
            .escape(char_to_byte(escape_char).unwrap_or(Some(b'\\')))
            .comment(char_to_byte(comment).unwrap_or(Some(b'#')))
            .from_path(path)
            .unwrap();
        let (mut fields, _) = infer_schema(&mut reader, None, has_header, &infer).unwrap();
        if !has_header && let Some(column_names) = column_names {
            fields = fields
                .into_iter()
                .zip(column_names)
                .map(|(field, name)| {
                    arrow2::datatypes::Field::new(name, field.data_type, true)
                        .with_metadata(field.metadata)
                })
                .collect::<Vec<_>>();
        }
        let mut rows = vec![ByteRecord::default(); limit.unwrap_or(100)];
        let rows_read = read_rows(&mut reader, 0, &mut rows).unwrap();
        let rows = &rows[..rows_read];
        let chunk =
            deserialize_batch(rows, &fields, projection.as_deref(), 0, deserialize_column).unwrap();
        if let Some(projection) = projection {
            fields = projection
                .into_iter()
                .map(|idx| fields[idx].clone())
                .collect();
        }
        let columns = chunk
            .into_arrays()
            .into_iter()
            // Roundtrip with Daft for casting.
            .map(|c| cast_array_from_daft_if_needed(cast_array_for_daft_if_needed(c)))
            .collect::<Vec<_>>();
        let schema: arrow2::datatypes::Schema = fields.into();
        // Roundtrip with Daft for casting.
        let schema = Schema::try_from(&schema).unwrap().to_arrow().unwrap();
        assert_eq!(out.schema.to_arrow().unwrap(), schema);
        let out_columns = (0..out.num_columns())
            .map(|i| out.get_column_by_index(i).unwrap().to_arrow())
            .collect::<Vec<_>>();
        assert_eq!(out_columns, columns);
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
            compression.map_or("".to_string(), |ext| format!(".{}", ext))
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
            ])?
            .into(),
        );
        if compression.is_none() {
            check_equal_local_arrow2(
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
            Some(
                CsvConvertOptions::default()
                    .with_column_names(Some(column_names.iter().map(|s| s.to_string()).collect())),
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
                Field::new("sepal.length", DataType::Float64),
                Field::new("sepal.width", DataType::Float64),
                Field::new("petal.length", DataType::Float64),
                Field::new("petal.width", DataType::Float64),
                Field::new("variety", DataType::Utf8),
            ])?
            .into(),
        );
        check_equal_local_arrow2(
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
            ])?
            .into(),
        );
        check_equal_local_arrow2(
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
            ])?
            .into(),
        );
        check_equal_local_arrow2(
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
            ])?
            .into(),
        );
        check_equal_local_arrow2(
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
            ])?
            .into(),
        );
        check_equal_local_arrow2(
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
            ])?
            .into(),
        );
        check_equal_local_arrow2(
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
            ])?
            .into(),
        );
        check_equal_local_arrow2(
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
            ])?
            .into(),
        );
        check_equal_local_arrow2(
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
                    .with_column_names(Some(column_names.iter().map(|s| s.to_string()).collect()))
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
            ])?
            .into(),
        );
        check_equal_local_arrow2(
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
            ])?
            .into(),
        );
        check_equal_local_arrow2(
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
            ])?
            .into(),
        );
        check_equal_local_arrow2(
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
            ])?
            .into(),
        );
        check_equal_local_arrow2(
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
            ])?
            .into(),
        );
        check_equal_local_arrow2(
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
            ])?
            .into(),
        );
        let null_column = table.get_column("petal.length")?;
        assert_eq!(null_column.data_type(), &DataType::Null);
        assert_eq!(null_column.len(), 6);
        assert_eq!(
            null_column.to_arrow(),
            Box::new(arrow2::array::NullArray::new(
                arrow2::datatypes::DataType::Null,
                6
            )) as Box<dyn arrow2::array::Array>
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
        ])?;

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
            ])?
            .into(),
        );
        let null_column = table.get_column("petal.length")?;
        assert_eq!(null_column.data_type(), &DataType::Null);
        assert_eq!(null_column.len(), 6);
        assert_eq!(
            null_column.to_arrow(),
            Box::new(arrow2::array::NullArray::new(
                arrow2::datatypes::DataType::Null,
                6
            )) as Box<dyn arrow2::array::Array>
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
            ])?
            .into(),
        );
        check_equal_local_arrow2(
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
        ])?;
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
            let column = table.get_column_by_index(idx)?;
            assert_eq!(column.to_arrow().null_count(), num_rows);
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
        assert!(matches!(err, DaftError::ArrowError(_)), "{}", err);
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
            ])?
            .into(),
        );

        // First 4 cols should have no nulls
        assert_eq!(table.get_column("sepal.length")?.to_arrow().null_count(), 0);
        assert_eq!(table.get_column("sepal.width")?.to_arrow().null_count(), 0);
        assert_eq!(table.get_column("petal.length")?.to_arrow().null_count(), 0);
        assert_eq!(table.get_column("petal.width")?.to_arrow().null_count(), 0);

        // Last col should have 3 nulls because of the missing data
        assert_eq!(table.get_column("variety")?.to_arrow().null_count(), 3);

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
        assert!(matches!(err, DaftError::ArrowError(_)), "{}", err);
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
            ])?
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
        ])?;

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
            table.get_column("variety")?.to_arrow(),
            Box::new(arrow2::array::Utf8Array::<i64>::from(vec![
                None,
                Some("Seratosa"),
                None,
            ])) as Box<dyn arrow2::array::Array>
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
            compression.map_or("".to_string(), |ext| format!(".{}", ext))
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
            ])?
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
            Some(
                CsvConvertOptions::default()
                    .with_column_names(Some(column_names.iter().map(|s| s.to_string()).collect())),
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
            Schema::new(vec![
                Field::new("a", DataType::Int64),
                Field::new("b", DataType::Utf8)
            ])?
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
                    .with_column_names(Some(column_names.iter().map(|s| s.to_string()).collect()))
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
            Schema::new(vec![Field::new("b", DataType::Utf8)])?.into(),
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
            ])?
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
            Schema::new(vec![Field::new("b", DataType::Utf8)])?.into(),
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
