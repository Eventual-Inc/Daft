use std::{collections::HashMap, io::SeekFrom, num::NonZeroUsize, sync::Arc};

use common_error::{DaftError, DaftResult};
use common_runtime::get_io_runtime;
use daft_compression::CompressionCodec;
use daft_core::prelude::*;
use daft_dsl::{expr::bound_expr::BoundExpr, optimization::get_required_columns};
use daft_io::{GetRange, GetResult, IOClient, IOStatsRef, SourceType, parse_url};
use daft_recordbatch::RecordBatch;
use futures::{
    Stream, StreamExt, TryStreamExt,
    stream::{BoxStream, once},
};
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use snafu::{
    ResultExt,
    futures::{TryFutureExt, try_future::Context},
};
use tokio::{
    fs::File,
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader},
    task::JoinHandle,
};
use tokio_util::io::StreamReader;

use crate::{
    ArrowSnafu, JoinSnafu, JsonConvertOptions, JsonParseOptions, JsonReadOptions, StdIOSnafu,
    decoding::deserialize_records,
    local::{read_json_array_impl, read_json_local_into_tables_with_range},
    schema::read_json_schema_single,
};

type TableChunkResult =
    super::Result<Context<JoinHandle<DaftResult<RecordBatch>>, super::JoinSnafu, super::Error>>;

type LineChunkResult = super::Result<Vec<String>>;

trait TableChunkStream: Stream<Item = TableChunkResult> {}
impl<T> TableChunkStream for T where T: Stream<Item = TableChunkResult> {}

trait LineChunkStream: Stream<Item = LineChunkResult> {}
impl<T> LineChunkStream for T where T: Stream<Item = LineChunkResult> {}

/// Single-file read exposed to Python FFI (`daft.daft.read_json`).
///
/// Only used by test paths; the production path uses `stream_json` via the scan operator.
#[allow(clippy::too_many_arguments)]
pub fn read_json(
    uri: &str,
    convert_options: Option<JsonConvertOptions>,
    parse_options: Option<JsonParseOptions>,
    read_options: Option<JsonReadOptions>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    multithreaded_io: bool,
    max_chunks_in_flight: Option<usize>,
) -> DaftResult<RecordBatch> {
    let runtime_handle = get_io_runtime(multithreaded_io);
    runtime_handle.block_on_current_thread(async {
        read_json_single_into_table(
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

// Parallel version of table concat
// get rid of this once Table APIs are parallel
pub(crate) fn tables_concat(mut tables: Vec<RecordBatch>) -> DaftResult<RecordBatch> {
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

pub(crate) fn truncate_tables_to_limit(
    tables: Vec<RecordBatch>,
    limit: Option<usize>,
) -> DaftResult<Vec<RecordBatch>> {
    let Some(limit) = limit else {
        return Ok(tables);
    };

    let mut out = Vec::new();
    let mut remaining = limit;
    for table in tables {
        if remaining == 0 {
            break;
        }
        let table_len = table.len();
        if table_len <= remaining {
            remaining -= table_len;
            out.push(table);
        } else {
            out.push(table.head(remaining)?);
            break;
        }
    }
    Ok(out)
}

/// Reads a single JSON file into multiple RecordBatches (one per chunk).
///
/// Only used by legacy test paths (via `read_json_single_into_table` → `read_json`).
async fn read_json_single_into_tables(
    uri: &str,
    convert_options: Option<JsonConvertOptions>,
    parse_options: Option<JsonParseOptions>,
    read_options: Option<JsonReadOptions>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    max_chunks_in_flight: Option<usize>,
) -> DaftResult<Vec<RecordBatch>> {
    let (source_type, fixed_uri) = parse_url(uri)?;
    let is_compressed = CompressionCodec::from_uri(uri).is_some();
    if matches!(source_type, SourceType::File) && !is_compressed {
        return read_json_local_into_tables_with_range(
            fixed_uri.as_ref(),
            convert_options,
            parse_options,
            read_options,
            max_chunks_in_flight,
            None,
        );
    }

    let predicate = convert_options.as_ref().and_then(|p| p.predicate.clone());

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

    let (table_stream, schema) = read_json_single_into_stream(
        uri.to_string(),
        convert_options_with_predicate_columns.unwrap_or_default(),
        parse_options.unwrap_or_default(),
        read_options,
        io_client,
        io_stats,
        None,
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
    // Limit the number of chunks we have in flight at any given time.
    let tables = table_stream.try_buffered(max_chunks_in_flight);

    let daft_schema: SchemaRef = Arc::new(Schema::try_from(schema)?);

    let include_column_indices = include_columns
        .map(|include_columns| {
            include_columns
                .iter()
                .map(|name| daft_schema.get_index(name))
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
        .try_take_while(|result| match (result, remaining_rows) {
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
        })
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .collect::<DaftResult<Vec<_>>>()?;

    // Handle empty table case.
    if collected_tables.is_empty() {
        return Ok(Vec::new());
    }
    truncate_tables_to_limit(collected_tables, limit)
}

/// Backward-compatible wrapper that concatenates all chunks into a single RecordBatch.
///
/// Only used by legacy test paths (via `read_json`).
async fn read_json_single_into_table(
    uri: &str,
    convert_options: Option<JsonConvertOptions>,
    parse_options: Option<JsonParseOptions>,
    read_options: Option<JsonReadOptions>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    max_chunks_in_flight: Option<usize>,
) -> DaftResult<RecordBatch> {
    let schema_hint = convert_options
        .as_ref()
        .and_then(|c| c.schema.as_ref())
        .map_or_else(|| Schema::empty().into(), |s| s.clone());
    let tables = read_json_single_into_tables(
        uri,
        convert_options,
        parse_options,
        read_options,
        io_client,
        io_stats,
        max_chunks_in_flight,
    )
    .await?;
    if tables.is_empty() {
        return Ok(RecordBatch::empty(Some(schema_hint)));
    }
    tables_concat(tables)
}

#[allow(clippy::too_many_arguments)]
pub async fn stream_json(
    uri: String,
    convert_options: Option<JsonConvertOptions>,
    parse_options: Option<JsonParseOptions>,
    read_options: Option<JsonReadOptions>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    max_chunks_in_flight: Option<usize>,
    range: Option<GetRange>,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let (source_type, fixed_uri) = parse_url(&uri)?;
    let is_compressed = CompressionCodec::from_uri(&uri).is_some();
    if matches!(source_type, SourceType::File) && !is_compressed {
        let fixed_uri = fixed_uri.to_string();
        let tables = read_json_local_into_tables_with_range(
            fixed_uri.as_ref(),
            convert_options,
            parse_options,
            read_options,
            max_chunks_in_flight,
            range,
        )?;
        return Ok(Box::pin(futures::stream::iter(tables.into_iter().map(Ok))));
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

    let (chunk_stream, _fields) = read_json_single_into_stream(
        uri.clone(),
        convert_options_with_predicate_columns.unwrap_or_default(),
        parse_options.unwrap_or_default(),
        read_options,
        io_client,
        io_stats,
        range,
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
    Ok(Box::pin(tables))
}

async fn read_json_single_into_stream(
    uri: String,
    convert_options: JsonConvertOptions,
    parse_options: JsonParseOptions,
    read_options: Option<JsonReadOptions>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    range: Option<GetRange>,
) -> DaftResult<(
    BoxStream<'static, TableChunkResult>,
    arrow::datatypes::Schema,
)> {
    let schema = match convert_options.schema {
        Some(schema) => schema.to_arrow()?,
        None => read_json_schema_single(
            &uri,
            parse_options.clone(),
            // Read at most 1 MiB when doing schema inference.
            Some(1024 * 1024),
            io_client.clone(),
            io_stats.clone(),
            range.clone(),
        )
        .await?
        .to_arrow()?,
    };

    let (reader, buffer_size, chunk_size): (Box<dyn AsyncBufRead + Unpin + Send>, usize, usize) =
        match io_client
            .single_url_get(uri.clone(), range, io_stats)
            .await?
        {
            GetResult::File(file) => {
                // Use user-provided buffer size, falling back to 64 * the user-provided chunk size if that exists, otherwise falling back to 256 KiB as the default.
                let buffer_size = read_options
                    .as_ref()
                    .and_then(|opt| {
                        opt.buffer_size
                            .or_else(|| opt.chunk_size.map(|cs| (64 * cs).min(256 * 1024 * 1024)))
                    })
                    .unwrap_or(256 * 1024);
                let mut local_file = File::open(file.path).await?;
                let reader: Box<dyn AsyncBufRead + Unpin + Send> = match file.range {
                    Some(range) => {
                        let length = range.end.saturating_sub(range.start);
                        local_file.seek(SeekFrom::Start(range.start as u64)).await?;
                        let limited = local_file.take(length as u64);
                        Box::new(BufReader::with_capacity(buffer_size, limited))
                    }
                    None => Box::new(BufReader::with_capacity(buffer_size, local_file)),
                };
                (
                    reader,
                    buffer_size,
                    read_options
                        .as_ref()
                        .and_then(|opt| {
                            opt.chunk_size
                                .or_else(|| opt.buffer_size.map(|bs| (bs / 64).max(16)))
                        })
                        .unwrap_or(64),
                )
            }
            GetResult::Stream(stream, ..) => {
                // Use user-provided buffer size, falling back to 256 * the user-provided chunk size if that exists, otherwise falling back to 8 MiB as the default for streams.
                let buffer_size = read_options
                    .as_ref()
                    .and_then(|opt| {
                        opt.buffer_size
                            .or_else(|| opt.chunk_size.map(|cs| (256 * cs).min(256 * 1024 * 1024)))
                    })
                    .unwrap_or(8 * 1024 * 1024);
                let chunk_size = read_options
                    .as_ref()
                    .and_then(|opt| {
                        opt.chunk_size
                            .or_else(|| opt.buffer_size.map(|bs| (bs / 256).max(16)))
                    })
                    .unwrap_or(64);
                (Box::new(StreamReader::new(stream)), buffer_size, chunk_size)
            }
        };
    // If file is compressed, wrap stream in decoding stream.
    let mut reader: Box<dyn AsyncBufRead + Unpin + Send> = match CompressionCodec::from_uri(&uri) {
        Some(compression) => Box::new(tokio::io::BufReader::with_capacity(
            buffer_size,
            compression.to_decoder(reader),
        )),
        None => reader,
    };

    // Only skip truly empty files when configured; do not accept leading whitespace.
    let buf = reader.fill_buf().await?;
    if buf.is_empty() {
        if parse_options.skip_empty_files {
            return Ok((Box::pin(futures::stream::iter(vec![])), schema.clone()));
        } else {
            return Err(super::Error::JsonDeserializationError {
                string: "Empty JSON file".to_string(),
            }
            .into());
        }
    }
    match buf[0] {
        b'[' => {
            let schema_clone = schema.clone();
            let inner: Context<JoinHandle<Result<RecordBatch, DaftError>>, JoinSnafu, _> =
                tokio::spawn(async move {
                    let (send, recv) = tokio::sync::oneshot::channel();
                    let mut buf = Vec::new();
                    reader.read_to_end(&mut buf).await?;
                    let daft_schema = Schema::try_from(schema_clone)?;
                    let chunk = read_json_array_impl(&buf, daft_schema, None);
                    let _ = send.send(chunk);

                    recv.await.context(super::OneShotRecvSnafu {})?
                })
                .context(super::JoinSnafu {});
            Ok((Box::pin(once(async move { Ok(inner) })), schema))
        }
        b'{' if buf.len() > 1 && matches!(buf[1], b'\n' | b'\r') => {
            let schema_clone = schema.clone();
            let inner: Context<JoinHandle<Result<RecordBatch, DaftError>>, JoinSnafu, _> =
                tokio::spawn(async move {
                    let (send, recv) = tokio::sync::oneshot::channel();
                    let mut buf = Vec::new();
                    reader.read_to_end(&mut buf).await?;
                    let daft_schema = Schema::try_from(schema_clone)?;
                    let chunk = read_json_array_impl(&buf, daft_schema, None);
                    let _ = send.send(chunk);

                    recv.await.context(super::OneShotRecvSnafu {})?
                })
                .context(super::JoinSnafu {});
            Ok((Box::pin(once(async move { Ok(inner) })), schema))
        }
        b'{' => {
            let read_stream =
                read_into_line_chunk_stream(reader, convert_options.limit, chunk_size);
            let projected_schema = match convert_options.include_columns {
                Some(projection) => {
                    let mut field_map = schema
                        .fields()
                        .iter()
                        .map(|f| (f.name().clone(), f.as_ref().clone()))
                        .collect::<HashMap<_, _>>();
                    let projected_fields = projection.into_iter().map(|col| field_map.remove(col.as_str()).ok_or(DaftError::ValueError(format!("Column {} in the projection doesn't exist in the JSON file; existing columns = {:?}", col, field_map.keys())))).collect::<DaftResult<Vec<_>>>()?;
                    arrow::datatypes::Schema::new(projected_fields)
                        .with_metadata(schema.metadata.clone())
                }
                None => schema,
            };

            Ok((
                Box::pin(parse_into_column_array_chunk_stream(
                    read_stream,
                    Arc::new(projected_schema.clone()),
                )?),
                projected_schema,
            ))
        }
        _ => Err(DaftError::ValueError(
            "Invalid JSON file format".to_string(),
        )),
    }
}

fn read_into_line_chunk_stream<R>(
    reader: R,
    num_rows: Option<usize>,
    chunk_size: usize,
) -> impl LineChunkStream + Send
where
    R: AsyncBufRead + Unpin + Send + 'static,
{
    let num_rows = num_rows.unwrap_or(usize::MAX);
    let chunk_size = chunk_size.max(1);
    // Stream of unparsed json string record chunks, grouped by byte size.
    futures::stream::try_unfold(
        (reader, 0usize),
        move |(mut reader, mut total_rows_read)| async move {
            if total_rows_read >= num_rows {
                return Ok(None);
            }

            let mut records: Vec<String> = Vec::new();
            let mut bytes_in_chunk: usize = 0;
            let mut line = String::new();

            while total_rows_read < num_rows {
                line.clear();
                let bytes_read = reader.read_line(&mut line).await.context(StdIOSnafu)?;
                if bytes_read == 0 {
                    break;
                }

                if line.ends_with('\n') {
                    line.pop();
                    if line.ends_with('\r') {
                        line.pop();
                    }
                }

                bytes_in_chunk = bytes_in_chunk.saturating_add(line.len());
                total_rows_read = total_rows_read.saturating_add(1);
                records.push(std::mem::take(&mut line));

                if bytes_in_chunk >= chunk_size {
                    break;
                }
            }

            if records.is_empty() {
                Ok(None)
            } else {
                Ok(Some((records, (reader, total_rows_read))))
            }
        },
    )
}

fn parse_into_column_array_chunk_stream(
    stream: impl LineChunkStream + Send,
    schema: Arc<arrow::datatypes::Schema>,
) -> DaftResult<impl TableChunkStream + Send> {
    let daft_schema: SchemaRef = Arc::new(Schema::try_from(schema.as_ref())?);
    let daft_fields = Arc::new(
        daft_schema
            .into_iter()
            .cloned()
            .map(Arc::new)
            .collect::<Vec<_>>(),
    );
    // Parsing stream: we spawn background tokio + rayon tasks so we can pipeline chunk parsing with chunk reading, and
    // we further parse each chunk column in parallel on the rayon threadpool.
    Ok(stream.map_ok(move |mut records| {
        let schema = schema.clone();
        let daft_schema = daft_schema.clone();
        let daft_fields = daft_fields.clone();
        let num_rows = records.len();
        tokio::spawn(async move {
            let (send, recv) = tokio::sync::oneshot::channel();
            rayon::spawn(move || {
                let result = (move || {
                    // TODO(Clark): Switch to streaming parse + array construction?
                    let parsed = records
                        .iter_mut()
                        .map(|unparsed_record| {
                            crate::deserializer::to_value(unsafe { unparsed_record.as_bytes_mut() })
                                .map_err(|e| super::Error::JsonDeserializationError {
                                    string: e.to_string(),
                                })
                        })
                        .collect::<super::Result<Vec<_>>>()?;
                    let chunk =
                        deserialize_records(&parsed, schema.as_ref()).context(ArrowSnafu)?;
                    let all_series = chunk
                        .into_iter()
                        .zip(daft_fields.iter())
                        .map(|(array, field)| Series::from_arrow(field.clone(), array))
                        .collect::<DaftResult<Vec<_>>>()?;
                    RecordBatch::new_with_size(daft_schema.clone(), all_series, num_rows)
                })();
                let _ = send.send(result);
            });
            recv.await.context(super::OneShotRecvSnafu {})?
        })
        .context(super::JoinSnafu {})
    }))
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, io::BufRead, sync::Arc};

    use arrow::array::ArrayRef;
    use common_error::DaftResult;
    use daft_core::prelude::*;
    use daft_io::{IOClient, IOConfig};
    use daft_recordbatch::RecordBatch;
    use futures::TryStreamExt;
    use indexmap::IndexMap;
    use rstest::rstest;

    use super::read_json;
    use crate::{
        JsonConvertOptions, JsonReadOptions,
        decoding::deserialize_records,
        inference::{column_types_map_to_fields, infer_records_schema},
    };

    #[test]
    fn test_streaming_chunk_size_is_bytes() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let chunks = rt
            .block_on(async {
                let data = b"{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n";
                let (mut w, r) = tokio::io::duplex(1024);
                tokio::spawn(async move {
                    use tokio::io::AsyncWriteExt;
                    w.write_all(&data[..]).await.unwrap();
                    w.shutdown().await.unwrap();
                });
                let reader = tokio::io::BufReader::new(r);
                let stream = super::read_into_line_chunk_stream(reader, None, 14);
                stream.try_collect::<Vec<_>>().await
            })
            .unwrap();

        assert_eq!(
            chunks,
            vec![
                vec!["{\"a\":1}".to_string(), "{\"a\":2}".to_string()],
                vec!["{\"a\":3}".to_string()],
            ]
        );
    }

    fn check_equal_local_arrow(
        path: &str,
        out: &RecordBatch,
        limit: Option<usize>,
        projection: Option<Vec<String>>,
    ) {
        let reader = std::io::BufReader::new(std::fs::File::open(path).unwrap());
        let mut lines = reader.lines().map(|l| l.unwrap()).collect::<Vec<_>>();
        let parsed = lines
            .iter_mut()
            .take(limit.unwrap_or(usize::MAX))
            .map(|record| crate::deserializer::to_value(unsafe { record.as_bytes_mut() }).unwrap())
            .collect::<Vec<_>>();
        // Get consolidated schema from parsed JSON.
        let mut column_types: IndexMap<String, HashSet<arrow::datatypes::DataType>> =
            IndexMap::new();
        for record in &parsed {
            let schema = infer_records_schema(record).unwrap();
            for field in schema.fields() {
                match column_types.entry(field.name().clone()) {
                    indexmap::map::Entry::Occupied(mut v) => {
                        v.get_mut().insert(field.data_type().clone());
                    }
                    indexmap::map::Entry::Vacant(v) => {
                        let mut a = HashSet::new();
                        a.insert(field.data_type().clone());
                        v.insert(a);
                    }
                }
            }
        }
        let fields = column_types_map_to_fields(column_types);
        let schema = arrow::datatypes::Schema::new(fields);
        // Apply projection to schema.
        let mut field_map = schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.as_ref().clone()))
            .collect::<IndexMap<_, _>>();
        let schema = match &projection {
            Some(projection) => {
                let projected_fields = projection
                    .iter()
                    .map(|c| field_map.swap_remove(c.as_str()).unwrap())
                    .collect::<Vec<_>>();
                arrow::datatypes::Schema::new(projected_fields)
            }
            None => arrow::datatypes::Schema::new(field_map.into_values().collect::<Vec<_>>()),
        };
        // Deserialize JSON records into arrow-rs column arrays.
        let columns: Vec<ArrayRef> = deserialize_records(&parsed, &schema).unwrap();
        // Convert schema to Daft schema for comparison.
        let daft_schema = Schema::try_from(&schema).unwrap();
        assert_eq!(out.schema.as_ref(), &daft_schema);
        let out_columns: Vec<ArrayRef> = (0..out.num_columns())
            .map(|i| out.get_column(i).to_arrow())
            .collect::<DaftResult<Vec<_>>>()
            .unwrap();
        assert_eq!(out_columns, columns);
    }

    #[rstest]
    fn test_json_read_local(
        #[values(
            // Uncompressed
            None,
            // brotli
            Some("br"),
            // bzip2
            Some("bz2"),
            // TODO(Clark): Add deflate compressed JSON file to test data fixtures.
            // // deflate
            // Some("deflate"),
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
            "{}/test/iris_tiny.jsonl{}",
            env!("CARGO_MANIFEST_DIR"),
            compression.map_or(String::new(), |ext| format!(".{}", ext))
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_json(file.as_ref(), None, None, None, io_client, None, true, None)?;
        assert_eq!(table.len(), 20);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("sepalLength", DataType::Float64),
                Field::new("sepalWidth", DataType::Float64),
                Field::new("petalLength", DataType::Float64),
                Field::new("petalWidth", DataType::Float64),
                Field::new("species", DataType::Utf8),
            ])
            .into(),
        );
        if compression.is_none() {
            check_equal_local_arrow(file.as_ref(), &table, None, None);
        }

        Ok(())
    }

    #[rstest]
    fn test_json_read_local_dtypes() -> DaftResult<()> {
        let file = format!("{}/test/dtypes.jsonl", env!("CARGO_MANIFEST_DIR"),);

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_json(file.as_ref(), None, None, None, io_client, None, true, None)?;
        assert_eq!(table.len(), 4);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("int", DataType::Int64),
                Field::new("float", DataType::Float64),
                Field::new("bool", DataType::Boolean),
                Field::new("str", DataType::Utf8),
                Field::new("null", DataType::Null),
                Field::new("date", DataType::Date),
                // Time unit should be coarest granularity found in file, i.e. microseconds.
                Field::new("time", DataType::Time(TimeUnit::Microseconds)),
                // Time unit should be coarsest granularity found in file, i.e. seconds due to naive date inclusion.
                Field::new(
                    "naive_timestamp",
                    DataType::Timestamp(TimeUnit::Seconds, None)
                ),
                // Timezone should be UTC due to field having multiple different timezones across records.
                // Time unit should be coarsest granularity found in file, i.e. milliseconds.
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Milliseconds, Some("Z".to_string()))
                ),
                Field::new("list", DataType::List(Box::new(DataType::Int64))),
                Field::new(
                    "obj",
                    DataType::Struct(vec![
                        Field::new("a", DataType::Int64),
                        Field::new("b", DataType::Boolean)
                    ])
                ),
                Field::new(
                    "nested_list",
                    DataType::List(Box::new(DataType::List(Box::new(DataType::Struct(vec![
                        Field::new("a", DataType::Utf8),
                    ])))))
                ),
                Field::new(
                    "nested_obj",
                    DataType::Struct(vec![
                        Field::new(
                            "obj",
                            DataType::Struct(vec![Field::new("a", DataType::Int64)])
                        ),
                        Field::new("list", DataType::List(Box::new(DataType::Int64))),
                    ])
                ),
            ])
            .into(),
        );
        check_equal_local_arrow(file.as_ref(), &table, None, None);

        Ok(())
    }

    #[test]
    fn test_json_read_local_limit() -> DaftResult<()> {
        let file = format!("{}/test/iris_tiny.jsonl", env!("CARGO_MANIFEST_DIR"),);

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_json(
            file.as_ref(),
            Some(JsonConvertOptions::default().with_limit(Some(5))),
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
                Field::new("sepalLength", DataType::Float64),
                Field::new("sepalWidth", DataType::Float64),
                Field::new("petalLength", DataType::Float64),
                Field::new("petalWidth", DataType::Float64),
                Field::new("species", DataType::Utf8),
            ])
            .into(),
        );
        check_equal_local_arrow(file.as_ref(), &table, Some(5), None);

        Ok(())
    }

    #[test]
    fn test_json_read_local_projection() -> DaftResult<()> {
        let file = format!("{}/test/iris_tiny.jsonl", env!("CARGO_MANIFEST_DIR"),);

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_json(
            file.as_ref(),
            Some(
                JsonConvertOptions::default().with_include_columns(Some(vec![
                    "petalWidth".to_string(),
                    "petalLength".to_string(),
                ])),
            ),
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
                Field::new("petalWidth", DataType::Float64),
                Field::new("petalLength", DataType::Float64),
            ])
            .into(),
        );
        check_equal_local_arrow(
            file.as_ref(),
            &table,
            None,
            Some(vec!["petalWidth".to_string(), "petalLength".to_string()]),
        );

        Ok(())
    }

    #[test]
    fn test_json_read_local_larger_than_buffer_size() -> DaftResult<()> {
        let file = format!("{}/test/iris_tiny.jsonl", env!("CARGO_MANIFEST_DIR"),);

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_json(
            file.as_ref(),
            None,
            None,
            Some(JsonReadOptions::default().with_buffer_size(Some(128))),
            io_client,
            None,
            true,
            None,
        )?;
        assert_eq!(table.len(), 20);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("sepalLength", DataType::Float64),
                Field::new("sepalWidth", DataType::Float64),
                Field::new("petalLength", DataType::Float64),
                Field::new("petalWidth", DataType::Float64),
                Field::new("species", DataType::Utf8),
            ])
            .into(),
        );
        check_equal_local_arrow(file.as_ref(), &table, None, None);

        Ok(())
    }

    #[test]
    fn test_json_read_local_larger_than_chunk_size() -> DaftResult<()> {
        let file = format!("{}/test/iris_tiny.jsonl", env!("CARGO_MANIFEST_DIR"),);

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_json(
            file.as_ref(),
            None,
            None,
            Some(JsonReadOptions::default().with_chunk_size(Some(5))),
            io_client,
            None,
            true,
            None,
        )?;
        assert_eq!(table.len(), 20);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("sepalLength", DataType::Float64),
                Field::new("sepalWidth", DataType::Float64),
                Field::new("petalLength", DataType::Float64),
                Field::new("petalWidth", DataType::Float64),
                Field::new("species", DataType::Utf8),
            ])
            .into(),
        );
        check_equal_local_arrow(file.as_ref(), &table, None, None);

        Ok(())
    }

    #[test]
    fn test_json_read_local_throttled_streaming() -> DaftResult<()> {
        let file = format!("{}/test/iris_tiny.jsonl", env!("CARGO_MANIFEST_DIR"),);

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_json(
            file.as_ref(),
            None,
            None,
            Some(JsonReadOptions::default().with_chunk_size(Some(5))),
            io_client,
            None,
            true,
            Some(2),
        )?;
        assert_eq!(table.len(), 20);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("sepalLength", DataType::Float64),
                Field::new("sepalWidth", DataType::Float64),
                Field::new("petalLength", DataType::Float64),
                Field::new("petalWidth", DataType::Float64),
                Field::new("species", DataType::Utf8),
            ])
            .into(),
        );
        check_equal_local_arrow(file.as_ref(), &table, None, None);

        Ok(())
    }

    #[test]
    fn test_json_read_local_nulls() -> DaftResult<()> {
        let file = format!("{}/test/iris_tiny_nulls.jsonl", env!("CARGO_MANIFEST_DIR"),);

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_json(file.as_ref(), None, None, None, io_client, None, true, None)?;
        assert_eq!(table.len(), 6);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("sepalLength", DataType::Float64),
                Field::new("sepalWidth", DataType::Float64),
                Field::new("petalLength", DataType::Float64),
                Field::new("petalWidth", DataType::Float64),
                Field::new("species", DataType::Utf8),
            ])
            .into(),
        );
        check_equal_local_arrow(file.as_ref(), &table, None, None);

        Ok(())
    }

    #[test]
    fn test_json_read_local_all_null_column() -> DaftResult<()> {
        let file = format!(
            "{}/test/iris_tiny_all_null_column.jsonl",
            env!("CARGO_MANIFEST_DIR"),
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_json(file.as_ref(), None, None, None, io_client, None, true, None)?;
        assert_eq!(table.len(), 6);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("sepalLength", DataType::Float64),
                Field::new("sepalWidth", DataType::Float64),
                // All null column parsed as null dtype.
                Field::new("petalLength", DataType::Null),
                Field::new("petalWidth", DataType::Float64),
                Field::new("species", DataType::Utf8),
            ])
            .into(),
        );
        let null_column = table.get_column(2);
        assert_eq!(null_column.data_type(), &DataType::Null);
        assert_eq!(null_column.len(), 6);
        assert_eq!(
            null_column.null().unwrap(),
            &NullArray::full_null("petalLength", &DataType::Null, 6)
        );

        Ok(())
    }

    #[test]
    fn test_json_read_local_all_null_column_with_schema() -> DaftResult<()> {
        let file = format!(
            "{}/test/iris_tiny_all_null_column.jsonl",
            env!("CARGO_MANIFEST_DIR"),
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let schema = Schema::new(vec![
            Field::new("sepalLength", DataType::Float64),
            Field::new("sepalWidth", DataType::Float64),
            // Manually specify this column as all-null.
            Field::new("petalLength", DataType::Null),
            Field::new("petalWidth", DataType::Float64),
            Field::new("species", DataType::Utf8),
        ]);
        let table = read_json(
            file.as_ref(),
            Some(JsonConvertOptions::default().with_schema(Some(schema.into()))),
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
                Field::new("sepalLength", DataType::Float64),
                Field::new("sepalWidth", DataType::Float64),
                // All null column parsed as null dtype.
                Field::new("petalLength", DataType::Null),
                Field::new("petalWidth", DataType::Float64),
                Field::new("species", DataType::Utf8),
            ])
            .into(),
        );
        let null_column = table.get_column(2);
        assert_eq!(null_column.data_type(), &DataType::Null);
        assert_eq!(null_column.len(), 6);
        assert_eq!(
            null_column.null().unwrap(),
            &NullArray::full_null("petalLength", &DataType::Null, 6)
        );

        Ok(())
    }

    #[test]
    fn test_json_read_local_all_null_column_with_schema_well_typed() -> DaftResult<()> {
        let file = format!(
            "{}/test/iris_tiny_all_null_column.jsonl",
            env!("CARGO_MANIFEST_DIR"),
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);
        let schema = Schema::new(vec![
            Field::new("sepalLength", DataType::Float64),
            Field::new("sepalWidth", DataType::Float64),
            // Provide a manual type for the all-null column.
            Field::new("petalLength", DataType::Float64),
            Field::new("petalWidth", DataType::Float64),
            Field::new("species", DataType::Utf8),
        ]);

        let table = read_json(
            file.as_ref(),
            Some(JsonConvertOptions::default().with_schema(Some(schema.into()))),
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
                Field::new("sepalLength", DataType::Float64),
                Field::new("sepalWidth", DataType::Float64),
                // All null column should have the provided dtype.
                Field::new("petalLength", DataType::Float64),
                Field::new("petalWidth", DataType::Float64),
                Field::new("species", DataType::Utf8),
            ])
            .into(),
        );
        let null_column = table.get_column(2);
        assert_eq!(null_column.data_type(), &DataType::Float64);
        assert_eq!(null_column.len(), 6);
        assert_eq!(null_column.null_count(), 6);

        Ok(())
    }

    #[test]
    fn test_json_read_local_wrong_type_yields_nulls() -> DaftResult<()> {
        let file = format!("{}/test/iris_tiny.jsonl", env!("CARGO_MANIFEST_DIR"),);

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let schema = Schema::new(vec![
            // Conversion to all of these types should fail, resulting in nulls.
            Field::new("sepalLength", DataType::Boolean),
            Field::new("sepalWidth", DataType::Boolean),
            Field::new("petalLength", DataType::Boolean),
            Field::new("petalWidth", DataType::Boolean),
            Field::new("species", DataType::Int64),
        ]);
        let table = read_json(
            file.as_ref(),
            Some(JsonConvertOptions::default().with_schema(Some(schema.into()))),
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

    #[rstest]
    fn test_json_read_s3(
        #[values(
            // Uncompressed
            None,
            // brotli
            Some("br"),
            // bzip2
            Some("bz2"),
            // TODO(Clark): Add deflate compressed JSON file to test data fixtures.
            // // deflate
            // Some("deflate"),
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
            "s3://daft-public-data/test_fixtures/json-dev/iris_tiny.jsonl{}",
            compression.map_or(String::new(), |ext| format!(".{}", ext))
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_json(file.as_ref(), None, None, None, io_client, None, true, None)?;
        assert_eq!(table.len(), 20);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("sepalLength", DataType::Float64),
                Field::new("sepalWidth", DataType::Float64),
                Field::new("petalLength", DataType::Float64),
                Field::new("petalWidth", DataType::Float64),
                Field::new("species", DataType::Utf8),
            ])
            .into(),
        );

        Ok(())
    }

    #[test]
    fn test_json_read_s3_limit() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/json-dev/iris_tiny.jsonl";

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_json(
            file,
            Some(JsonConvertOptions::default().with_limit(Some(5))),
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
                Field::new("sepalLength", DataType::Float64),
                Field::new("sepalWidth", DataType::Float64),
                Field::new("petalLength", DataType::Float64),
                Field::new("petalWidth", DataType::Float64),
                Field::new("species", DataType::Utf8),
            ])
            .into(),
        );

        Ok(())
    }

    #[test]
    fn test_json_read_s3_projection() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/json-dev/iris_tiny.jsonl";

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_json(
            file,
            Some(
                JsonConvertOptions::default().with_include_columns(Some(vec![
                    "petalWidth".to_string(),
                    "petalLength".to_string(),
                ])),
            ),
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
                Field::new("petalWidth", DataType::Float64),
                Field::new("petalLength", DataType::Float64),
            ])
            .into(),
        );

        Ok(())
    }

    #[test]
    fn test_json_read_s3_larger_than_buffer_size() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/json-dev/iris_tiny.jsonl";

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_json(
            file,
            None,
            None,
            Some(JsonReadOptions::default().with_buffer_size(Some(128))),
            io_client,
            None,
            true,
            None,
        )?;
        assert_eq!(table.len(), 20);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("sepalLength", DataType::Float64),
                Field::new("sepalWidth", DataType::Float64),
                Field::new("petalLength", DataType::Float64),
                Field::new("petalWidth", DataType::Float64),
                Field::new("species", DataType::Utf8),
            ])
            .into(),
        );

        Ok(())
    }

    #[test]
    fn test_json_read_s3_larger_than_chunk_size() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/json-dev/iris_tiny.jsonl";

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_json(
            file,
            None,
            None,
            Some(JsonReadOptions::default().with_chunk_size(Some(5))),
            io_client,
            None,
            true,
            None,
        )?;
        assert_eq!(table.len(), 20);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("sepalLength", DataType::Float64),
                Field::new("sepalWidth", DataType::Float64),
                Field::new("petalLength", DataType::Float64),
                Field::new("petalWidth", DataType::Float64),
                Field::new("species", DataType::Utf8),
            ])
            .into(),
        );

        Ok(())
    }

    #[test]
    fn test_json_read_s3_throttled_streaming() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/json-dev/iris_tiny.jsonl";

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_json(
            file,
            None,
            None,
            Some(JsonReadOptions::default().with_chunk_size(Some(5))),
            io_client,
            None,
            true,
            Some(2),
        )?;
        assert_eq!(table.len(), 20);
        assert_eq!(
            table.schema,
            Schema::new(vec![
                Field::new("sepalLength", DataType::Float64),
                Field::new("sepalWidth", DataType::Float64),
                Field::new("petalLength", DataType::Float64),
                Field::new("petalWidth", DataType::Float64),
                Field::new("species", DataType::Utf8),
            ])
            .into(),
        );

        Ok(())
    }
}
