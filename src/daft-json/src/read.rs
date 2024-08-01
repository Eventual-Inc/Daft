use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};

use common_error::{DaftError, DaftResult};
use daft_core::{schema::Schema, utils::arrow::cast_array_for_daft_if_needed, Series};
use daft_dsl::optimization::get_required_columns;
use daft_io::{get_runtime, parse_url, GetResult, IOClient, IOStatsRef, SourceType};
use daft_table::Table;
use futures::{stream::BoxStream, Stream, StreamExt, TryStreamExt};
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use snafu::{
    futures::{try_future::Context, TryFutureExt, TryStreamExt as _},
    ResultExt,
};
use tokio::{
    fs::File,
    io::{AsyncBufRead, AsyncBufReadExt, BufReader},
    task::JoinHandle,
};
use tokio_util::io::StreamReader;

use crate::{decoding::deserialize_records, local::read_json_local, ArrowSnafu, ChunkSnafu};
use crate::{
    schema::read_json_schema_single, JsonConvertOptions, JsonParseOptions, JsonReadOptions,
};
use daft_compression::CompressionCodec;

type TableChunkResult =
    super::Result<Context<JoinHandle<DaftResult<Table>>, super::JoinSnafu, super::Error>>;

type LineChunkResult = super::Result<Vec<String>>;

trait TableChunkStream: Stream<Item = TableChunkResult> {}
impl<T> TableChunkStream for T where T: Stream<Item = TableChunkResult> {}

trait LineChunkStream: Stream<Item = LineChunkResult> {}
impl<T> LineChunkStream for T where T: Stream<Item = LineChunkResult> {}

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
) -> DaftResult<Table> {
    let runtime_handle = get_runtime(multithreaded_io)?;
    let _rt_guard = runtime_handle.enter();
    runtime_handle.block_on(async {
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

#[allow(clippy::too_many_arguments)]
pub fn read_json_bulk(
    uris: &[&str],
    convert_options: Option<JsonConvertOptions>,
    parse_options: Option<JsonParseOptions>,
    read_options: Option<JsonReadOptions>,
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
                let table = read_json_single_into_table(
                    uri.as_str(),
                    convert_options,
                    parse_options,
                    read_options,
                    io_client,
                    io_stats,
                    max_chunks_in_flight,
                )
                .await?;
                DaftResult::Ok(table)
            })
            .context(crate::JoinSnafu)
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
                    // Limit has been met, early-teriminate.
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

// Parallel version of table concat
// get rid of this once Table APIs are parallel
pub(crate) fn tables_concat(mut tables: Vec<Table>) -> DaftResult<Table> {
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

async fn read_json_single_into_table(
    uri: &str,
    convert_options: Option<JsonConvertOptions>,
    parse_options: Option<JsonParseOptions>,
    read_options: Option<JsonReadOptions>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    max_chunks_in_flight: Option<usize>,
) -> DaftResult<Table> {
    let (source_type, fixed_uri) = parse_url(uri)?;
    let is_compressed = CompressionCodec::from_uri(uri).is_some();
    if matches!(source_type, SourceType::File) && !is_compressed {
        return read_json_local(
            fixed_uri.as_ref(),
            convert_options,
            parse_options,
            read_options,
            max_chunks_in_flight,
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
                        include_columns.push(rc)
                    }
                }
            }
            // if we have a limit and a predicate, remove limit for stream
            co.limit = None;
            Some(co)
        }
    };

    let (table_stream, schema) = read_json_single_into_stream(
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
    let tables = table_stream
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
        let daft_schema = Arc::new(Schema::try_from(&schema)?);
        return Table::empty(Some(daft_schema));
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

pub async fn stream_json(
    uri: String,
    convert_options: Option<JsonConvertOptions>,
    parse_options: Option<JsonParseOptions>,
    read_options: Option<JsonReadOptions>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    max_chunks_in_flight: Option<usize>,
) -> DaftResult<BoxStream<'static, DaftResult<Vec<Table>>>> {
    // BoxStream::
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

    let (chunk_stream, _fields) = read_json_single_into_stream(
        &uri,
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
    Ok(Box::pin(tables))
}

async fn read_json_single_into_stream(
    uri: &str,
    convert_options: JsonConvertOptions,
    parse_options: JsonParseOptions,
    read_options: Option<JsonReadOptions>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<(impl TableChunkStream + Send, arrow2::datatypes::Schema)> {
    let schema = match convert_options.schema {
        Some(schema) => schema.to_arrow()?,
        None => read_json_schema_single(
            uri,
            parse_options.clone(),
            // Read at most 1 MiB when doing schema inference.
            Some(1024 * 1024),
            io_client.clone(),
            io_stats.clone(),
        )
        .await?
        .to_arrow()?,
    };

    let (reader, buffer_size, chunk_size): (Box<dyn AsyncBufRead + Unpin + Send>, usize, usize) =
        match io_client
            .single_url_get(uri.to_string(), None, io_stats)
            .await?
        {
            GetResult::File(file) => {
                // Use user-provided buffer size, falling back to 8 * the user-provided chunk size if that exists, otherwise falling back to 512 KiB as the default.
                let buffer_size = read_options
                    .as_ref()
                    .and_then(|opt| {
                        opt.buffer_size
                            .or_else(|| opt.chunk_size.map(|cs| (64 * cs).min(256 * 1024 * 1024)))
                    })
                    .unwrap_or(256 * 1024);
                (
                    Box::new(BufReader::with_capacity(
                        buffer_size,
                        File::open(file.path).await?,
                    )),
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
            GetResult::Stream(stream, ..) => (
                Box::new(StreamReader::new(stream)),
                // Use user-provided buffer size, falling back to 8 * the user-provided chunk size if that exists, otherwise falling back to 512 KiB as the default.
                read_options
                    .as_ref()
                    .and_then(|opt| {
                        opt.buffer_size
                            .or_else(|| opt.chunk_size.map(|cs| (256 * cs).min(256 * 1024 * 1024)))
                    })
                    .unwrap_or(8 * 1024 * 1024),
                read_options
                    .as_ref()
                    .and_then(|opt| {
                        opt.chunk_size
                            .or_else(|| opt.buffer_size.map(|bs| (bs / 256).max(16)))
                    })
                    .unwrap_or(64),
            ),
        };
    // If file is compressed, wrap stream in decoding stream.
    let reader: Box<dyn AsyncBufRead + Unpin + Send> = match CompressionCodec::from_uri(uri) {
        Some(compression) => Box::new(tokio::io::BufReader::with_capacity(
            buffer_size,
            compression.to_decoder(reader),
        )),
        None => reader,
    };
    let read_stream = read_into_line_chunk_stream(reader, convert_options.limit, chunk_size);
    let (projected_schema, schema_is_projection) = match convert_options.include_columns {
        Some(projection) => {
            let mut field_map = schema
                .fields
                .into_iter()
                .map(|f| (f.name.clone(), f))
                .collect::<HashMap<_, _>>();
            let projected_fields = projection.into_iter().map(|col| field_map.remove(col.as_str()).ok_or(DaftError::ValueError(format!("Column {} in the projection doesn't exist in the JSON file; existing columns = {:?}", col, field_map.keys())))).collect::<DaftResult<Vec<_>>>()?;
            (
                arrow2::datatypes::Schema::from(projected_fields).with_metadata(schema.metadata),
                true,
            )
        }
        None => (schema, false),
    };
    Ok((
        parse_into_column_array_chunk_stream(
            read_stream,
            projected_schema.clone().into(),
            schema_is_projection,
        )?,
        projected_schema,
    ))
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
    // Stream of unparsed json string record chunks.
    let line_stream = tokio_stream::wrappers::LinesStream::new(reader.lines());
    line_stream
        .take(num_rows)
        .try_chunks(chunk_size)
        .context(ChunkSnafu)
}

fn parse_into_column_array_chunk_stream(
    stream: impl LineChunkStream + Send,
    schema: Arc<arrow2::datatypes::Schema>,
    schema_is_projection: bool,
) -> DaftResult<impl TableChunkStream + Send> {
    let daft_schema = Arc::new(daft_core::schema::Schema::try_from(schema.as_ref())?);
    let daft_fields = Arc::new(
        daft_schema
            .fields
            .values()
            .map(|f| Arc::new(f.clone()))
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
                    let chunk = deserialize_records(&parsed, schema.as_ref(), schema_is_projection)
                        .context(ArrowSnafu)?;
                    let all_series = chunk
                        .into_iter()
                        .zip(daft_fields.iter())
                        .map(|(array, field)| {
                            Series::try_from_field_and_arrow_array(
                                field.clone(),
                                cast_array_for_daft_if_needed(array),
                            )
                        })
                        .collect::<DaftResult<Vec<_>>>()?;
                    Ok(Table::new_unchecked(
                        daft_schema.clone(),
                        all_series,
                        num_rows,
                    ))
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

    use common_error::DaftResult;

    use daft_core::{
        datatypes::{Field, TimeUnit},
        schema::Schema,
        utils::arrow::{cast_array_for_daft_if_needed, cast_array_from_daft_if_needed},
        DataType,
    };
    use daft_io::{IOClient, IOConfig};
    use daft_table::Table;
    use indexmap::IndexMap;
    use rstest::rstest;

    use crate::{
        decoding::deserialize_records,
        inference::{column_types_map_to_fields, infer_records_schema},
    };
    use crate::{JsonConvertOptions, JsonReadOptions};

    use super::read_json;

    fn check_equal_local_arrow2(
        path: &str,
        out: &Table,
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
        let mut column_types: IndexMap<String, HashSet<arrow2::datatypes::DataType>> =
            IndexMap::new();
        parsed.iter().for_each(|record| {
            let schema = infer_records_schema(record).unwrap();
            for field in schema.fields {
                match column_types.entry(field.name) {
                    indexmap::map::Entry::Occupied(mut v) => {
                        v.get_mut().insert(field.data_type);
                    }
                    indexmap::map::Entry::Vacant(v) => {
                        let mut a = HashSet::new();
                        a.insert(field.data_type);
                        v.insert(a);
                    }
                }
            }
        });
        let fields = column_types_map_to_fields(column_types);
        let schema: arrow2::datatypes::Schema = fields.into();
        // Apply projection to schema.
        let mut field_map = schema
            .fields
            .iter()
            .map(|f| (f.name.clone(), f.clone()))
            .collect::<IndexMap<_, _>>();
        let (schema, is_projection) = match &projection {
            Some(projection) => (
                projection
                    .iter()
                    .map(|c| field_map.swap_remove(c.as_str()).unwrap())
                    .collect::<Vec<_>>()
                    .into(),
                true,
            ),
            None => (field_map.into_values().collect::<Vec<_>>().into(), false),
        };
        // Deserialize JSON records into Arrow2 column arrays.
        let columns = deserialize_records(&parsed, &schema, is_projection).unwrap();
        // Roundtrip columns with Daft for casting.
        let columns = columns
            .into_iter()
            .map(|c| cast_array_from_daft_if_needed(cast_array_for_daft_if_needed(c)))
            .collect::<Vec<_>>();
        // Roundtrip schema with Daft for casting.
        let schema = Schema::try_from(&schema).unwrap().to_arrow().unwrap();
        assert_eq!(out.schema.to_arrow().unwrap(), schema);
        let out_columns = (0..out.num_columns())
            .map(|i| out.get_column_by_index(i).unwrap().to_arrow())
            .collect::<Vec<_>>();
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
            compression.map_or("".to_string(), |ext| format!(".{}", ext))
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
            ])?
            .into(),
        );
        if compression.is_none() {
            check_equal_local_arrow2(file.as_ref(), &table, None, None);
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
            ])?
            .into(),
        );
        check_equal_local_arrow2(file.as_ref(), &table, None, None);

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
            ])?
            .into(),
        );
        check_equal_local_arrow2(file.as_ref(), &table, Some(5), None);

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
            ])?
            .into(),
        );
        check_equal_local_arrow2(
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
            ])?
            .into(),
        );
        check_equal_local_arrow2(file.as_ref(), &table, None, None);

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
            ])?
            .into(),
        );
        check_equal_local_arrow2(file.as_ref(), &table, None, None);

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
            ])?
            .into(),
        );
        check_equal_local_arrow2(file.as_ref(), &table, None, None);

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
            ])?
            .into(),
        );
        check_equal_local_arrow2(file.as_ref(), &table, None, None);

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
            ])?
            .into(),
        );
        let null_column = table.get_column("petalLength")?;
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
        ])?;
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
            ])?
            .into(),
        );
        let null_column = table.get_column("petalLength")?;
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
        ])?;

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
            ])?
            .into(),
        );
        let null_column = table.get_column("petalLength")?;
        assert_eq!(null_column.data_type(), &DataType::Float64);
        assert_eq!(null_column.len(), 6);
        assert_eq!(null_column.to_arrow().null_count(), 6);

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
        ])?;
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
            let column = table.get_column_by_index(idx)?;
            assert_eq!(column.to_arrow().null_count(), num_rows);
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
            compression.map_or("".to_string(), |ext| format!(".{}", ext))
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
            ])?
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
            ])?
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
            ])?
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
            ])?
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
            ])?
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
            ])?
            .into(),
        );

        Ok(())
    }
}
