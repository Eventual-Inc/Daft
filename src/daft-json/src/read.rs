use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};

use common_error::{DaftError, DaftResult};
use daft_core::{schema::Schema, utils::arrow::cast_array_for_daft_if_needed, Series};
use daft_io::{get_runtime, GetResult, IOClient, IOStatsRef};
use daft_table::Table;
use futures::{Stream, StreamExt, TryStreamExt};
use rayon::prelude::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
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

use crate::{decoding::deserialize_records, ArrowSnafu, ChunkSnafu};
use crate::{
    metadata::read_json_schema_single, JsonConvertOptions, JsonParseOptions, JsonReadOptions,
};
use daft_decoding::compression::CompressionCodec;

trait LineChunkStream = Stream<Item = super::Result<Vec<String>>>;
trait ColumnArrayChunkStream = Stream<
    Item = super::Result<
        Context<
            JoinHandle<DaftResult<Vec<Box<dyn arrow2::array::Array>>>>,
            super::JoinSnafu,
            super::Error,
        >,
    >,
>;

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
    let tables = runtime_handle
        .block_on(async move {
            // Launch a read task per URI, throttling the number of concurrent file reads to num_parallel tasks.
            let task_stream = futures::stream::iter(uris.iter().enumerate().map(|(i, uri)| {
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
                    Ok((i, table))
                })
            }));
            let mut remaining_rows = convert_options
                .as_ref()
                .and_then(|opts| opts.limit.map(|limit| limit as i64));
            task_stream
                // Each task is annotated with its position in the output, so we can use unordered buffering to help mitigate stragglers
                // and sort the task results at the end.
                .buffer_unordered(num_parallel_tasks)
                // Terminate the stream if we have already reached the row limit. With the upstream buffering, we will still read up to
                // num_parallel_tasks redundant files.
                .try_take_while(|result| {
                    match (result, remaining_rows) {
                        // Limit has been met, early-teriminate.
                        (_, Some(rows_left)) if rows_left <= 0 => futures::future::ready(Ok(false)),
                        // Limit has not yet been met, update remaining limit slack and continue.
                        (Ok((_, table)), Some(rows_left)) => {
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
        })
        .context(super::JoinSnafu {})?;

    // Sort the task results by task index, yielding tables whose order matches the input URI order.
    let mut collected = tables.into_iter().collect::<DaftResult<Vec<_>>>()?;
    collected.sort_by_key(|(idx, _)| *idx);
    Ok(collected.into_iter().map(|(_, v)| v).collect())
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
    let (chunk_stream, schema) = read_json_single_into_stream(
        uri,
        convert_options.unwrap_or_default(),
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
            .try_into()
            .unwrap()
    });
    // Collect all chunks in chunk x column form.
    let chunks = chunk_stream
        // Limit the number of chunks we have in flight at any given time.
        .try_buffered(max_chunks_in_flight)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .collect::<DaftResult<Vec<_>>>()?;
    // Handle empty table case.
    if chunks.is_empty() {
        let daft_schema = Arc::new(Schema::try_from(&schema)?);
        return Table::empty(Some(daft_schema));
    }
    // Transpose chunk x column into column x chunk.
    let mut column_arrays = vec![Vec::with_capacity(chunks.len()); chunks[0].len()];
    for chunk in chunks.into_iter() {
        for (idx, col) in chunk.into_iter().enumerate() {
            column_arrays[idx].push(col);
        }
    }
    // Build table from chunks.
    // TODO(Clark): Don't concatenate all chunks from a file into a single table, since MicroPartition is natively chunked.
    chunks_to_table(column_arrays, schema)
}

async fn read_json_single_into_stream(
    uri: &str,
    convert_options: JsonConvertOptions,
    parse_options: JsonParseOptions,
    read_options: Option<JsonReadOptions>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<(
    impl ColumnArrayChunkStream + Send,
    arrow2::datatypes::Schema,
)> {
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
            GetResult::Stream(stream, _, _) => (
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
        ),
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
) -> impl ColumnArrayChunkStream + Send {
    // Parsing stream: we spawn background tokio + rayon tasks so we can pipeline chunk parsing with chunk reading, and
    // we further parse each chunk column in parallel on the rayon threadpool.
    stream.map_ok(move |records| {
        let schema = schema.clone();
        tokio::spawn(async move {
            let (send, recv) = tokio::sync::oneshot::channel();
            rayon::spawn(move || {
                let result = (move || {
                    // TODO(Clark): Switch to streaming parse + array construction?
                    let parsed = records
                        .iter()
                        .map(|unparsed_record| {
                            json_deserializer::parse(unparsed_record.as_bytes()).map_err(|e| {
                                super::Error::JsonDeserializationError {
                                    string: e.to_string(),
                                }
                            })
                        })
                        .collect::<super::Result<Vec<_>>>()?;
                    let chunk = deserialize_records(parsed, schema.as_ref(), schema_is_projection)
                        .context(ArrowSnafu)?;
                    Ok(chunk)
                })();
                let _ = send.send(result);
            });
            recv.await.context(super::OneShotRecvSnafu {})?
        })
        .context(super::JoinSnafu {})
    })
}

fn chunks_to_table(
    chunks: Vec<Vec<Box<dyn arrow2::array::Array>>>,
    schema: arrow2::datatypes::Schema,
) -> DaftResult<Table> {
    // Concatenate column chunks and convert into Daft Series.
    // Note that this concatenation is done in parallel on the rayon threadpool.
    let columns_series = chunks
        .into_par_iter()
        .zip(&schema.fields)
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
    // Build Daft Table.
    let daft_schema = Schema::try_from(&schema)?;
    Table::new(daft_schema, columns_series)
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
        let lines = reader.lines().collect::<Vec<_>>();
        let parsed = lines
            .iter()
            .take(limit.unwrap_or(usize::MAX))
            .map(|record| json_deserializer::parse(record.as_ref().unwrap().as_bytes()).unwrap())
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
                    .map(|c| field_map.remove(c.as_str()).unwrap())
                    .collect::<Vec<_>>()
                    .into(),
                true,
            ),
            None => (
                field_map
                    .into_values()
                    .into_iter()
                    .collect::<Vec<_>>()
                    .into(),
                false,
            ),
        };
        // Deserialize JSON records into Arrow2 column arrays.
        let columns = deserialize_records(parsed, &schema, is_projection).unwrap();
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
                // TODO(Clark): Add coverage for time parsing once we add support for representing time series in Daft.
                // // Timezone should be finest granularity found in file, i.e. nanoseconds.
                // Field::new("time", DataType::Time(TimeUnit::Nanoseconds)),
                // Timezone should be finest granularity found in file, i.e. microseconds.
                Field::new(
                    "naive_timestamp",
                    DataType::Timestamp(TimeUnit::Microseconds, None)
                ),
                // Timezone should be UTC due to field having multiple different timezones across records.
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
                // All null column should have hte provided dtype.
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

        Ok(())
    }

    #[test]
    fn test_json_read_s3_projection() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/json-dev/iris_tiny.jsonl";

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

        Ok(())
    }

    #[test]
    fn test_json_read_s3_larger_than_buffer_size() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/json-dev/iris_tiny.jsonl";

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

        Ok(())
    }

    #[test]
    fn test_json_read_s3_larger_than_chunk_size() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/json-dev/iris_tiny.jsonl";

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

        Ok(())
    }

    #[test]
    fn test_json_read_s3_throttled_streaming() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/json-dev/iris_tiny.jsonl";

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

        Ok(())
    }
}
