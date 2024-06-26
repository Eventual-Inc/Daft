use std::{collections::HashSet, sync::Arc};

use common_error::DaftResult;
use daft_core::schema::Schema;
use daft_io::{get_runtime, GetResult, IOClient, IOStatsRef};
use futures::{StreamExt, TryStreamExt};
use indexmap::IndexMap;
use snafu::ResultExt;
use tokio::{
    fs::File,
    io::{AsyncBufRead, AsyncBufReadExt, BufReader},
};
use tokio_util::io::StreamReader;

use crate::{
    inference::{column_types_map_to_fields, infer_records_schema},
    ArrowSnafu, JsonParseOptions, StdIOSnafu,
};
use daft_compression::CompressionCodec;

#[derive(Debug, Clone)]
pub struct JsonReadStats {
    pub total_bytes_read: usize,
    pub total_records_read: usize,
    pub mean_record_size_bytes: f64,
    pub stddev_record_size_bytes: f64,
}

impl JsonReadStats {
    pub fn new(
        total_bytes_read: usize,
        total_records_read: usize,
        mean_record_size_bytes: f64,
        stddev_record_size_bytes: f64,
    ) -> Self {
        Self {
            total_bytes_read,
            total_records_read,
            mean_record_size_bytes,
            stddev_record_size_bytes,
        }
    }
}

impl Default for JsonReadStats {
    fn default() -> Self {
        Self::new(0, 0, 0f64, 0f64)
    }
}

pub fn read_json_schema(
    uri: &str,
    parse_options: Option<JsonParseOptions>,
    max_bytes: Option<usize>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<Schema> {
    let runtime_handle = get_runtime(true)?;
    let _rt_guard = runtime_handle.enter();
    runtime_handle.block_on(async {
        read_json_schema_single(
            uri,
            parse_options.unwrap_or_default(),
            // Default to 1 MiB.
            max_bytes.or(Some(1024 * 1024)),
            io_client,
            io_stats,
        )
        .await
    })
}

pub async fn read_json_schema_bulk(
    uris: &[&str],
    parse_options: Option<JsonParseOptions>,
    max_bytes: Option<usize>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    num_parallel_tasks: usize,
) -> DaftResult<Vec<Schema>> {
    let runtime_handle = get_runtime(true)?;
    let _rt_guard = runtime_handle.enter();
    let result = runtime_handle
        .block_on(async {
            let task_stream = futures::stream::iter(uris.iter().map(|uri| {
                let owned_string = uri.to_string();
                let owned_client = io_client.clone();
                let owned_io_stats = io_stats.clone();
                let owned_parse_options = parse_options.clone();
                tokio::spawn(async move {
                    read_json_schema_single(
                        &owned_string,
                        owned_parse_options.unwrap_or_default(),
                        max_bytes,
                        owned_client,
                        owned_io_stats,
                    )
                    .await
                })
            }));
            task_stream
                .buffered(num_parallel_tasks)
                .try_collect::<Vec<_>>()
                .await
        })
        .context(super::JoinSnafu {})?;
    result.into_iter().collect::<DaftResult<Vec<_>>>()
}

pub(crate) async fn read_json_schema_single(
    uri: &str,
    _: JsonParseOptions,
    max_bytes: Option<usize>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<Schema> {
    let (reader, max_bytes): (Box<dyn AsyncBufRead + Unpin + Send>, Option<usize>) = match io_client
        .single_url_get(uri.to_string(), None, io_stats)
        .await?
    {
        GetResult::File(file) => (
            Box::new(BufReader::new(File::open(file.path).await?)),
            max_bytes,
        ),
        GetResult::Stream(stream, size, ..) => (
            Box::new(StreamReader::new(stream)),
            // Truncate max_bytes to size if both are set.
            max_bytes.map(|m| size.map(|s| m.min(s)).unwrap_or(m)),
        ),
    };
    // If file is compressed, wrap stream in decoding stream.
    let reader: Box<dyn AsyncBufRead + Unpin + Send> = match CompressionCodec::from_uri(uri) {
        Some(compression) => Box::new(tokio::io::BufReader::new(compression.to_decoder(reader))),
        None => reader,
    };
    let arrow_schema = infer_schema(reader, None, max_bytes).await?;
    let schema = Schema::try_from(&arrow_schema)?;
    Ok(schema)
}

async fn infer_schema<R>(
    reader: R,
    max_rows: Option<usize>,
    max_bytes: Option<usize>,
) -> DaftResult<arrow2::datatypes::Schema>
where
    R: tokio::io::AsyncBufRead + Unpin + Send,
{
    let max_records = max_rows.unwrap_or(usize::MAX);
    let max_bytes = max_bytes.unwrap_or(usize::MAX);
    let mut total_bytes = 0;
    // Stream of unparsed JSON string records.
    let line_stream = tokio_stream::wrappers::LinesStream::new(reader.lines());
    let mut schema_stream = line_stream
        .try_take_while(|record| {
            // Terminate scan if we've exceeded our max_bytes threshold with the last-read line.
            if total_bytes >= max_bytes {
                futures::future::ready(Ok(false))
            } else {
                total_bytes += record.len();
                futures::future::ready(Ok(true))
            }
        })
        .take(max_records)
        .map(|record| {
            let mut record = record.context(StdIOSnafu)?;

            // Parse record into a JSON Value, then infer the schema.
            let parsed_record = crate::deserializer::to_value(unsafe { record.as_bytes_mut() })
                .map_err(|e| super::Error::JsonDeserializationError {
                    string: e.to_string(),
                })?;
            infer_records_schema(&parsed_record).context(ArrowSnafu)
        });
    // Collect all inferred dtypes for each column.
    let mut column_types: IndexMap<String, HashSet<arrow2::datatypes::DataType>> = IndexMap::new();
    while let Some(schema) = schema_stream.next().await.transpose()? {
        for field in schema.fields {
            // Get-and-mutate-or-insert.
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
    }
    // Convert column types map to dtype-consolidated column fields.
    let fields = column_types_map_to_fields(column_types);
    Ok(fields.into())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_core::{
        datatypes::{Field, TimeUnit},
        schema::Schema,
        DataType,
    };
    use daft_io::{IOClient, IOConfig};
    use rstest::rstest;

    use super::read_json_schema;

    #[rstest]
    fn test_json_schema_local(
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

        let schema = read_json_schema(file.as_ref(), None, None, io_client.clone(), None)?;
        assert_eq!(
            schema,
            Schema::new(vec![
                Field::new("sepalLength", DataType::Float64),
                Field::new("sepalWidth", DataType::Float64),
                Field::new("petalLength", DataType::Float64),
                Field::new("petalWidth", DataType::Float64),
                Field::new("species", DataType::Utf8),
            ])?,
        );

        Ok(())
    }

    #[rstest]
    fn test_json_schema_local_dtypes() -> DaftResult<()> {
        let file = format!("{}/test/dtypes.jsonl", env!("CARGO_MANIFEST_DIR"),);

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let schema = read_json_schema(file.as_ref(), None, None, io_client, None)?;
        assert_eq!(
            schema,
            Schema::new(vec![
                Field::new("int", DataType::Int64),
                Field::new("float", DataType::Float64),
                Field::new("bool", DataType::Boolean),
                Field::new("str", DataType::Utf8),
                Field::new("null", DataType::Null),
                Field::new("date", DataType::Date),
                // // Time unit should be coarsest granularity found in file, i.e. microseconds.
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
            ])?,
        );

        Ok(())
    }

    #[test]
    fn test_json_schema_local_nulls() -> DaftResult<()> {
        let file = format!("{}/test/iris_tiny_nulls.jsonl", env!("CARGO_MANIFEST_DIR"),);

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;
        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let schema = read_json_schema(file.as_ref(), None, None, io_client.clone(), None)?;
        assert_eq!(
            schema,
            Schema::new(vec![
                Field::new("sepalLength", DataType::Float64),
                Field::new("sepalWidth", DataType::Float64),
                Field::new("petalLength", DataType::Float64),
                Field::new("petalWidth", DataType::Float64),
                Field::new("species", DataType::Utf8),
            ])?,
        );

        Ok(())
    }

    #[test]
    fn test_json_schema_local_conflicting_types_utf8_fallback() -> DaftResult<()> {
        let file = format!(
            "{}/test/iris_tiny_conflicting_dtypes.jsonl",
            env!("CARGO_MANIFEST_DIR"),
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;
        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let schema = read_json_schema(file.as_ref(), None, None, io_client.clone(), None)?;
        assert_eq!(
            schema,
            Schema::new(vec![
                // All conflicting dtypes fall back to UTF8.
                Field::new("sepalLength", DataType::Utf8),
                Field::new("sepalWidth", DataType::Utf8),
                Field::new("petalLength", DataType::Utf8),
                // Float + Int => Float, non-conflicting
                Field::new("petalWidth", DataType::Float64),
                Field::new("species", DataType::Utf8),
            ])?,
        );

        Ok(())
    }

    #[test]
    fn test_json_schema_local_max_bytes() -> DaftResult<()> {
        let file = format!("{}/test/iris_tiny.jsonl", env!("CARGO_MANIFEST_DIR"),);

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;
        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let schema = read_json_schema(file.as_ref(), None, Some(100), io_client.clone(), None)?;
        assert_eq!(
            schema,
            Schema::new(vec![
                Field::new("sepalLength", DataType::Float64),
                Field::new("sepalWidth", DataType::Float64),
                Field::new("petalLength", DataType::Float64),
                Field::new("petalWidth", DataType::Float64),
                Field::new("species", DataType::Utf8),
            ])?,
        );

        Ok(())
    }

    #[rstest]
    fn test_json_schema_s3(
        #[values(
                // Uncompressed
                None,
                // brotli
                Some("br"),
                // bzip2
                Some("bz2"),
                // TODO(Clark): Add deflate compressed JSON file to test data fixtures.
                // deflate
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

        let schema = read_json_schema(file.as_ref(), None, None, io_client.clone(), None)?;
        assert_eq!(
            schema,
            Schema::new(vec![
                Field::new("sepalLength", DataType::Float64),
                Field::new("sepalWidth", DataType::Float64),
                Field::new("petalLength", DataType::Float64),
                Field::new("petalWidth", DataType::Float64),
                Field::new("species", DataType::Utf8),
            ])?
        );

        Ok(())
    }
}
