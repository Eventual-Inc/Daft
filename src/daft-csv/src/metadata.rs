use std::{collections::HashSet, sync::Arc};

use arrow2::io::csv::read_async::{AsyncReader, AsyncReaderBuilder};
use async_compat::CompatExt;
use common_error::DaftResult;
use csv_async::ByteRecord;
use daft_core::schema::Schema;
use daft_io::{get_runtime, GetResult, IOClient, IOStatsRef};
use futures::{StreamExt, TryStreamExt};
use snafu::ResultExt;
use tokio::{
    fs::File,
    io::{AsyncBufRead, AsyncRead, BufReader},
};
use tokio_util::io::StreamReader;

use crate::{schema::merge_schema, CsvParseOptions};
use daft_compression::CompressionCodec;
use daft_decoding::inference::infer;

const DEFAULT_COLUMN_PREFIX: &str = "column_";

#[derive(Debug, Clone)]
pub struct CsvReadStats {
    pub total_bytes_read: usize,
    pub total_records_read: usize,
    pub mean_record_size_bytes: f64,
    pub stddev_record_size_bytes: f64,
}

impl CsvReadStats {
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

impl Default for CsvReadStats {
    fn default() -> Self {
        Self::new(0, 0, 0f64, 0f64)
    }
}

pub fn read_csv_schema(
    uri: &str,
    parse_options: Option<CsvParseOptions>,
    max_bytes: Option<usize>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<(Schema, CsvReadStats)> {
    let runtime_handle = get_runtime(true)?;
    let _rt_guard = runtime_handle.enter();
    runtime_handle.block_on(async {
        read_csv_schema_single(
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

pub async fn read_csv_schema_bulk(
    uris: &[&str],
    parse_options: Option<CsvParseOptions>,
    max_bytes: Option<usize>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    num_parallel_tasks: usize,
) -> DaftResult<Vec<(Schema, CsvReadStats)>> {
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
                    read_csv_schema_single(
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

pub(crate) async fn read_csv_schema_single(
    uri: &str,
    parse_options: CsvParseOptions,
    max_bytes: Option<usize>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<(Schema, CsvReadStats)> {
    let compression_codec = CompressionCodec::from_uri(uri);
    match io_client
        .single_url_get(uri.to_string(), None, io_stats)
        .await?
    {
        GetResult::File(file) => {
            read_csv_schema_from_compressed_reader(
                BufReader::new(File::open(file.path).await?),
                compression_codec,
                parse_options,
                max_bytes,
            )
            .await
        }
        GetResult::Stream(stream, size, ..) => {
            read_csv_schema_from_compressed_reader(
                StreamReader::new(stream),
                compression_codec,
                parse_options,
                // Truncate max_bytes to size if both are set.
                max_bytes.map(|m| size.map(|s| m.min(s)).unwrap_or(m)),
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn read_csv_schema_from_compressed_reader<R>(
    reader: R,
    compression_codec: Option<CompressionCodec>,
    parse_options: CsvParseOptions,
    max_bytes: Option<usize>,
) -> DaftResult<(Schema, CsvReadStats)>
where
    R: AsyncBufRead + Unpin + Send + 'static,
{
    match compression_codec {
        Some(compression) => {
            read_csv_schema_from_uncompressed_reader(
                compression.to_decoder(reader),
                parse_options,
                max_bytes,
            )
            .await
        }
        None => read_csv_schema_from_uncompressed_reader(reader, parse_options, max_bytes).await,
    }
}

#[allow(clippy::too_many_arguments)]
async fn read_csv_schema_from_uncompressed_reader<R>(
    reader: R,
    parse_options: CsvParseOptions,
    max_bytes: Option<usize>,
) -> DaftResult<(Schema, CsvReadStats)>
where
    R: AsyncRead + Unpin + Send,
{
    let (schema, read_stats) =
        read_csv_arrow_schema_from_uncompressed_reader(reader, parse_options, max_bytes).await?;
    Ok((Schema::try_from(&schema)?, read_stats))
}

#[allow(clippy::too_many_arguments)]
async fn read_csv_arrow_schema_from_uncompressed_reader<R>(
    reader: R,
    parse_options: CsvParseOptions,
    max_bytes: Option<usize>,
) -> DaftResult<(arrow2::datatypes::Schema, CsvReadStats)>
where
    R: AsyncRead + Unpin + Send,
{
    let mut reader = AsyncReaderBuilder::new()
        .has_headers(parse_options.has_header)
        .delimiter(parse_options.delimiter)
        .double_quote(parse_options.double_quote)
        .quote(parse_options.quote)
        .escape(parse_options.escape_char)
        .comment(parse_options.comment)
        .buffer_capacity(max_bytes.unwrap_or(1 << 20).min(1 << 20))
        .flexible(parse_options.allow_variable_columns)
        .create_reader(reader.compat());
    let (fields, read_stats) =
        infer_schema(&mut reader, None, max_bytes, parse_options.has_header).await?;
    Ok((fields.into(), read_stats))
}

async fn infer_schema<R>(
    reader: &mut AsyncReader<R>,
    max_rows: Option<usize>,
    max_bytes: Option<usize>,
    has_header: bool,
) -> arrow2::error::Result<(Vec<arrow2::datatypes::Field>, CsvReadStats)>
where
    R: futures::AsyncRead + Unpin + Send,
{
    let mut record = ByteRecord::new();
    // get or create header names
    // when has_header is false, creates default column names with column_ prefix
    let (headers, did_read_record): (Vec<String>, bool) = if has_header {
        (
            reader
                .headers()
                .await?
                .iter()
                .map(|s| s.to_string())
                .collect(),
            false,
        )
    } else {
        // Save the csv reader position before reading headers
        if !reader.read_byte_record(&mut record).await? {
            return Ok((vec![], Default::default()));
        }
        let first_record_count = record.len();
        (
            (0..first_record_count)
                .map(|i| format!("{}{}", DEFAULT_COLUMN_PREFIX, i + 1))
                .collect(),
            true,
        )
    };
    // keep track of inferred field types
    let mut column_types: Vec<HashSet<arrow2::datatypes::DataType>> =
        vec![HashSet::new(); headers.len()];
    let mut records_count = 0;
    let mut total_bytes = 0;
    let mut mean = 0f64;
    let mut m2 = 0f64;
    if did_read_record {
        records_count += 1;
        let record_size = record.as_slice().len();
        total_bytes += record_size;
        let delta = (record_size as f64) - mean;
        mean += delta / (records_count as f64);
        let delta2 = (record_size as f64) - mean;
        m2 += delta * delta2;
        for (i, column) in column_types.iter_mut().enumerate() {
            if let Some(string) = record.get(i) {
                column.insert(infer(string));
            }
        }
    }
    let max_records = max_rows.unwrap_or(usize::MAX);
    let max_bytes = max_bytes.unwrap_or(usize::MAX);
    while records_count < max_records && total_bytes < max_bytes {
        if !reader.read_byte_record(&mut record).await? {
            break;
        }
        records_count += 1;
        let record_size = record.as_slice().len();
        total_bytes += record_size;
        let delta = (record_size as f64) - mean;
        mean += delta / (records_count as f64);
        let delta2 = (record_size as f64) - mean;
        m2 += delta * delta2;
        for (i, column) in column_types.iter_mut().enumerate() {
            if let Some(string) = record.get(i) {
                column.insert(infer(string));
            }
        }
    }
    let fields = merge_schema(&headers, &mut column_types);
    let std = (m2 / ((records_count - 1) as f64)).sqrt();
    Ok((
        fields,
        CsvReadStats::new(total_bytes, records_count, mean, std),
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::{DaftError, DaftResult};
    use daft_core::{datatypes::Field, schema::Schema, DataType};
    use daft_io::{IOClient, IOConfig};
    use rstest::rstest;

    use crate::CsvParseOptions;

    use super::read_csv_schema;

    #[rstest]
    fn test_csv_schema_local(
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

        let (schema, read_stats) =
            read_csv_schema(file.as_ref(), None, None, io_client.clone(), None)?;
        assert_eq!(
            schema,
            Schema::new(vec![
                Field::new("sepal.length", DataType::Float64),
                Field::new("sepal.width", DataType::Float64),
                Field::new("petal.length", DataType::Float64),
                Field::new("petal.width", DataType::Float64),
                Field::new("variety", DataType::Utf8),
            ])?,
        );
        assert_eq!(read_stats.total_bytes_read, 328);
        assert_eq!(read_stats.total_records_read, 20);

        Ok(())
    }

    #[test]
    fn test_csv_schema_local_delimiter() -> DaftResult<()> {
        let file = format!(
            "{}/test/iris_tiny_bar_delimiter.csv",
            env!("CARGO_MANIFEST_DIR"),
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;
        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let (schema, read_stats) = read_csv_schema(
            file.as_ref(),
            Some(CsvParseOptions::default().with_delimiter(b'|')),
            None,
            io_client.clone(),
            None,
        )?;
        assert_eq!(
            schema,
            Schema::new(vec![
                Field::new("sepal.length", DataType::Float64),
                Field::new("sepal.width", DataType::Float64),
                Field::new("petal.length", DataType::Float64),
                Field::new("petal.width", DataType::Float64),
                Field::new("variety", DataType::Utf8),
            ])?,
        );
        assert_eq!(read_stats.total_bytes_read, 328);
        assert_eq!(read_stats.total_records_read, 20);

        Ok(())
    }

    #[test]
    fn test_csv_schema_local_read_stats() -> DaftResult<()> {
        let file = format!("{}/test/iris_tiny.csv", env!("CARGO_MANIFEST_DIR"),);

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;
        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let (_, read_stats) = read_csv_schema(file.as_ref(), None, None, io_client.clone(), None)?;
        assert_eq!(read_stats.total_bytes_read, 328);
        assert_eq!(read_stats.total_records_read, 20);

        Ok(())
    }

    #[test]
    fn test_csv_schema_local_no_headers() -> DaftResult<()> {
        let file = format!(
            "{}/test/iris_tiny_no_headers.csv",
            env!("CARGO_MANIFEST_DIR"),
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;
        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let (schema, read_stats) = read_csv_schema(
            file.as_ref(),
            Some(CsvParseOptions::default().with_has_header(false)),
            None,
            io_client.clone(),
            None,
        )?;
        assert_eq!(
            schema,
            Schema::new(vec![
                Field::new("column_1", DataType::Float64),
                Field::new("column_2", DataType::Float64),
                Field::new("column_3", DataType::Float64),
                Field::new("column_4", DataType::Float64),
                Field::new("column_5", DataType::Utf8),
            ])?,
        );
        assert_eq!(read_stats.total_bytes_read, 328);
        assert_eq!(read_stats.total_records_read, 20);

        Ok(())
    }

    #[test]
    fn test_csv_schema_local_empty_lines_skipped() -> DaftResult<()> {
        let file = format!(
            "{}/test/iris_tiny_empty_lines.csv",
            env!("CARGO_MANIFEST_DIR"),
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;
        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let (schema, read_stats) =
            read_csv_schema(file.as_ref(), None, None, io_client.clone(), None)?;
        assert_eq!(
            schema,
            Schema::new(vec![
                Field::new("sepal.length", DataType::Float64),
                Field::new("sepal.width", DataType::Float64),
                Field::new("petal.length", DataType::Float64),
                Field::new("petal.width", DataType::Float64),
                Field::new("variety", DataType::Utf8),
            ])?,
        );
        assert_eq!(read_stats.total_bytes_read, 49);
        assert_eq!(read_stats.total_records_read, 3);

        Ok(())
    }

    #[test]
    fn test_csv_schema_local_nulls() -> DaftResult<()> {
        let file = format!("{}/test/iris_tiny_nulls.csv", env!("CARGO_MANIFEST_DIR"),);

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;
        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let (schema, read_stats) =
            read_csv_schema(file.as_ref(), None, None, io_client.clone(), None)?;
        assert_eq!(
            schema,
            Schema::new(vec![
                Field::new("sepal.length", DataType::Float64),
                Field::new("sepal.width", DataType::Float64),
                Field::new("petal.length", DataType::Float64),
                Field::new("petal.width", DataType::Float64),
                Field::new("variety", DataType::Utf8),
            ])?,
        );
        assert_eq!(read_stats.total_bytes_read, 82);
        assert_eq!(read_stats.total_records_read, 6);

        Ok(())
    }

    #[test]
    fn test_csv_schema_local_conflicting_types_utf8_fallback() -> DaftResult<()> {
        let file = format!(
            "{}/test/iris_tiny_conflicting_dtypes.csv",
            env!("CARGO_MANIFEST_DIR"),
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;
        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let (schema, read_stats) =
            read_csv_schema(file.as_ref(), None, None, io_client.clone(), None)?;
        assert_eq!(
            schema,
            Schema::new(vec![
                // All conflicting dtypes fall back to UTF8.
                Field::new("sepal.length", DataType::Utf8),
                Field::new("sepal.width", DataType::Utf8),
                Field::new("petal.length", DataType::Utf8),
                Field::new("petal.width", DataType::Utf8),
                Field::new("variety", DataType::Utf8),
            ])?,
        );
        assert_eq!(read_stats.total_bytes_read, 33);
        assert_eq!(read_stats.total_records_read, 2);

        Ok(())
    }

    #[test]
    fn test_csv_schema_local_max_bytes() -> DaftResult<()> {
        let file = format!("{}/test/iris_tiny.csv", env!("CARGO_MANIFEST_DIR"),);

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;
        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let (schema, read_stats) =
            read_csv_schema(file.as_ref(), None, Some(100), io_client.clone(), None)?;
        assert_eq!(
            schema,
            Schema::new(vec![
                Field::new("sepal.length", DataType::Float64),
                Field::new("sepal.width", DataType::Float64),
                Field::new("petal.length", DataType::Float64),
                Field::new("petal.width", DataType::Float64),
                Field::new("variety", DataType::Utf8),
            ])?,
        );
        // Max bytes doesn't include header, so add 15 bytes to upper bound.
        assert!(
            read_stats.total_bytes_read <= 100 + 15,
            "{}",
            read_stats.total_bytes_read
        );
        assert!(
            read_stats.total_records_read <= 10,
            "{}",
            read_stats.total_records_read
        );

        Ok(())
    }

    #[test]
    fn test_csv_schema_local_invalid_column_header_mismatch() -> DaftResult<()> {
        let file = format!(
            "{}/test/iris_tiny_invalid_header_cols_mismatch.csv",
            env!("CARGO_MANIFEST_DIR"),
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;
        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let err = read_csv_schema(file.as_ref(), None, None, io_client.clone(), None);
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
    fn test_csv_schema_local_invalid_no_header_variable_num_cols() -> DaftResult<()> {
        let file = format!(
            "{}/test/iris_tiny_invalid_no_header_variable_num_cols.csv",
            env!("CARGO_MANIFEST_DIR"),
        );

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;
        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let err = read_csv_schema(
            file.as_ref(),
            Some(CsvParseOptions::default().with_has_header(false)),
            None,
            io_client.clone(),
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

    #[rstest]
    fn test_csv_schema_s3(
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

        let (schema, _) = read_csv_schema(file.as_ref(), None, None, io_client.clone(), None)?;
        assert_eq!(
            schema,
            Schema::new(vec![
                Field::new("a", DataType::Int64),
                Field::new("b", DataType::Utf8)
            ])?
        );

        Ok(())
    }
}
