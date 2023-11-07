use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};

use arrow2::{
    datatypes::Field,
    io::csv::read_async::{read_rows, AsyncReaderBuilder, ByteRecord},
};
use async_compat::{Compat, CompatExt};
use common_error::DaftResult;
use csv_async::AsyncReader;
use daft_core::{
    schema::{Schema, SchemaRef},
    utils::arrow::cast_array_for_daft_if_needed,
    Series,
};
use daft_io::{get_runtime, GetResult, IOClient, IOStatsRef};
use daft_table::Table;
use futures::TryStreamExt;
use rayon::prelude::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};
use snafu::{futures::TryFutureExt, ResultExt};
use tokio::{
    fs::File,
    io::{AsyncBufRead, AsyncRead, BufReader},
};
use tokio_util::io::StreamReader;

use crate::metadata::read_csv_schema_single;
use crate::{compression::CompressionCodec, ArrowSnafu};
use daft_decoding::deserialize::deserialize_column;

#[allow(clippy::too_many_arguments)]
pub fn read_csv(
    uri: &str,
    column_names: Option<Vec<&str>>,
    include_columns: Option<Vec<&str>>,
    num_rows: Option<usize>,
    has_header: bool,
    delimiter: Option<u8>,
    double_quote: bool,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    multithreaded_io: bool,
    schema: Option<SchemaRef>,
    buffer_size: Option<usize>,
    chunk_size: Option<usize>,
    max_chunks_in_flight: Option<usize>,
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
            delimiter.unwrap_or(b','),
            double_quote,
            io_client,
            io_stats,
            schema,
            buffer_size,
            chunk_size,
            max_chunks_in_flight,
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
    delimiter: u8,
    double_quote: bool,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    schema: Option<SchemaRef>,
    buffer_size: Option<usize>,
    chunk_size: Option<usize>,
    max_chunks_in_flight: Option<usize>,
) -> DaftResult<Table> {
    let (schema, estimated_mean_row_size, estimated_std_row_size) = match schema {
        Some(schema) => (schema.to_arrow()?, None, None),
        None => {
            let (schema, _, _, mean, std) = read_csv_schema_single(
                uri,
                has_header,
                Some(delimiter),
                double_quote,
                // Read at most 1 MiB when doing schema inference.
                Some(1024 * 1024),
                io_client.clone(),
                io_stats.clone(),
            )
            .await?;
            (schema.to_arrow()?, Some(mean), Some(std))
        }
    };
    let compression_codec = CompressionCodec::from_uri(uri);
    match io_client
        .single_url_get(uri.to_string(), None, io_stats)
        .await?
    {
        GetResult::File(file) => {
            read_csv_from_compressed_reader(
                BufReader::new(File::open(file.path).await?),
                compression_codec,
                column_names,
                include_columns,
                num_rows,
                has_header,
                delimiter,
                double_quote,
                schema,
                // Default buffer size of 512 KiB.
                buffer_size.unwrap_or(512 * 1024),
                // Default chunk size of 64 KiB.
                chunk_size.unwrap_or(64 * 1024),
                // Default max chunks in flight is set to 2x the number of cores, which should ensure pipelining of reading chunks
                // with the parsing of chunks on the rayon threadpool.
                max_chunks_in_flight.unwrap_or(
                    std::thread::available_parallelism()
                        .unwrap_or(NonZeroUsize::new(2).unwrap())
                        .checked_mul(2.try_into().unwrap())
                        .unwrap()
                        .try_into()
                        .unwrap(),
                ),
                estimated_mean_row_size,
                estimated_std_row_size,
            )
            .await
        }
        GetResult::Stream(stream, _, _) => {
            read_csv_from_compressed_reader(
                StreamReader::new(stream),
                compression_codec,
                column_names,
                include_columns,
                num_rows,
                has_header,
                delimiter,
                double_quote,
                schema,
                // Default buffer size of 512 KiB.
                buffer_size.unwrap_or(512 * 1024),
                // Default chunk size of 64 KiB.
                chunk_size.unwrap_or(64 * 1024),
                // Default max chunks in flight is set to 2x the number of cores, which should ensure pipelining of reading chunks
                // with the parsing of chunks on the rayon threadpool.
                max_chunks_in_flight.unwrap_or(
                    std::thread::available_parallelism()
                        .unwrap_or(NonZeroUsize::new(2).unwrap())
                        .checked_mul(2.try_into().unwrap())
                        .unwrap()
                        .try_into()
                        .unwrap(),
                ),
                estimated_mean_row_size,
                estimated_std_row_size,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn read_csv_from_compressed_reader<R>(
    reader: R,
    compression_codec: Option<CompressionCodec>,
    column_names: Option<Vec<&str>>,
    include_columns: Option<Vec<&str>>,
    num_rows: Option<usize>,
    has_header: bool,
    delimiter: u8,
    double_quote: bool,
    schema: arrow2::datatypes::Schema,
    buffer_size: usize,
    chunk_size: usize,
    max_chunks_in_flight: usize,
    estimated_mean_row_size: Option<f64>,
    estimated_std_row_size: Option<f64>,
) -> DaftResult<Table>
where
    R: AsyncBufRead + Unpin + Send + 'static,
{
    match compression_codec {
        Some(compression) => {
            read_csv_from_uncompressed_reader(
                compression.to_decoder(reader),
                column_names,
                include_columns,
                num_rows,
                has_header,
                delimiter,
                double_quote,
                schema,
                buffer_size,
                chunk_size,
                max_chunks_in_flight,
                estimated_mean_row_size,
                estimated_std_row_size,
            )
            .await
        }
        None => {
            read_csv_from_uncompressed_reader(
                reader,
                column_names,
                include_columns,
                num_rows,
                has_header,
                delimiter,
                double_quote,
                schema,
                buffer_size,
                chunk_size,
                max_chunks_in_flight,
                estimated_mean_row_size,
                estimated_std_row_size,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn read_csv_from_uncompressed_reader<R>(
    stream_reader: R,
    column_names: Option<Vec<&str>>,
    include_columns: Option<Vec<&str>>,
    num_rows: Option<usize>,
    has_header: bool,
    delimiter: u8,
    double_quote: bool,
    schema: arrow2::datatypes::Schema,
    buffer_size: usize,
    chunk_size: usize,
    max_chunks_in_flight: usize,
    estimated_mean_row_size: Option<f64>,
    estimated_std_row_size: Option<f64>,
) -> DaftResult<Table>
where
    R: AsyncRead + Unpin + Send,
{
    let reader = AsyncReaderBuilder::new()
        .has_headers(has_header)
        .delimiter(delimiter)
        .double_quote(double_quote)
        .buffer_capacity(buffer_size)
        .create_reader(stream_reader.compat());
    let mut fields = schema.fields;
    // Rename fields, if necessary.
    if let Some(column_names) = column_names {
        fields = fields
            .into_iter()
            .zip(column_names.iter())
            .map(|(field, name)| {
                Field::new(*name, field.data_type, field.is_nullable).with_metadata(field.metadata)
            })
            .collect();
    }
    // Read CSV into Arrow2 column chunks.
    let column_chunks = read_into_column_chunks(
        reader,
        fields.clone().into(),
        fields_to_projection_indices(&fields, &include_columns),
        num_rows,
        chunk_size,
        max_chunks_in_flight,
        estimated_mean_row_size,
        estimated_std_row_size,
    )
    .await?;
    // Truncate fields to only contain projected columns.
    if let Some(include_columns) = include_columns {
        let field_map = fields
            .into_iter()
            .map(|field| (field.name.clone(), field))
            .collect::<HashMap<String, Field>>();
        fields = include_columns
            .into_iter()
            .map(|col| field_map[col].clone())
            .collect::<Vec<_>>();
    }
    // Concatenate column chunks and convert into Daft Series.
    // Note that this concatenation is done in parallel on the rayon threadpool.
    let columns_series = column_chunks
        .into_par_iter()
        .zip(&fields)
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
    let schema: arrow2::datatypes::Schema = fields.into();
    let daft_schema = Schema::try_from(&schema)?;
    Table::new(daft_schema, columns_series)
}

#[allow(clippy::too_many_arguments)]
async fn read_into_column_chunks<R>(
    mut reader: AsyncReader<Compat<R>>,
    fields: Arc<Vec<arrow2::datatypes::Field>>,
    projection_indices: Arc<Vec<usize>>,
    num_rows: Option<usize>,
    chunk_size: usize,
    max_chunks_in_flight: usize,
    estimated_mean_row_size: Option<f64>,
    estimated_std_row_size: Option<f64>,
) -> DaftResult<Vec<Vec<Box<dyn arrow2::array::Array>>>>
where
    R: AsyncRead + Unpin + Send,
{
    let num_fields = fields.len();
    let num_rows = num_rows.unwrap_or(usize::MAX);
    let mut estimated_mean_row_size = estimated_mean_row_size.unwrap_or(200f64);
    let mut estimated_std_row_size = estimated_std_row_size.unwrap_or(20f64);
    // Stream of unparsed CSV byte record chunks.
    let read_stream = async_stream::try_stream! {
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
            yield chunk_buffer
        }
    };
    // Parsing stream: we spawn background tokio + rayon tasks so we can pipeline chunk parsing with chunk reading, and
    // we further parse each chunk column in parallel on the rayon threadpool.
    let parse_stream = read_stream.map_ok(|record| {
        let fields = fields.clone();
        let projection_indices = projection_indices.clone();
        tokio::spawn(async move {
            let (send, recv) = tokio::sync::oneshot::channel();
            rayon::spawn(move || {
                let result = (move || {
                    let chunk = projection_indices
                        .par_iter()
                        .map(|idx| {
                            deserialize_column(
                                record.as_slice(),
                                *idx,
                                fields[*idx].data_type().clone(),
                                0,
                            )
                        })
                        .collect::<arrow2::error::Result<Vec<Box<dyn arrow2::array::Array>>>>()?;
                    DaftResult::Ok(chunk)
                })();
                let _ = send.send(result);
            });
            recv.await.context(super::OneShotRecvSnafu {})?
        })
        .context(super::JoinSnafu {})
    });
    // Collect all chunks in chunk x column form.
    let chunks = parse_stream
        // Limit the number of chunks we have in flight at any given time.
        .try_buffered(max_chunks_in_flight)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .collect::<DaftResult<Vec<_>>>()?;
    // Transpose chunk x column into column x chunk.
    let mut column_arrays = vec![Vec::with_capacity(chunks.len()); projection_indices.len()];
    for chunk in chunks.into_iter() {
        for (idx, col) in chunk.into_iter().enumerate() {
            column_arrays[idx].push(col);
        }
    }
    Ok(column_arrays)
}

fn fields_to_projection_indices(
    fields: &Vec<arrow2::datatypes::Field>,
    include_columns: &Option<Vec<&str>>,
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
                    .map(|c| field_name_to_idx[c])
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

    use super::read_csv;

    fn check_equal_local_arrow2(
        path: &str,
        out: &Table,
        has_header: bool,
        delimiter: Option<u8>,
        double_quote: bool,
        column_names: Option<Vec<&str>>,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) {
        let mut reader = ReaderBuilder::new()
            .delimiter(delimiter.unwrap_or(b','))
            .double_quote(double_quote)
            .from_path(path)
            .unwrap();
        let (mut fields, _) = infer_schema(&mut reader, None, has_header, &infer).unwrap();
        if !has_header && let Some(column_names) = column_names {
            fields = fields.into_iter().zip(column_names).map(|(field, name)| arrow2::datatypes::Field::new(name, field.data_type, true).with_metadata(field.metadata)).collect::<Vec<_>>();
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

        let table = read_csv(
            file.as_ref(),
            None,
            None,
            None,
            true,
            None,
            true,
            io_client,
            None,
            true,
            None,
            None,
            None,
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
        if compression.is_none() {
            check_equal_local_arrow2(file.as_ref(), &table, true, None, true, None, None, None);
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
            Some(column_names.clone()),
            None,
            None,
            false,
            None,
            true,
            io_client,
            None,
            true,
            None,
            None,
            None,
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
            None,
            Some(5),
            true,
            Some(b'|'),
            true,
            io_client,
            None,
            true,
            None,
            None,
            None,
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
            Some(b'|'),
            true,
            None,
            None,
            Some(5),
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
            None,
            Some(5),
            true,
            None,
            false,
            io_client,
            None,
            true,
            None,
            None,
            None,
            None,
        )?;
        assert_eq!(table.len(), 5);
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
            Some(5),
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
            None,
            None,
            Some(5),
            true,
            None,
            true,
            io_client,
            None,
            true,
            None,
            None,
            None,
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
        check_equal_local_arrow2(file.as_ref(), &table, true, None, true, None, None, Some(5));

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
            None,
            Some(vec!["petal.length", "petal.width"]),
            None,
            true,
            None,
            true,
            io_client,
            None,
            true,
            None,
            None,
            None,
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
            Some(column_names.clone()),
            Some(vec!["petal.length", "petal.width"]),
            None,
            false,
            None,
            true,
            io_client,
            None,
            true,
            None,
            None,
            None,
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
            None,
            true,
            None,
            true,
            io_client,
            None,
            true,
            None,
            Some(128),
            None,
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
        check_equal_local_arrow2(file.as_ref(), &table, true, None, true, None, None, None);

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
            None,
            true,
            None,
            true,
            io_client,
            None,
            true,
            None,
            None,
            Some(100),
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
        check_equal_local_arrow2(file.as_ref(), &table, true, None, true, None, None, None);

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
            None,
            true,
            None,
            true,
            io_client,
            None,
            true,
            None,
            None,
            None,
            Some(5),
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
        check_equal_local_arrow2(file.as_ref(), &table, true, None, true, None, None, None);

        Ok(())
    }

    #[test]
    fn test_csv_read_local_nulls() -> DaftResult<()> {
        let file = format!("{}/test/iris_tiny_nulls.csv", env!("CARGO_MANIFEST_DIR"),);

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_csv(
            file.as_ref(),
            None,
            None,
            None,
            true,
            None,
            true,
            io_client,
            None,
            true,
            None,
            None,
            None,
            None,
        )?;
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
        check_equal_local_arrow2(file.as_ref(), &table, true, None, true, None, None, None);

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

        let table = read_csv(
            file.as_ref(),
            None,
            None,
            None,
            true,
            None,
            true,
            io_client,
            None,
            true,
            None,
            None,
            None,
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
            None,
            None,
            None,
            true,
            None,
            true,
            io_client,
            None,
            true,
            Some(schema.into()),
            None,
            None,
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

        let table = read_csv(
            file.as_ref(),
            None,
            None,
            None,
            true,
            None,
            true,
            io_client,
            None,
            true,
            None,
            None,
            None,
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
        check_equal_local_arrow2(file.as_ref(), &table, true, None, true, None, None, None);

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
            None,
            None,
            None,
            true,
            None,
            true,
            io_client,
            None,
            true,
            Some(schema.into()),
            None,
            None,
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

        let err = read_csv(
            file.as_ref(),
            None,
            None,
            None,
            true,
            None,
            true,
            io_client,
            None,
            true,
            None,
            None,
            None,
            None,
        );
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
            None,
            None,
            false,
            None,
            true,
            io_client,
            None,
            true,
            None,
            None,
            None,
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

        let table = read_csv(
            file.as_ref(),
            None,
            None,
            None,
            true,
            None,
            true,
            io_client,
            None,
            true,
            None,
            None,
            None,
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
    fn test_csv_read_s3_no_headers() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/csv-dev/mvp_no_header.csv";

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let column_names = vec!["a", "b"];
        let table = read_csv(
            file,
            Some(column_names.clone()),
            None,
            None,
            false,
            None,
            true,
            io_client,
            None,
            true,
            None,
            None,
            None,
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

        let column_names = vec!["a", "b"];
        let table = read_csv(
            file,
            Some(column_names.clone()),
            Some(vec!["b"]),
            None,
            false,
            None,
            true,
            io_client,
            None,
            true,
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

    #[test]
    fn test_csv_read_s3_limit() -> DaftResult<()> {
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
            true,
            io_client,
            None,
            true,
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
    fn test_csv_read_s3_projection() -> DaftResult<()> {
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
            true,
            io_client,
            None,
            true,
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
            None,
            true,
            None,
            true,
            io_client,
            None,
            true,
            None,
            Some(100),
            None,
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
            None,
            true,
            None,
            true,
            io_client,
            None,
            true,
            None,
            None,
            Some(100),
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

        let table = read_csv(
            file,
            None,
            None,
            None,
            true,
            None,
            true,
            io_client,
            None,
            true,
            None,
            None,
            None,
            Some(5),
        )?;
        assert_eq!(table.len(), 5000);

        Ok(())
    }
}
