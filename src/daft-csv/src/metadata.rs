use std::{collections::HashSet, sync::Arc};

use arrow2::io::csv::read_async::{infer, AsyncReader, AsyncReaderBuilder};
use async_compat::CompatExt;
use common_error::DaftResult;
use csv_async::ByteRecord;
use daft_core::schema::Schema;
use daft_io::{get_runtime, GetResult, IOClient, IOStatsRef};
use futures::AsyncRead;
use tokio::fs::File;
use tokio_util::io::StreamReader;

pub fn read_csv_schema(
    uri: &str,
    has_header: bool,
    delimiter: Option<u8>,
    max_bytes: Option<usize>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<Schema> {
    let runtime_handle = get_runtime(true)?;
    let _rt_guard = runtime_handle.enter();
    runtime_handle.block_on(async {
        let (schema, _, _) = read_csv_schema_single(
            uri,
            has_header,
            delimiter,
            // Default to 1 MiB.
            max_bytes.or(Some(1 << 20)),
            io_client,
            io_stats,
        )
        .await?;
        Ok(schema)
    })
}

pub(crate) async fn read_csv_schema_single(
    uri: &str,
    has_header: bool,
    delimiter: Option<u8>,
    max_bytes: Option<usize>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<(Schema, usize, usize)> {
    match io_client
        .single_url_get(uri.to_string(), None, io_stats)
        .await?
    {
        GetResult::File(file) => {
            read_csv_schema_from_reader(
                File::open(file.path).await?.compat(),
                has_header,
                delimiter,
                max_bytes,
            )
            .await
        }
        GetResult::Stream(stream, size, _) => {
            let stream_reader = StreamReader::new(stream).compat();
            read_csv_schema_from_reader(
                stream_reader,
                has_header,
                delimiter,
                // Truncate max_bytes to size if both are set.
                max_bytes.map(|m| size.map(|s| m.min(s)).unwrap_or(m)),
            )
            .await
        }
    }
}

async fn read_csv_schema_from_reader<R>(
    reader: R,
    has_header: bool,
    delimiter: Option<u8>,
    max_bytes: Option<usize>,
) -> DaftResult<(Schema, usize, usize)>
where
    R: AsyncRead + Unpin + Send,
{
    let (schema, records_count, total_bytes) =
        read_csv_arrow_schema_from_reader(reader, has_header, delimiter, max_bytes).await?;
    Ok((Schema::try_from(&schema)?, records_count, total_bytes))
}

pub(crate) async fn read_csv_arrow_schema_from_reader<R>(
    reader: R,
    has_header: bool,
    delimiter: Option<u8>,
    max_bytes: Option<usize>,
) -> DaftResult<(arrow2::datatypes::Schema, usize, usize)>
where
    R: AsyncRead + Unpin + Send,
{
    let mut reader = AsyncReaderBuilder::new()
        .has_headers(has_header)
        .delimiter(delimiter.unwrap_or(b','))
        .buffer_capacity(max_bytes.unwrap_or(1 << 20).min(1 << 20))
        .create_reader(reader);
    let (fields, records_count, total_bytes) =
        infer_schema(&mut reader, None, max_bytes, has_header, &infer).await?;
    Ok((fields.into(), records_count, total_bytes))
}

pub async fn infer_schema<R, F>(
    reader: &mut AsyncReader<R>,
    max_rows: Option<usize>,
    max_bytes: Option<usize>,
    has_header: bool,
    infer: &F,
) -> arrow2::error::Result<(Vec<arrow2::datatypes::Field>, usize, usize)>
where
    R: AsyncRead + Unpin + Send,
    F: Fn(&[u8]) -> arrow2::datatypes::DataType,
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
            return Ok((vec![], 0, 0));
        }
        let first_record_count = record.len();
        (
            (0..first_record_count)
                .map(|i| format!("column_{}", i + 1))
                .collect(),
            true,
        )
    };
    // keep track of inferred field types
    let mut column_types: Vec<HashSet<arrow2::datatypes::DataType>> =
        vec![HashSet::new(); headers.len()];
    let mut records_count = 0;
    let mut total_bytes = 0;
    if did_read_record {
        records_count += 1;
        total_bytes += record.as_slice().len();
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
        total_bytes += record.as_slice().len();
        for (i, column) in column_types.iter_mut().enumerate() {
            if let Some(string) = record.get(i) {
                column.insert(infer(string));
            }
        }
    }
    let fields = merge_schema(&headers, &mut column_types);
    Ok((fields, records_count, total_bytes))
}

fn merge_fields(
    field_name: &str,
    possibilities: &mut HashSet<arrow2::datatypes::DataType>,
) -> arrow2::datatypes::Field {
    use arrow2::datatypes::DataType;

    if possibilities.len() > 1 {
        // Drop nulls from possibilities.
        possibilities.remove(&DataType::Null);
    }
    // determine data type based on possible types
    // if there are incompatible types, use DataType::Utf8
    let data_type = match possibilities.len() {
        1 => possibilities.drain().next().unwrap(),
        2 => {
            if possibilities.contains(&DataType::Int64)
                && possibilities.contains(&DataType::Float64)
            {
                // we have an integer and double, fall down to double
                DataType::Float64
            } else {
                // default to Utf8 for conflicting datatypes (e.g bool and int)
                DataType::Utf8
            }
        }
        _ => DataType::Utf8,
    };
    arrow2::datatypes::Field::new(field_name, data_type, true)
}

fn merge_schema(
    headers: &[String],
    column_types: &mut [HashSet<arrow2::datatypes::DataType>],
) -> Vec<arrow2::datatypes::Field> {
    headers
        .iter()
        .zip(column_types.iter_mut())
        .map(|(field_name, possibilities)| merge_fields(field_name, possibilities))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_core::{datatypes::Field, schema::Schema, DataType};
    use daft_io::{IOClient, IOConfig};

    use super::read_csv_schema;

    #[test]
    fn test_csv_schema_from_s3() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/csv-dev/mvp.csv";

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;
        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let schema = read_csv_schema(file, true, None, None, io_client.clone(), None)?;
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
