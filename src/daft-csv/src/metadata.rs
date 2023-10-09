use std::sync::Arc;

use arrow2::io::csv::read_async::{infer, infer_schema, AsyncReaderBuilder};
use async_compat::CompatExt;
use common_error::DaftResult;
use daft_core::schema::Schema;
use daft_io::{get_runtime, GetResult, IOClient};
use futures::{io::Cursor, AsyncRead, AsyncSeek};
use tokio::fs::File;

pub fn read_csv_schema(
    uri: &str,
    has_header: bool,
    delimiter: Option<u8>,
    io_client: Arc<IOClient>,
) -> DaftResult<Schema> {
    let runtime_handle = get_runtime(true)?;
    let _rt_guard = runtime_handle.enter();
    runtime_handle
        .block_on(async { read_csv_schema_single(uri, has_header, delimiter, io_client).await })
}

async fn read_csv_schema_single(
    uri: &str,
    has_header: bool,
    delimiter: Option<u8>,
    io_client: Arc<IOClient>,
) -> DaftResult<Schema> {
    match io_client.single_url_get(uri.to_string(), None).await? {
        GetResult::File(file) => {
            read_csv_schema_from_reader(
                File::open(file.path).await?.compat(),
                has_header,
                delimiter,
            )
            .await
        }
        result @ GetResult::Stream(..) => {
            read_csv_schema_from_reader(Cursor::new(result.bytes().await?), has_header, delimiter)
                .await
        }
    }
}

async fn read_csv_schema_from_reader<R>(
    reader: R,
    has_header: bool,
    delimiter: Option<u8>,
) -> DaftResult<Schema>
where
    R: AsyncRead + AsyncSeek + Unpin + Sync + Send,
{
    let mut reader = AsyncReaderBuilder::new()
        .has_headers(has_header)
        .delimiter(delimiter.unwrap_or(b','))
        .create_reader(reader);
    let (fields, _) = infer_schema(&mut reader, None, has_header, &infer).await?;
    let schema: arrow2::datatypes::Schema = fields.into();
    Schema::try_from(&schema)
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

        let schema = read_csv_schema(file, true, None, io_client.clone())?;
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
