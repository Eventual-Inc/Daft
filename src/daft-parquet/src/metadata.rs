use std::sync::Arc;

use arrow2::io::parquet::read::{column_iter_to_arrays, infer_schema};
use common_error::DaftResult;
use daft_core::{utils::arrow::cast_array_for_daft_if_needed, Series};
use daft_io::IOClient;
use daft_table::Table;
use futures::StreamExt;
use parquet2::{
    metadata::FileMetaData,
    read::{deserialize_metadata, BasicDecompressor, PageReader},
};
use snafu::ResultExt;

use crate::{Error, UnableToOpenFileSnafu, UnableToParseMetadataSnafu, UnableToReadBytesSnafu};

fn metadata_len(buffer: &[u8], len: usize) -> i32 {
    i32::from_le_bytes(buffer[len - 8..len - 4].try_into().unwrap())
}

async fn read_parquet_metadata(
    uri: &str,
    size: usize,
    io_client: Arc<IOClient>,
) -> super::Result<FileMetaData> {
    const HEADER_SIZE: usize = PARQUET_MAGIC.len();
    const FOOTER_SIZE: usize = 8;
    const PARQUET_MAGIC: [u8; 4] = [b'P', b'A', b'R', b'1'];

    /// The number of bytes read at the end of the parquet file on first read
    const DEFAULT_FOOTER_READ_SIZE: usize = 1024 * 1024;
    let default_end_len = std::cmp::min(DEFAULT_FOOTER_READ_SIZE, size);

    let start = size.saturating_sub(default_end_len);
    let data = io_client
        .single_url_get(uri.into(), Some(start..size))
        .await
        .context(UnableToOpenFileSnafu { path: uri })?
        .bytes()
        .await
        .context(UnableToReadBytesSnafu { path: uri })?;

    let buffer = data.as_ref();
    if buffer[default_end_len - 4..] != PARQUET_MAGIC {
        return Err(Error::InvalidParquetFile {
            path: uri.into(),
            footer: buffer[default_end_len - 4..].into(),
        });
    }
    let metadata_size = metadata_len(buffer, default_end_len);
    let footer_len = FOOTER_SIZE + metadata_size as usize;

    if footer_len > size {
        return Err(Error::InvalidParquetFooterSize {
            path: uri.into(),
            footer_size: footer_len,
            file_size: size,
        });
    }

    let reader: &[u8] = if footer_len < buffer.len() {
        // the whole metadata is in the bytes we already read
        let remaining = buffer.len() - footer_len;
        &buffer[remaining..]
    } else {
        // the end of file read by default is not long enough, read again including the metadata.
        todo!("need more data");
    };
    let max_size = reader.len() * 2 + 1024;
    deserialize_metadata(reader, max_size).context(UnableToParseMetadataSnafu { path: uri })
}

async fn read_row_group(
    uri: &str,
    row_group: usize,
    metadata: &FileMetaData,
    io_client: Arc<IOClient>,
) -> DaftResult<Table> {
    let rg = metadata.row_groups.get(row_group).unwrap();

    let columns = rg.columns();

    let arrow_schema = infer_schema(metadata).unwrap();
    let daft_schema = daft_core::schema::Schema::try_from(&arrow_schema).unwrap();
    let mut daft_series = Vec::with_capacity(columns.len());
    for (ii, col) in columns.iter().enumerate() {
        let (start, len) = col.byte_range();
        let end = start + len;

        // should be async
        let get_result = io_client
            .single_url_get(uri.into(), Some(start as usize..end as usize))
            .await
            .unwrap();

        // // should stream this instead
        let bytes = get_result.bytes().await.unwrap();
        let buffer = bytes.to_vec();
        let pages = PageReader::new(
            std::io::Cursor::new(buffer),
            col,
            Arc::new(|_, _| true),
            vec![],
            4 * 1024 * 1024,
        );

        let decom = BasicDecompressor::new(pages, vec![]);
        let ptype = &col.descriptor().descriptor.primitive_type;

        let field = &arrow_schema.fields[ii];
        let arr_iter = column_iter_to_arrays(
            vec![decom],
            vec![ptype],
            field.clone(),
            None,
            col.num_values() as usize,
        )
        .unwrap();
        let all_arrays = arr_iter.collect::<arrow2::error::Result<Vec<_>>>().unwrap();
        let ser = all_arrays
            .into_iter()
            .map(|a| Series::try_from((field.name.as_str(), cast_array_for_daft_if_needed(a))))
            .collect::<DaftResult<Vec<Series>>>()
            .unwrap();

        let series = Series::concat(ser.iter().collect::<Vec<_>>().as_ref()).unwrap();
        daft_series.push(series);
    }
    Table::new(daft_schema, daft_series)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_io::IOClient;

    use super::{read_parquet_metadata, read_row_group};

    #[tokio::test]
    async fn test_parquet_metadata_from_s3() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/parquet-dev/mvp.parquet";
        let size = 9882;
        let io_client = Arc::new(IOClient::default());
        let metadata = read_parquet_metadata(file, size, io_client.clone()).await?;
        // assert_eq!(metadata.num_rows, 100);

        println!(
            "{}",
            read_row_group(file, 0, &metadata, io_client.clone()).await?
        );

        Ok(())
    }
}
