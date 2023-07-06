use std::sync::Arc;

use daft_io::IOClient;
use parquet2::{metadata::FileMetaData, read::deserialize_metadata};
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
    const DEFAULT_FOOTER_READ_SIZE: usize = 64 * 1024;
    let default_end_len = std::cmp::min(DEFAULT_FOOTER_READ_SIZE, size) as usize;

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

    let reader: &[u8] = if (footer_len as usize) < buffer.len() {
        // the whole metadata is in the bytes we already read
        let remaining = buffer.len() - footer_len as usize;
        &buffer[remaining..]
    } else {
        // the end of file read by default is not long enough, read again including the metadata.
        todo!("need more data");
    };
    let max_size = reader.len() * 2 + 1024;
    deserialize_metadata(reader, max_size).context(UnableToParseMetadataSnafu { path: uri })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_io::IOClient;

    use super::read_parquet_metadata;

    #[tokio::test]
    async fn test_parquet_metadata_from_s3() -> crate::Result<()> {
        let io_client = Arc::new(IOClient::default());
        let metadata = read_parquet_metadata(
            "s3://daft-public-data/test_fixtures/parquet-dev/mvp.parquet",
            9882,
            io_client,
        )
        .await?;
        assert_eq!(metadata.num_rows, 100);
        Ok(())
    }
}
