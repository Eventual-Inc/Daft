use std::sync::Arc;

use daft_io::IOClient;

use parquet2::{metadata::FileMetaData, read::deserialize_metadata};
use snafu::ResultExt;

use crate::{Error, JoinSnafu, UnableToParseMetadataSnafu};

fn metadata_len(buffer: &[u8], len: usize) -> i32 {
    i32::from_le_bytes(buffer[len - 8..len - 4].try_into().unwrap())
}

pub async fn read_parquet_metadata(
    uri: &str,
    size: usize,
    io_client: Arc<IOClient>,
) -> super::Result<FileMetaData> {
    const FOOTER_SIZE: usize = 8;
    const PARQUET_MAGIC: [u8; 4] = [b'P', b'A', b'R', b'1'];

    /// The number of bytes read at the end of the parquet file on first read
    const DEFAULT_FOOTER_READ_SIZE: usize = 128 * 1024;
    let default_end_len = std::cmp::min(DEFAULT_FOOTER_READ_SIZE, size);

    let start = size.saturating_sub(default_end_len);
    let mut data = io_client
        .single_url_get(uri.into(), Some(start..size))
        .await?
        .bytes()
        .await?;

    let buffer = data.as_ref();
    if buffer[buffer.len() - 4..] != PARQUET_MAGIC {
        return Err(Error::InvalidParquetFile {
            path: uri.into(),
            footer: buffer[buffer.len() - 4..].into(),
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

    let remaining;
    if footer_len < buffer.len() {
        // the whole metadata is in the bytes we already read
        remaining = buffer.len() - footer_len;
    } else {
        // the end of file read by default is not long enough, read again including the metadata.

        let start = size.saturating_sub(footer_len);
        data = io_client
            .single_url_get(uri.into(), Some(start..size))
            .await?
            .bytes()
            .await?;
        remaining = data.len() - footer_len;
    };

    let buffer = data.as_ref();

    if buffer[buffer.len() - 4..] != PARQUET_MAGIC {
        return Err(Error::InvalidParquetFile {
            path: uri.into(),
            footer: buffer[buffer.len() - 4..].into(),
        });
    }
    // use rayon here
    tokio::task::spawn_blocking(move || {
        let reader = &data.as_ref()[remaining..];
        let max_size = reader.len() * 2 + 1024;
        deserialize_metadata(reader, max_size)
    })
    .await
    .context(JoinSnafu {
        path: uri.to_string(),
    })?
    .context(UnableToParseMetadataSnafu { path: uri })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_io::{IOClient, IOConfig};

    use super::read_parquet_metadata;

    #[tokio::test]
    async fn test_parquet_metadata_from_s3() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/parquet-dev/mvp.parquet";
        let size = 9882;

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;
        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let metadata = read_parquet_metadata(file, size, io_client.clone()).await?;
        assert_eq!(metadata.num_rows, 100);

        Ok(())
    }
}
