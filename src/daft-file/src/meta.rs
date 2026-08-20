use common_error::{DaftError, DaftResult};
use daft_core::file::FileReference;

/// Checks whether the file at the given reference exists.
pub async fn file_exists(file_ref: FileReference) -> DaftResult<bool> {
    let io_config = file_ref.io_config.unwrap_or_default();
    let io_client = daft_io::get_io_client(true, io_config)?;

    let (source, path) = io_client
        .get_source_and_path(&file_ref.url)
        .await
        .map_err(DaftError::from)?;

    match source.get_size(&path, None).await {
        Ok(_) => Ok(true),
        Err(daft_io::Error::NotFound { .. } | daft_io::Error::NotAFile { .. }) => Ok(false),
        Err(e) => Err(DaftError::from(e)),
    }
}

/// Blocking version of `file_exists`.
/// Checks whether the file at the given reference exists.
pub fn file_exists_blocking(file_ref: FileReference) -> DaftResult<bool> {
    let rt = common_runtime::get_io_runtime(true);
    rt.block_within_async_context(file_exists(file_ref))
        .flatten()
}
