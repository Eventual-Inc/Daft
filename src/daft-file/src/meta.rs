use common_error::{DaftError, DaftResult};
use daft_core::file::FileReference;

use crate::{DaftFile, file::BUFFER_SIZE_SNIFF};

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

/// Returns the size of the file at the given reference.
///
/// For whole-file references this only performs a metadata request. Byte-range references retain
/// the existing `DaftFile` behavior, where the returned size is the size of the loaded range.
pub async fn file_size(file_ref: FileReference) -> DaftResult<usize> {
    if file_ref.position.is_some() || file_ref.size.is_some() {
        return DaftFile::load(file_ref, false, Some(BUFFER_SIZE_SNIFF))
            .await?
            .size();
    }

    let io_config = file_ref.io_config.unwrap_or_default();
    let io_client = daft_io::get_io_client(true, io_config)?;

    let (source, path) = io_client
        .get_source_and_path(&file_ref.url)
        .await
        .map_err(DaftError::from)?;

    source.get_size(&path, None).await.map_err(DaftError::from)
}

/// Blocking version of `file_exists`.
/// Checks whether the file at the given reference exists.
pub fn file_exists_blocking(file_ref: FileReference) -> DaftResult<bool> {
    let rt = common_runtime::get_io_runtime(true);
    rt.block_within_async_context(file_exists(file_ref))
        .flatten()
}
