use std::{
    io,
    io::{BufReader, Cursor, Read, Seek, SeekFrom},
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use common_file::FileReference;
use daft_io::{GetRange, IOConfig, IOStatsRef, ObjectSource};

#[cfg_attr(feature = "python", pyo3::pyclass(name = "PyDaftFile"))]
pub struct DaftFile {
    pub(crate) path: Option<String>,
    pub(crate) cursor: Option<FileCursor>,
    pub(crate) position: usize,
}

impl TryFrom<FileReference> for DaftFile {
    type Error = DaftError;

    fn try_from(value: FileReference) -> Result<Self, Self::Error> {
        match value {
            FileReference::Reference(path, ioconfig) => Self::from_path(path, *ioconfig),
            FileReference::Data(items) => Ok(Self::from_bytes(items)),
        }
    }
}

impl DaftFile {
    pub fn from_path(path: String, io_config: Option<IOConfig>) -> DaftResult<Self> {
        let io_config = io_config.unwrap_or_default();

        let io_client = daft_io::get_io_client(true, Arc::new(io_config))?;
        let rt = common_runtime::get_io_runtime(true);

        let (source, path) = rt.block_within_async_context(async move {
            io_client
                .get_source_and_path(&path)
                .await
                .map_err(DaftError::from)
        })??;

        // Default to 8MB buffer
        const DEFAULT_BUFFER_SIZE: usize = 8 * 1024 * 1024;

        let reader = ObjectSourceReader::new(source, path.clone(), None);

        // we wrap it in a BufReader so we are not making so many network requests for each byte read
        let buffered_reader = BufReader::with_capacity(DEFAULT_BUFFER_SIZE, reader);

        Ok(Self {
            path: Some(path),
            cursor: Some(FileCursor::ObjectReader(buffered_reader)),
            position: 0,
        })
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self {
            path: None,
            cursor: Some(FileCursor::Memory(Cursor::new(bytes))),
            position: 0,
        }
    }

    pub fn size(&self) -> DaftResult<usize> {
        let cursor = self
            .cursor
            .as_ref()
            .ok_or_else(|| DaftError::ComputeError("File not open".to_string()))?;

        match cursor {
            FileCursor::ObjectReader(reader) => {
                let reader = reader.get_ref();
                let source = reader.source.clone();
                let uri = reader.uri.clone();
                let io_stats = reader.io_stats.clone();

                let rt = common_runtime::get_io_runtime(true);

                let size = rt.block_within_async_context(async move {
                    source
                        .get_size(&uri, io_stats)
                        .await
                        .map_err(|e| DaftError::ComputeError(e.to_string()))
                })??;
                Ok(size)
            }
            FileCursor::Memory(mem_cursor) => Ok(mem_cursor.get_ref().len()),
        }
    }
}

// Simple wrapper around ObjectSource
pub(crate) struct ObjectSourceReader {
    pub(crate) source: Arc<dyn ObjectSource>,
    pub(crate) uri: String,
    pub(crate) position: usize,
    pub(crate) io_stats: Option<IOStatsRef>,
    // Cache for full file content when range requests aren't supported
    cached_content: Option<Vec<u8>>,
    // Flag to track if range requests are supported
    supports_range: Option<bool>,
}

impl ObjectSourceReader {
    pub fn new(source: Arc<dyn ObjectSource>, uri: String, io_stats: Option<IOStatsRef>) -> Self {
        Self {
            source,
            uri,
            position: 0,
            io_stats,
            cached_content: None,
            supports_range: None,
        }
    }
    // Helper to read the entire file content
    fn read_full_content(&self) -> io::Result<Vec<u8>> {
        let rt = common_runtime::get_io_runtime(true);

        let source = self.source.clone();
        let uri = self.uri.clone();
        let io_stats = self.io_stats.clone();

        rt.block_within_async_context(async move {
            let result = source
                .get(&uri, None, io_stats)
                .await
                .map_err(map_get_error)?;

            result
                .bytes()
                .await
                .map(|b| b.to_vec())
                .map_err(map_bytes_error)
        })
        .map_err(map_async_error)
        .flatten()
    }
}

// Implement Read for synchronous reading

impl Read for ObjectSourceReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        // If we have cached content, serve from cache
        if let Some(ref content) = self.cached_content {
            if self.position >= content.len() {
                return Ok(0); // EOF
            }

            let available = content.len() - self.position;
            let bytes_to_read = std::cmp::min(buf.len(), available);

            buf[..bytes_to_read]
                .copy_from_slice(&content[self.position..self.position + bytes_to_read]);
            self.position += bytes_to_read;

            return Ok(bytes_to_read);
        }

        // First time reading, or range support is known
        let rt = common_runtime::get_io_runtime(true);
        let start = self.position;
        let end = start + buf.len();

        // If we already know range requests aren't supported, read full content
        if self.supports_range == Some(false) {
            // Read entire file and cache it
            let content = self.read_full_content()?;

            // Determine how many bytes to return from the full content
            let bytes_to_read = if start < content.len() {
                let end = std::cmp::min(end, content.len());
                let bytes_to_read = end - start;

                // Copy the requested portion to the output buffer
                buf[..bytes_to_read].copy_from_slice(&content[start..end]);

                bytes_to_read
            } else {
                0 // Position is beyond EOF
            };

            // Update position and cache the content
            self.position += bytes_to_read;
            self.cached_content = Some(content);

            return Ok(bytes_to_read);
        }

        // Try range request if support is unknown or known to be supported
        let range = Some(GetRange::Bounded(start..end));
        let source = self.source.clone();
        let uri = self.uri.clone();
        let io_stats = self.io_stats.clone();

        let range_result = rt
            .block_within_async_context(async move {
                match source.get(&uri, range, io_stats.clone()).await {
                    Ok(result) => {
                        let bytes = result.bytes().await.map_err(map_bytes_error)?;
                        Ok((bytes.to_vec(), true)) // Range request succeeded
                    }
                    Err(e) => {
                        // EOF
                        if let daft_io::Error::InvalidRangeRequest {
                            source: daft_io::range::InvalidGetRange::StartTooLarge { .. },
                        } = e
                        {
                            Ok((Vec::new(), true))
                        } else {
                            let error_str = e.to_string();
                            // Check if error suggests range requests aren't supported
                            if error_str.contains("Requested Range Not Satisfiable")
                                || error_str.contains("416")
                            {
                                // Fall back to reading the entire file
                                let result = source
                                    .get(&uri, None, io_stats)
                                    .await
                                    .map_err(map_get_error)?;

                                let bytes = result.bytes().await.map_err(map_bytes_error)?;
                                Ok((bytes.to_vec(), false)) // Range request not supported
                            } else {
                                Err(map_get_error(e))
                            }
                        }
                    }
                }
            })
            .map_err(map_async_error)??;

        let (bytes, supports_range) = range_result;
        self.supports_range = Some(supports_range);

        if !supports_range {
            // Range requests not supported - cache the full content
            let bytes_to_read = if start < bytes.len() {
                let end = std::cmp::min(end, bytes.len());
                let bytes_to_read = end - start;

                // Copy the requested portion to the output buffer
                buf[..bytes_to_read].copy_from_slice(&bytes[start..end]);

                bytes_to_read
            } else {
                0 // Position is beyond EOF
            };

            self.position += bytes_to_read;
            self.cached_content = Some(bytes);

            Ok(bytes_to_read)
        } else {
            // Range requests supported - use the returned bytes directly
            if bytes.is_empty() {
                return Ok(0);
            }

            let bytes_to_copy = std::cmp::min(buf.len(), bytes.len());
            buf[..bytes_to_copy].copy_from_slice(&bytes[..bytes_to_copy]);

            self.position += bytes_to_copy;
            Ok(bytes_to_copy)
        }
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        // If we have cached content, serve from cache
        if let Some(ref content) = self.cached_content {
            if self.position >= content.len() {
                return Ok(0); // EOF
            }

            let bytes_to_read = content.len() - self.position;
            buf.extend_from_slice(&content[self.position..]);

            self.position = content.len();

            return Ok(bytes_to_read);
        }

        let content = self.read_full_content()?;

        if self.position >= content.len() {
            return Ok(0);
        }

        let bytes_to_read = content.len() - self.position;
        buf.extend_from_slice(&content[self.position..]);

        self.cached_content = Some(content);
        self.position = self.cached_content.as_ref().unwrap().len();

        Ok(bytes_to_read)
    }
}

impl Seek for ObjectSourceReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        // Calculate the new position
        let new_position = match pos {
            SeekFrom::Start(offset) => offset as usize,
            SeekFrom::End(offset) => {
                let rt = common_runtime::get_io_runtime(true);

                let source = self.source.clone();
                let uri = self.uri.clone();
                let io_stats = self.io_stats.clone();

                let size = rt
                    .block_within_async_context(async move {
                        source.get_size(&uri, io_stats).await.map_err(map_get_error)
                    })
                    .map_err(map_async_error)??;

                if offset < 0 {
                    size.saturating_sub((-offset) as usize)
                } else {
                    size.saturating_add(offset as usize)
                }
            }
            SeekFrom::Current(offset) => {
                if offset < 0 {
                    self.position.saturating_sub((-offset) as usize)
                } else {
                    self.position.saturating_add(offset as usize)
                }
            }
        };

        // Update position
        self.position = new_position;

        Ok(self.position as u64)
    }
}

pub(crate) enum FileCursor {
    ObjectReader(BufReader<ObjectSourceReader>),
    Memory(std::io::Cursor<Vec<u8>>),
}

impl Read for FileCursor {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::ObjectReader(cursor) => cursor.read(buf),
            Self::Memory(cursor) => cursor.read(buf),
        }
    }
    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        match self {
            Self::ObjectReader(cursor) => cursor.read_to_end(buf),
            Self::Memory(cursor) => cursor.read_to_end(buf),
        }
    }
}

impl Seek for FileCursor {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match self {
            Self::ObjectReader(cursor) => cursor.seek(pos),
            Self::Memory(cursor) => cursor.seek(pos),
        }
    }
}
fn map_get_error(e: daft_io::Error) -> io::Error {
    io::Error::other(format!("Get failed: {}", e))
}
fn map_bytes_error(e: daft_io::Error) -> io::Error {
    io::Error::other(format!("Bytes failed: {}", e))
}
fn map_async_error(e: DaftError) -> io::Error {
    io::Error::other(format!("Async context failed: {}", e))
}
