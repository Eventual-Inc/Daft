use std::{
    io,
    io::{BufReader, Cursor, Read, Seek, SeekFrom},
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use common_file::FileReference;
use daft_io::{GetRange, IOConfig, IOStatsRef, ObjectSource};

pub struct DaftFile {
    pub(crate) cursor: Option<FileCursor>,
    pub(crate) position: usize,
}

impl TryFrom<FileReference> for DaftFile {
    type Error = DaftError;

    fn try_from(value: FileReference) -> Result<Self, Self::Error> {
        match value {
            FileReference::Reference(path, ioconfig) => {
                Self::from_path(path, ioconfig.map(|cfg| cfg.as_ref().clone()))
            }
            FileReference::Data(items) => Ok(Self::from_bytes(Arc::unwrap_or_clone(items))),
        }
    }
}

impl DaftFile {
    pub fn from_path(path: String, io_conf: Option<IOConfig>) -> DaftResult<Self> {
        let io_client = daft_io::get_io_client(true, io_conf.map(Arc::new).unwrap_or_default())?;
        let rt = common_runtime::get_io_runtime(true);

        let (source, path, file_size, supports_range) =
            rt.block_within_async_context(async move {
                let (source, path) = io_client
                    .get_source_and_path(&path)
                    .await
                    .map_err(DaftError::from)?;
                // getting the size is pretty cheap, so we do it upfront
                // we grab the size upfront so we can use it to determine if we are at the end of the file
                let file_size = source
                    .get_size(&path, None)
                    .await
                    .map_err(|e| DaftError::ComputeError(e.to_string()))?;
                let supports_range = source
                    .supports_range(&path)
                    .await
                    .map_err(|e| DaftError::ComputeError(e.to_string()))?;
                DaftResult::Ok((source, path, file_size, supports_range))
            })??;

        // Default to 16MB buffer
        const DEFAULT_BUFFER_SIZE: usize = 16 * 1024 * 1024;

        let mut reader = ObjectSourceReader::new(source, path, None, file_size);
        if !supports_range || file_size <= DEFAULT_BUFFER_SIZE {
            let mut buf = Vec::with_capacity(file_size);
            reader.read_to_end(&mut buf)?;
            Ok(Self::from_bytes(buf))
        } else {
            // we wrap it in a BufReader so we are not making so many network requests for each byte read
            let buffered_reader = BufReader::with_capacity(DEFAULT_BUFFER_SIZE, reader);

            Ok(Self {
                cursor: Some(FileCursor::ObjectReader(buffered_reader)),
                position: 0,
            })
        }
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self {
            cursor: Some(FileCursor::Memory(Cursor::new(bytes))),
            position: 0,
        }
    }

    pub fn size(&self) -> DaftResult<usize> {
        self.cursor
            .as_ref()
            .map(|c| c.size())
            .ok_or(DaftError::IoError(std::io::Error::other("File not open")))
    }
}

// Simple wrapper around ObjectSource
pub(crate) struct ObjectSourceReader {
    pub(crate) source: Arc<dyn ObjectSource>,
    pub(crate) uri: String,
    pub(crate) position: usize,
    pub(crate) io_stats: Option<IOStatsRef>,
    size: usize,
}

impl ObjectSourceReader {
    pub fn new(
        source: Arc<dyn ObjectSource>,
        uri: String,
        io_stats: Option<IOStatsRef>,
        size: usize,
    ) -> Self {
        Self {
            source,
            uri,
            position: 0,
            io_stats,
            size,
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

        let rt = common_runtime::get_io_runtime(true);
        let start = self.position;
        let end = start + buf.len();

        let range = Some(GetRange::Bounded(start..end));
        let source = self.source.clone();
        let uri = self.uri.clone();
        let io_stats = self.io_stats.clone();
        let bytes = rt
            .block_within_async_context(async move {
                match source.get(&uri, range, io_stats.clone()).await {
                    Ok(result) => {
                        let bytes = result.bytes().await.map_err(map_bytes_error)?;
                        Ok(bytes.to_vec())
                    }
                    Err(e) => {
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
                            Ok(bytes.to_vec())
                        } else {
                            Err(map_get_error(e))
                        }
                    }
                }
            })
            .map_err(map_async_error)??;

        if bytes.is_empty() {
            return Ok(0);
        }

        let bytes_to_copy = std::cmp::min(buf.len(), bytes.len());
        buf[..bytes_to_copy].copy_from_slice(&bytes[..bytes_to_copy]);

        self.position += bytes_to_copy;
        Ok(bytes_to_copy)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        let content = self.read_full_content()?;

        if self.position >= content.len() {
            return Ok(0);
        }

        let bytes_to_read = content.len() - self.position;
        buf.extend_from_slice(&content[self.position..]);

        self.position = content.len();

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

impl FileCursor {
    pub fn size(&self) -> usize {
        match self {
            Self::ObjectReader(cursor) => cursor.get_ref().size,
            Self::Memory(cursor) => cursor.get_ref().len(),
        }
    }
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
