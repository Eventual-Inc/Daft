use std::{
    io,
    io::{BufReader, Cursor, Read, Seek, SeekFrom},
    sync::Arc,
};

use common_error::DaftError;
use daft_io::{GetRange, IOStatsRef, ObjectSource, python::IOConfig};
use pyo3::{
    exceptions::{PyIOError, PyValueError},
    prelude::*,
};

#[pyclass]
pub struct PyDaftFile {
    path: Option<String>,
    cursor: Option<FileCursor>,
    position: usize,
}

enum FileCursor {
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

#[pymethods]
impl PyDaftFile {
    #[staticmethod]
    #[pyo3(signature = (path, io_config = None))]
    fn _from_path(path: String, io_config: Option<IOConfig>) -> PyResult<Self> {
        let io_config = io_config.unwrap_or_default();

        let io_client = daft_io::get_io_client(true, Arc::new(io_config.config))?;
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

    #[staticmethod]
    fn _from_bytes(bytes: Vec<u8>) -> Self {
        Self {
            path: None,
            cursor: Some(FileCursor::Memory(Cursor::new(bytes))),
            position: 0,
        }
    }

    #[pyo3(signature=(size=-1))]
    fn read(&mut self, size: isize) -> PyResult<Vec<u8>> {
        let cursor = self
            .cursor
            .as_mut()
            .ok_or_else(|| PyIOError::new_err("File not open"))?;

        if size == -1 {
            let mut buffer = Vec::new();
            let bytes_read = cursor
                .read_to_end(&mut buffer)
                .map_err(|e| PyIOError::new_err(e.to_string()))?;

            buffer.truncate(bytes_read);
            self.position = bytes_read;

            Ok(buffer)
        } else {
            let mut buffer = vec![0u8; size as usize];

            let bytes_read = cursor
                .read(&mut buffer)
                .map_err(|e| PyIOError::new_err(e.to_string()))?;

            buffer.truncate(bytes_read);
            self.position += bytes_read;

            Ok(buffer)
        }
    }

    // Seek to position
    fn seek(&mut self, offset: i64, whence: Option<usize>) -> PyResult<u64> {
        let whence = match whence.unwrap_or(0) {
            0 => {
                if offset < 0 {
                    return Err(PyValueError::new_err("Seek offset cannot be negative"));
                }
                SeekFrom::Start(offset as u64)
            }
            1 => SeekFrom::Current(offset),
            2 => SeekFrom::End(offset),
            _ => return Err(PyValueError::new_err("Invalid whence value")),
        };

        let cursor = self
            .cursor
            .as_mut()
            .ok_or_else(|| PyValueError::new_err("File not open"))?;

        let new_pos = cursor
            .seek(whence)
            .map_err(|e| PyIOError::new_err(e.to_string()))?;

        self.position = new_pos as usize;
        Ok(new_pos)
    }

    // Return current position
    fn tell(&self) -> PyResult<u64> {
        if self.cursor.is_none() {
            return Ok(0);
        }
        Ok(self.position as u64)
    }

    // Close the file
    fn close(&mut self) -> PyResult<()> {
        self.cursor = None;
        self.position = 0;
        Ok(())
    }

    // Context manager support
    fn __enter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: Option<PyObject>,
        _exc_value: Option<PyObject>,
        _traceback: Option<PyObject>,
    ) -> PyResult<()> {
        self.close()
    }

    // String representation
    fn __str__(&self) -> PyResult<String> {
        match &self.path {
            Some(path) => Ok(format!("File({})", path)),
            None => Ok("File(None)".to_string()),
        }
    }

    fn closed(&self) -> PyResult<bool> {
        Ok(self.cursor.is_none())
    }
}

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyDaftFile>()?;

    Ok(())
}

// Simple wrapper around ObjectSource
struct ObjectSourceReader {
    source: Arc<dyn ObjectSource>,
    uri: String,
    position: usize,
    io_stats: Option<IOStatsRef>,
}

impl ObjectSourceReader {
    pub fn new(source: Arc<dyn ObjectSource>, uri: String, io_stats: Option<IOStatsRef>) -> Self {
        Self {
            source,
            uri,
            position: 0,
            io_stats,
        }
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
                let result = source
                    .get(&uri, range, io_stats)
                    .await
                    .map_err(map_get_error)?;
                result.bytes().await.map_err(map_bytes_error)
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
        let rt = common_runtime::get_io_runtime(true);

        let source = self.source.clone();
        let uri = self.uri.clone();
        let io_stats = self.io_stats.clone();

        let size = rt
            .block_within_async_context(async move {
                source
                    .get_size(&uri, io_stats.clone())
                    .await
                    .map_err(map_get_error)
            })
            .map_err(map_async_error)??;

        let source = self.source.clone();
        let uri = self.uri.clone();
        let io_stats = self.io_stats.clone();
        let position = self.position;

        let bytes = rt
            .block_within_async_context(async move {
                let range = Some(GetRange::Bounded(position..size));

                let result = source
                    .get(&uri, range, io_stats)
                    .await
                    .map_err(map_get_error)?;

                result.bytes().await.map_err(map_bytes_error)
            })
            .map_err(map_async_error)??;

        buf.reserve(bytes.len());

        buf.extend_from_slice(&bytes);

        self.position += bytes.len();

        Ok(bytes.len())
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
fn map_get_error(e: daft_io::Error) -> io::Error {
    io::Error::other(format!("Get failed: {}", e))
}
fn map_bytes_error(e: daft_io::Error) -> io::Error {
    io::Error::other(format!("Bytes failed: {}", e))
}
fn map_async_error(e: DaftError) -> io::Error {
    io::Error::other(format!("Async context failed: {}", e))
}
