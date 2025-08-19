use std::{
    fs::File,
    io::{Cursor, Read, Seek, SeekFrom},
    path::Path,
};

use common_error::DaftError;
use pyo3::{
    exceptions::{PyIOError, PyNotImplementedError, PyValueError},
    prelude::*,
};

#[pyclass]
pub struct PyDaftFile {
    path: Option<String>,
    cursor: Option<FileCursor>,
    position: usize,
}

enum FileCursor {
    Memory(Cursor<Vec<u8>>),
    File(std::fs::File),
}

#[pymethods]
impl PyDaftFile {
    #[staticmethod]
    fn _from_path(path: String) -> PyResult<Self> {
        let (source, normalized) = daft_io::parse_url(&path).map_err(DaftError::from)?;
        if !matches!(source, daft_io::SourceType::File) {
            return Err(PyNotImplementedError::new_err(
                "remote protocols not yet supported!",
            ));
        }

        let path = normalized.replace("file://", "");

        let file = File::open(Path::new(&path))
            .map_err(|e| PyIOError::new_err(format!("Failed to open file {}: {}", path, e)))?;
        Ok(Self {
            path: Some(path),
            cursor: Some(FileCursor::File(file)),
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

    // Read bytes from file
    #[pyo3(signature=(size=-1))]
    fn read(&mut self, size: isize) -> PyResult<Vec<u8>> {
        let cursor = self
            .cursor
            .as_mut()
            .ok_or_else(|| PyIOError::new_err("File not open"))?;

        if size == -1 {
            let mut buffer = Vec::new();
            let bytes_read = match cursor {
                FileCursor::Memory(c) => c
                    .read_to_end(&mut buffer)
                    .map_err(|e| PyIOError::new_err(e.to_string()))?,
                FileCursor::File(f) => f
                    .read_to_end(&mut buffer)
                    .map_err(|e| PyIOError::new_err(e.to_string()))?,
            };

            buffer.truncate(bytes_read);
            self.position += bytes_read;

            Ok(buffer)
        } else {
            let mut buffer = vec![0u8; size as usize];

            let bytes_read = match cursor {
                FileCursor::Memory(c) => c
                    .read(&mut buffer)
                    .map_err(|e| PyIOError::new_err(e.to_string()))?,
                FileCursor::File(f) => f
                    .read(&mut buffer)
                    .map_err(|e| PyIOError::new_err(e.to_string()))?,
            };

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
        let new_pos = match cursor {
            FileCursor::Memory(c) => c
                .seek(whence)
                .map_err(|e| PyIOError::new_err(e.to_string()))?,
            FileCursor::File(f) => f
                .seek(whence)
                .map_err(|e| PyIOError::new_err(e.to_string()))?,
        };

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

    // Path protocol support
    fn __fspath__(&self) -> PyResult<String> {
        match &self.path {
            Some(path) => Ok(path.clone()),
            None => Err(PyIOError::new_err("File path not available")),
        }
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
