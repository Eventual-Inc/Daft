use std::io::{Read, Seek, SeekFrom};

use common_error::DaftError;
use daft_io::python::IOConfig as PyIOConfig;
use pyo3::{
    exceptions::{PyIOError, PyValueError},
    prelude::*,
};

use crate::file::{DaftFile, FileCursor};

#[cfg_attr(feature = "python", pymethods)]
impl DaftFile {
    #[staticmethod]
    #[pyo3(signature = (path, io_config = None))]
    fn _from_path(path: String, io_config: Option<PyIOConfig>) -> PyResult<Self> {
        Ok(Self::from_path(path, io_config.map(|conf| conf.config))?)
    }

    #[staticmethod]
    fn _from_bytes(bytes: Vec<u8>) -> Self {
        Self::from_bytes(bytes)
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

    fn supports_range_requests(&mut self) -> PyResult<bool> {
        let cursor = self
            .cursor
            .as_mut()
            .ok_or_else(|| PyIOError::new_err("File not open"))?;

        // Try to read a single byte from the beginning
        let supports_range = match cursor {
            FileCursor::ObjectReader(reader) => {
                let rt = common_runtime::get_io_runtime(true);
                let inner_reader = reader.get_ref();
                let uri = inner_reader.uri.clone();
                let source = inner_reader.source.clone();

                rt.block_within_async_context(async move {
                    source.supports_range(&uri).await.map_err(DaftError::from)
                })??
            }
            FileCursor::Memory(_) => true,
        };

        Ok(supports_range)
    }

    #[pyo3(name = "size")]
    fn py_size(&mut self) -> PyResult<usize> {
        Ok(self.size()?)
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<DaftFile>()?;

    Ok(())
}
