use std::{
    io::{Read, Seek, SeekFrom},
    sync::Arc,
};

use common_error::DaftError;
use common_file::FileReference;
use daft_io::python::IOConfig as PyIOConfig;
use pyo3::{
    exceptions::{PyIOError, PyTypeError, PyValueError},
    prelude::*,
    types::{PyBytes, PyString, PyTuple},
};

use crate::file::{DaftFile, FileCursor};

#[cfg_attr(feature = "python", pymethods)]
impl DaftFile {
    pub fn _get_file(&self) -> PyResult<FileReference> {
        match &self.cursor {
            Some(FileCursor::ObjectReader(reader)) => {
                let reader = reader.get_ref();

                Ok(FileReference::Reference(
                    reader.uri.clone(),
                    reader.io_config.clone(),
                ))
            }
            Some(FileCursor::Memory(cursor)) => {
                Ok(FileReference::Data(Arc::new(cursor.get_ref().clone())))
            }
            None => Err(PyIOError::new_err("file is not open")),
        }
    }

    #[staticmethod]
    fn _from_tuple(tuple: Bound<'_, PyAny>) -> PyResult<Self> {
        let tuple = tuple.downcast::<PyTuple>()?;
        let first = tuple.get_item(0)?;
        if first.is_instance_of::<PyString>() {
            let url = first.extract::<String>()?;
            let io_config = tuple.get_item(1)?.extract::<Option<PyIOConfig>>()?;
            Self::_from_path(url, io_config)
        } else if first.is_instance_of::<PyBytes>() {
            let data = first.extract::<Vec<u8>>()?;
            Ok(Self::_from_bytes(data))
        } else {
            Err(PyErr::new::<PyTypeError, _>("Expected a string or bytes"))
        }
    }

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
    fn read(&mut self, size: isize) -> PyResult<Option<Vec<u8>>> {
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

            Ok(Some(buffer))
        } else {
            let mut buffer = vec![0u8; size as usize];

            if self.position == cursor.size() {
                return Ok(None);
            }

            let bytes_read = cursor
                .read(&mut buffer)
                .map_err(|e| PyIOError::new_err(e.to_string()))?;

            buffer.truncate(bytes_read);
            self.position += bytes_read;

            Ok(Some(buffer))
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
        Ok("File".to_string())
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
