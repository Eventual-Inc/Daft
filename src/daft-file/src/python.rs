use std::{
    io::{Cursor, Read, Seek, SeekFrom},
    ops::{Deref, DerefMut},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use common_error::DaftError;
use daft_core::file::FileReference;
use pyo3::{
    exceptions::{PyIOError, PyRuntimeError, PyValueError},
    prelude::*,
};

use crate::file::{DaftFile, FileCursor};

#[pyclass]
#[derive(Clone)]
struct PyFileReference {
    inner: Arc<FileReference>,
}

#[pymethods]
impl PyFileReference {
    #[staticmethod]
    fn _from_tuple(tuple: Bound<'_, PyAny>) -> PyResult<Self> {
        let f: FileReference = tuple.extract()?;
        Ok(Self { inner: Arc::new(f) })
    }

    pub fn __enter__(&self) -> PyResult<PyDaftFile> {
        Ok(DaftFile::load_blocking(self.inner.as_ref().clone(), true)?.into())
    }

    pub fn _get_file(&self) -> FileReference {
        self.inner.as_ref().clone()
    }

    fn __exit__(
        &mut self,
        _exc_type: Option<Py<PyAny>>,
        _exc_value: Option<Py<PyAny>>,
        _traceback: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        Ok(())
    }

    fn seekable(&self) -> PyResult<bool> {
        Ok(true)
    }

    fn readable(&self) -> PyResult<bool> {
        Ok(true)
    }

    fn isatty(&self) -> PyResult<bool> {
        Ok(false)
    }

    fn writable(&self) -> PyResult<bool> {
        Ok(false)
    }
}

#[pyclass]
struct PyDaftFile {
    inner: DaftFile,
    inside_context: AtomicBool,
}

impl From<DaftFile> for PyDaftFile {
    fn from(inner: DaftFile) -> Self {
        Self {
            inner,
            inside_context: AtomicBool::new(false),
        }
    }
}
impl Deref for PyDaftFile {
    type Target = DaftFile;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl DerefMut for PyDaftFile {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl PyDaftFile {
    fn check_context(&self) -> PyResult<()> {
        if self.inside_context.load(Ordering::SeqCst) {
            return Ok(());
        }
        Err(PyRuntimeError::new_err(
            "File not opened inside a context manager. use `with file.open() as f:`",
        ))
    }
}

#[pymethods]
impl PyDaftFile {
    #[staticmethod]
    fn _from_file_reference(f: PyFileReference) -> PyResult<Self> {
        Ok(DaftFile::load_blocking(f.inner.as_ref().clone(), false)?.into())
    }

    #[pyo3(signature=(size=-1))]
    fn read(&mut self, size: isize) -> PyResult<Vec<u8>> {
        self.check_context()?;
        let cursor = self
            .inner
            .cursor
            .as_mut()
            .ok_or_else(|| PyIOError::new_err("File not open"))?;

        if size == -1 {
            let mut buffer = Vec::new();
            let bytes_read = cursor
                .read_to_end(&mut buffer)
                .map_err(|e| PyIOError::new_err(e.to_string()))?;

            buffer.truncate(bytes_read);
            self.inner.position = bytes_read;

            Ok(buffer)
        } else {
            let mut buffer = vec![0u8; size as usize];

            if self.inner.position == cursor.size() {
                return Ok(vec![]);
            }

            let bytes_read = cursor
                .read(&mut buffer)
                .map_err(|e| PyIOError::new_err(e.to_string()))?;

            buffer.truncate(bytes_read);
            self.position += bytes_read;

            Ok(buffer)
        }
    }

    // Seek to position
    #[pyo3(signature=(offset, whence=Some(0)))]
    fn seek(&mut self, offset: i64, whence: Option<usize>) -> PyResult<u64> {
        self.check_context()?;
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
        slf.inside_context.store(true, Ordering::SeqCst);
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: Option<Py<PyAny>>,
        _exc_value: Option<Py<PyAny>>,
        _traceback: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        self.inside_context.store(false, Ordering::SeqCst);
        self.close()
    }

    // String representation
    fn __str__(&self) -> PyResult<String> {
        Ok("File".to_string())
    }

    fn closed(&self) -> PyResult<bool> {
        Ok(self.cursor.is_none())
    }

    fn _supports_range_requests(&mut self) -> PyResult<bool> {
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

    fn guess_mime_type(&mut self) -> Option<String> {
        self.cursor.as_mut().and_then(|c| c.mime_type())
    }
}

#[pyfunction]
fn guess_mimetype_from_content(mut bytes: Vec<u8>) -> PyResult<Option<String>> {
    let mut cursor = Cursor::new(&mut bytes);
    Ok(crate::guess_mimetype_from_content(&mut cursor)?)
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyDaftFile>()?;
    parent.add_class::<PyFileReference>()?;
    parent.add_function(wrap_pyfunction!(guess_mimetype_from_content, parent)?)?;

    Ok(())
}
