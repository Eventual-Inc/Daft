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

#[pyclass(from_py_object)]
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

    pub fn __enter__(&self, py: Python<'_>) -> PyResult<PyDaftFile> {
        let file_ref = self.inner.as_ref().clone();
        py.detach(move || Ok(DaftFile::load_blocking(file_ref, true, None)?.into()))
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

    fn path(&self) -> PyResult<String> {
        Ok(self.inner.url.clone())
    }

    fn name(&self) -> PyResult<String> {
        // Extract the filename from the URL/path
        let url = &self.inner.url;

        // Try to parse as URL and extract path component
        if let Ok(parsed) = url::Url::parse(url)
            && let Some(mut segments) = parsed.path_segments()
            && let Some(last) = segments.next_back()
            && !last.is_empty()
        {
            return Ok(last.to_string());
        }

        // Fallback: treat as file path and get basename
        if let Some(last) = url.rsplit('/').next()
            && !last.is_empty()
        {
            return Ok(last.to_string());
        }

        // If all else fails, return the full URL
        Ok(url.clone())
    }

    fn position(&self) -> PyResult<Option<u64>> {
        Ok(self.inner.position)
    }

    fn size(&self) -> PyResult<Option<u64>> {
        Ok(self.inner.size)
    }

    fn exists(&self, py: Python<'_>) -> PyResult<bool> {
        let file_ref = self.inner.as_ref().clone();
        py.detach(move || crate::meta::file_exists_blocking(file_ref).map_err(|e| e.into()))
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
    #[pyo3(signature=(f, buffer_size=None))]
    fn _from_file_reference(py: Python<'_>, f: PyFileReference, buffer_size: Option<usize>) -> PyResult<Self> {
        let file_ref = f.inner.as_ref().clone();
        py.detach(move || Ok(DaftFile::load_blocking(file_ref, false, buffer_size)?.into()))
    }

    #[pyo3(signature=(size=-1))]
    fn read(&mut self, py: Python<'_>, size: isize) -> PyResult<Vec<u8>> {
        self.check_context()?;
        let mut cursor = self
            .inner
            .cursor
            .take()
            .ok_or_else(|| PyIOError::new_err("File not open"))?;
        let current_position = self.inner.position;
        let current_size = cursor.size();

        let result: Result<(Vec<u8>, usize, bool, FileCursor), (PyErr, FileCursor)> = py.detach(move || {
            if size == -1 {
                let mut buffer = Vec::new();
                match cursor.read_to_end(&mut buffer) {
                    Ok(bytes_read) => {
                        buffer.truncate(bytes_read);
                        Ok((buffer, bytes_read, true, cursor))
                    }
                    Err(e) => Err((PyIOError::new_err(e.to_string()), cursor)),
                }
            } else {
                if current_position == current_size {
                    return Ok((vec![], 0usize, false, cursor));
                }
                let mut buffer = vec![0u8; size as usize];
                match cursor.read(&mut buffer) {
                    Ok(bytes_read) => {
                        buffer.truncate(bytes_read);
                        Ok((buffer, bytes_read, false, cursor))
                    }
                    Err(e) => Err((PyIOError::new_err(e.to_string()), cursor)),
                }
            }
        });

        match result {
            Ok((buffer, bytes_read, is_read_all, cursor_back)) => {
                self.inner.cursor = Some(cursor_back);
                if is_read_all {
                    self.inner.position = bytes_read;
                } else {
                    self.inner.position += bytes_read;
                }
                Ok(buffer)
            }
            Err((e, cursor_back)) => {
                self.inner.cursor = Some(cursor_back);
                Err(e)
            }
        }
    }

    #[pyo3(signature=(offset, whence=Some(0)))]
    fn seek(&mut self, py: Python<'_>, offset: i64, whence: Option<usize>) -> PyResult<u64> {
        self.check_context()?;
        let seek_from = match whence.unwrap_or(0) {
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

        let mut cursor = self
            .inner
            .cursor
            .take()
            .ok_or_else(|| PyValueError::new_err("File not open"))?;

        let result: Result<(u64, FileCursor), (PyErr, FileCursor)> = py.detach(move || {
            match cursor.seek(seek_from) {
                Ok(new_pos) => Ok((new_pos, cursor)),
                Err(e) => Err((PyIOError::new_err(e.to_string()), cursor)),
            }
        });

        match result {
            Ok((new_pos, cursor_back)) => {
                self.inner.cursor = Some(cursor_back);
                self.inner.position = new_pos as usize;
                Ok(new_pos)
            }
            Err((e, cursor_back)) => {
                self.inner.cursor = Some(cursor_back);
                Err(e)
            }
        }
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

    fn _supports_range_requests(&mut self, py: Python<'_>) -> PyResult<bool> {
        let cursor = self
            .inner
            .cursor
            .as_mut()
            .ok_or_else(|| PyIOError::new_err("File not open"))?;

        match cursor {
            FileCursor::ObjectReader(reader) => {
                let rt = common_runtime::get_io_runtime(true);
                let inner_reader = reader.get_ref();
                let uri = inner_reader.uri.clone();
                let source = inner_reader.source.clone();

                Ok(py.detach(move || {
                    rt.block_within_async_context(async move {
                        source.supports_range(&uri).await.map_err(DaftError::from)
                    })
                })??)
            }
            FileCursor::Memory(_) => Ok(true),
        }
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
