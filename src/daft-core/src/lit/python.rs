use std::{
    fs::File,
    io::{Cursor, Read, Seek, SeekFrom},
    path::Path,
};

use common_error::DaftError;
use daft_schema::prelude::TimeUnit;
use indexmap::IndexMap;
use pyo3::{
    exceptions::{PyIOError, PyValueError},
    intern,
    prelude::*,
    types::PyNone,
    IntoPyObjectExt,
};

use super::Literal;
use crate::{lit::DaftFile, python::PySeries, utils::display::display_decimal128};

#[pyclass]
pub struct PyDaftFile {
    inner: DaftFile,
    cursor: Option<FileCursor>,
    position: usize,
}

enum FileCursor {
    Memory(Cursor<Vec<u8>>),
    File(std::fs::File),
}

impl PyDaftFile {
    // Helper to ensure file is opened
    fn ensure_opened(&mut self) -> PyResult<()> {
        if self.cursor.is_none() {
            match &self.inner {
                DaftFile::Reference(path) => {
                    let file = File::open(Path::new(path)).map_err(|e| {
                        PyIOError::new_err(format!("Failed to open file {}: {}", path, e))
                    })?;
                    self.cursor = Some(FileCursor::File(file));
                }
                DaftFile::Data(data) => {
                    let cursor = Cursor::new(data.clone());
                    self.cursor = Some(FileCursor::Memory(cursor));
                }
            }
        }
        Ok(())
    }
}
#[pymethods]
impl PyDaftFile {
    #[staticmethod]
    fn from_reference(path: String) -> PyResult<Self> {
        let path = path.replace("file://", "");

        let file = File::open(Path::new(&path))
            .map_err(|e| PyIOError::new_err(format!("Failed to open file {}: {}", path, e)))?;
        Ok(Self {
            inner: DaftFile::Reference(path),
            cursor: Some(FileCursor::File(file)),
            position: 0,
        })
    }
    #[staticmethod]
    fn from_bytes(bytes: Vec<u8>) -> Self {
        Self {
            inner: DaftFile::Data(bytes.clone()), // todo: really bad clone here
            cursor: Some(FileCursor::Memory(Cursor::new(bytes))),
            position: 0,
        }
    }

    // Read bytes from file
    fn read(&mut self, size: Option<isize>) -> PyResult<Vec<u8>> {
        self.ensure_opened()?;

        let size = match size {
            Some(s) if s >= 0 => s as usize,
            Some(_) => usize::MAX, // -1 means read all
            None => usize::MAX,
        };

        let cursor = self.cursor.as_mut().unwrap();
        let mut buffer = vec![0u8; size];

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

    // Seek to position
    fn seek(&mut self, offset: i64, whence: Option<usize>) -> PyResult<u64> {
        self.ensure_opened()?;

        let whence = match whence.unwrap_or(0) {
            0 => SeekFrom::Start(offset as u64),
            1 => SeekFrom::Current(offset),
            2 => SeekFrom::End(offset),
            _ => return Err(PyValueError::new_err("Invalid whence value")),
        };

        let cursor = self.cursor.as_mut().unwrap();
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
        match &self.inner {
            DaftFile::Reference(path) => Ok(path.clone()),
            DaftFile::Data(_) => Err(PyValueError::new_err(
                "Cannot convert in-memory data to filesystem path",
            )),
        }
    }

    // String representation
    fn __str__(&self) -> PyResult<String> {
        match &self.inner {
            DaftFile::Reference(path) => Ok(path.clone()),
            DaftFile::Data(_) => Ok("<DaftFile: in-memory data>".to_string()),
        }
    }
}

impl<'py> IntoPyObject<'py> for Literal {
    type Target = PyAny;

    type Output = Bound<'py, Self::Target>;

    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        fn div_rem(l: i64, r: i64) -> (i64, i64) {
            (l / r, l % r)
        }

        match self {
            Self::Null => Ok(PyNone::get(py).to_owned().into_any()),
            Self::Boolean(val) => val.into_bound_py_any(py),
            Self::Utf8(val) => val.into_bound_py_any(py),
            Self::Binary(val) => val.into_bound_py_any(py),
            Self::Int8(val) => val.into_bound_py_any(py),
            Self::UInt8(val) => val.into_bound_py_any(py),
            Self::Int16(val) => val.into_bound_py_any(py),
            Self::UInt16(val) => val.into_bound_py_any(py),
            Self::Int32(val) => val.into_bound_py_any(py),
            Self::UInt32(val) => val.into_bound_py_any(py),
            Self::Int64(val) => val.into_bound_py_any(py),
            Self::UInt64(val) => val.into_bound_py_any(py),
            Self::Timestamp(val, time_unit, tz) => {
                let ts = (val as f64) / (time_unit.to_scale_factor() as f64);

                py.import(intern!(py, "datetime"))?
                    .getattr(intern!(py, "datetime"))?
                    .call_method1(intern!(py, "fromtimestamp"), (ts, tz))
            }
            Self::Date(val) => py
                .import(intern!(py, "datetime"))?
                .getattr(intern!(py, "date"))?
                .call_method1(intern!(py, "fromtimestamp"), (val,)),
            Self::Time(val, time_unit) => {
                let (h, m, s, us) = match time_unit {
                    TimeUnit::Nanoseconds => {
                        let (h, rem) = div_rem(val, 60 * 60 * 1_000_000_000);
                        let (m, rem) = div_rem(rem, 60 * 1_000_000_000);
                        let (s, rem) = div_rem(rem, 1_000_000_000);
                        let us = rem / 1_000;
                        (h, m, s, us)
                    }
                    TimeUnit::Microseconds => {
                        let (h, rem) = div_rem(val, 60 * 60 * 1_000_000);
                        let (m, rem) = div_rem(rem, 60 * 1_000_000);
                        let (s, us) = div_rem(rem, 1_000_000);
                        (h, m, s, us)
                    }
                    TimeUnit::Milliseconds => {
                        let (h, rem) = div_rem(val, 60 * 60 * 1_000);
                        let (m, rem) = div_rem(rem, 60 * 1_000);
                        let (s, ms) = div_rem(rem, 1_000);
                        let us = ms * 1_000;
                        (h, m, s, us)
                    }
                    TimeUnit::Seconds => {
                        let (h, rem) = div_rem(val, 60 * 60);
                        let (m, s) = div_rem(rem, 60);
                        (h, m, s, 0)
                    }
                };

                py.import(intern!(py, "datetime"))?
                    .getattr(intern!(py, "time"))?
                    .call1((h, m, s, us))
            }
            Self::Duration(val, time_unit) => {
                let (d, s, us) = match time_unit {
                    TimeUnit::Nanoseconds => {
                        let (d, rem) = div_rem(val, 24 * 60 * 60 * 1_000_000_000);
                        let (s, rem) = div_rem(rem, 1_000_000_000);
                        let us = rem / 1_000;
                        (d, s, us)
                    }
                    TimeUnit::Microseconds => {
                        let (d, rem) = div_rem(val, 24 * 60 * 60 * 1_000_000);
                        let (s, us) = div_rem(rem, 1_000_000);
                        (d, s, us)
                    }
                    TimeUnit::Milliseconds => {
                        let (d, rem) = div_rem(val, 24 * 60 * 60 * 1_000);
                        let (s, ms) = div_rem(rem, 1_000);
                        let us = ms * 1_000;
                        (d, s, us)
                    }
                    TimeUnit::Seconds => {
                        let (d, s) = div_rem(val, 24 * 60 * 60);
                        (d, s, 0)
                    }
                };

                py.import(intern!(py, "datetime"))?
                    .getattr(intern!(py, "timedelta"))?
                    .call1((d, s, us))
            }
            Self::Interval(_) => {
                Err(DaftError::NotImplemented("Interval literal to Python".to_string()).into())
            }
            Self::Float32(val) => val.into_bound_py_any(py),
            Self::Float64(val) => val.into_bound_py_any(py),
            Self::Decimal(val, p, s) => py
                .import(intern!(py, "decimal"))?
                .getattr(intern!(py, "Decimal"))?
                .call1((display_decimal128(val, p, s),)),
            Self::List(series) => py
                .import(intern!(py, "daft.series"))?
                .getattr(intern!(py, "Series"))?
                .getattr(intern!(py, "_from_pyseries"))?
                .call1((PySeries { series },)),
            Self::Python(val) => val.0.as_ref().into_bound_py_any(py),
            Self::Struct(entries) => entries
                .into_iter()
                .map(|(f, v)| (f.name, v))
                .collect::<IndexMap<_, _>>()
                .into_bound_py_any(py),
            Self::File(DaftFile::Data(data)) => PyDaftFile::from_bytes(data).into_bound_py_any(py),
            Self::File(DaftFile::Reference(path)) => PyDaftFile::from_reference(path)
                .expect("Failed to create PyDaftFile from reference")
                .into_bound_py_any(py),
        }
    }
}
