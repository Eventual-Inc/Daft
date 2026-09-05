//! Python-based OpenDAL backend.
//!
//! This module provides an [`ObjectSource`] implementation that drives the
//! `opendal` Python package (installed via `pip install "daft[extra-fs]"`)
//! through its `AsyncOperator` API. It is used as a runtime fallback for
//! OpenDAL services that are not compiled into the native build, so the
//! default wheel stays small while still supporting all services that the
//! Python binding ships.
//!
//! Coroutines are driven with a fresh `asyncio` event loop per call, inside a
//! `spawn_blocking` thread: event loops are thread-affine and each blocking
//! call may land on a different thread, so a loop is never shared.

use std::{any::Any, collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream, stream::BoxStream};
use pyo3::{
    prelude::*,
    types::{PyBool, PyBytes, PyDict, PyString, PyTuple},
};

use crate::{
    FileFormat, GetRange,
    multipart::MultipartWriter,
    object_io::{FileMetadata, FileType, GetResult, LSResult, ObjectSource},
    object_store_glob,
    opendal_source::{url_prefix, url_to_opendal_path},
    stats::IOStatsRef,
    stream_utils::io_stats_on_bytestream,
};

/// Upper bound on buffered data for the multipart-writer emulation. The
/// `opendal` Python binding has no multipart/append API, so parts are buffered
/// in memory and flushed as a single write on `complete`. This cap prevents
/// unbounded memory usage on very large uploads.
const MAX_BUFFERED_WRITE_BYTES: usize = 1024 * 1024 * 1024; // 1GB

pub(crate) struct PythonOpenDALSource {
    /// The `opendal.AsyncOperator` instance.
    operator: Py<PyAny>,
    /// The `daft.io.opendal_bridge` helper module.
    bridge: Py<PyAny>,
    scheme: String,
}

impl PythonOpenDALSource {
    pub async fn get_client(
        scheme: &str,
        config: &BTreeMap<String, String>,
    ) -> super::Result<Arc<dyn ObjectSource>> {
        let scheme = scheme.to_string();
        let config = config.clone();
        let (operator, bridge) = tokio::task::spawn_blocking({
            let scheme = scheme.clone();
            move || {
                Python::attach(|py| -> super::Result<(Py<PyAny>, Py<PyAny>)> {
                    let pyerr = |e: PyErr| super::Error::UnableToCreateClient {
                        store: super::SourceType::OpenDAL {
                            scheme: scheme.clone(),
                        },
                        // Embed the PyErr text: the error display omits `source`,
                        // so a bare PyErr would lose the failure message.
                        source: format!("{e}").into(),
                    };
                    let opendal_mod = py
                        .import("opendal")
                        .map_err(|_| super::Error::OpenDALPythonPackageMissing)?;
                    let bridge = py
                        .import("daft.io.opendal_bridge")
                        .map_err(|_| pyerr(PyErr::new::<pyo3::exceptions::PyImportError, _>(
                            "The `daft.io.opendal_bridge` helper module could not be imported. Reinstall Daft to restore it.",
                        )))?;
                    let kwargs = PyDict::new(py);
                    for (k, v) in &config {
                        kwargs.set_item(k.as_str(), v.as_str()).map_err(&pyerr)?;
                    }
                    let operator = opendal_mod
                        .getattr("AsyncOperator")
                        .map_err(&pyerr)?
                        .call((scheme.as_str(),), Some(&kwargs))
                        .map_err(pyerr)?;
                    Ok((operator.unbind(), bridge.unbind().into_any()))
                })
            }
        })
        .await
        .map_err(|e| super::Error::Generic {
            store: super::SourceType::OpenDAL {
                scheme: scheme.clone(),
            },
            source: e.into(),
        })??;

        Ok(Arc::new(Self {
            operator,
            bridge,
            scheme,
        }))
    }

    /// Run `bridge.<fn_name>(...)` on a fresh event loop in a blocking
    /// thread, then post-process the result while the GIL is still held.
    /// `make_payload` builds the full positional-argument tuple (the
    /// operator is passed in as its first element) under the GIL.
    async fn run_bridge<M, P, R>(
        &self,
        uri: &str,
        fn_name: &'static str,
        make_payload: M,
        process: P,
    ) -> super::Result<R>
    where
        M: FnOnce(Python<'_>, &Bound<'_, PyAny>) -> PyResult<Py<PyTuple>> + Send + 'static,
        P: FnOnce(Python<'_>, &Bound<'_, PyAny>) -> PyResult<R> + Send + 'static,
        R: Send + 'static,
    {
        let (operator, bridge) =
            Python::attach(|py| (self.operator.clone_ref(py), self.bridge.clone_ref(py)));
        let uri_owned = uri.to_string();
        let scheme = self.scheme.clone();
        tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                let translate = |e: PyErr| translate_py_err(py, e, &uri_owned, &scheme);
                let args = make_payload(py, operator.bind(py)).map_err(&translate)?;
                let result = bridge
                    .bind(py)
                    .call_method1(fn_name, args.bind(py))
                    .map_err(&translate)?;
                process(py, &result).map_err(&translate)
            })
        })
        .await
        .map_err(|e| super::Error::Generic {
            store: super::SourceType::OpenDAL {
                scheme: self.scheme.clone(),
            },
            source: e.into(),
        })?
    }

    /// Run `bridge.call(operator, method, args, kwargs)`; `make_payload`
    /// builds the positional/keyword arguments under the GIL.
    async fn run_op<M, P, R>(
        &self,
        uri: &str,
        method: &'static str,
        make_payload: M,
        process: P,
    ) -> super::Result<R>
    where
        M: FnOnce(Python<'_>) -> PyResult<(Py<PyTuple>, Py<PyDict>)> + Send + 'static,
        P: FnOnce(Python<'_>, &Bound<'_, PyAny>) -> PyResult<R> + Send + 'static,
        R: Send + 'static,
    {
        self.run_bridge(
            uri,
            "call",
            move |py, operator| {
                let (args, kwargs) = make_payload(py)?;
                PyTuple::new(
                    py,
                    [
                        operator.clone().into_any().unbind(),
                        PyString::new(py, method).into_any().unbind(),
                        args.into_any(),
                        kwargs.into_any(),
                    ],
                )
                .map(|t| t.unbind())
            },
            process,
        )
        .await
    }

    async fn stat_metadata(&self, uri: &str, path: &str) -> super::Result<(bool, u64)> {
        let path_owned = path.to_string();
        self.run_op(
            uri,
            "stat",
            move |py| {
                let args = PyTuple::new(py, [path_owned.as_str()])?;
                Ok((args.unbind(), PyDict::new(py).unbind()))
            },
            |_py, result| {
                let is_dir = result.getattr("mode")?.call_method0("is_dir")?.extract()?;
                let content_length = result.getattr("content_length")?.extract()?;
                Ok((is_dir, content_length))
            },
        )
        .await
    }
}

/// Translate known `opendal` Python exceptions into their `daft-io` error
/// equivalents so upstream handling (retries, not-found semantics) keeps
/// working across the Python fallback.
fn translate_py_err(py: Python<'_>, err: PyErr, uri: &str, scheme: &str) -> super::Error {
    let store = super::SourceType::OpenDAL {
        scheme: scheme.to_string(),
    };
    if let Ok(exceptions) = py.import("opendal.exceptions") {
        let class_of = |name: &str| exceptions.getattr(name).ok();
        if let Some(cls) = class_of("NotFound")
            && err.is_instance(py, &cls)
        {
            return super::Error::NotFound {
                path: uri.to_string(),
                source: Box::new(err),
            };
        }
        if let Some(cls) = class_of("PermissionDenied")
            && err.is_instance(py, &cls)
        {
            return super::Error::Unauthorized {
                store,
                path: uri.to_string(),
                source: Box::new(err),
            };
        }
        if let Some(cls) = class_of("RateLimited")
            && err.is_instance(py, &cls)
        {
            return super::Error::Throttled {
                path: uri.to_string(),
                source: Box::new(err),
            };
        }
        if let Some(cls) = class_of("IsADirectory")
            && err.is_instance(py, &cls)
        {
            return super::Error::NotAFile {
                path: uri.to_string(),
            };
        }
    }
    super::Error::Generic {
        store,
        source: if let Some(tb) = err.traceback(py) {
            format!("{err}\n{tb}").into()
        } else {
            Box::new(err)
        },
    }
}

struct PythonOpenDALMultipartWriter {
    source: Arc<PythonOpenDALSource>,
    uri: String,
    parts: Vec<Bytes>,
    total_len: usize,
}

#[async_trait]
impl MultipartWriter for PythonOpenDALMultipartWriter {
    fn part_size(&self) -> usize {
        5 * 1024 * 1024 // 5MB
    }

    async fn put_part(&mut self, data: Bytes) -> super::Result<()> {
        self.total_len += data.len();
        if self.total_len > MAX_BUFFERED_WRITE_BYTES {
            return Err(super::Error::InvalidArgument {
                msg: format!(
                    "Buffered write to '{}' exceeds the {}GB limit of the Python OpenDAL backend. \
                     The `opendal` Python package has no multipart API, so writes are buffered \
                     in memory and flushed as a single write on completion.",
                    self.uri,
                    MAX_BUFFERED_WRITE_BYTES / 1024 / 1024 / 1024,
                ),
            });
        }
        self.parts.push(data);
        Ok(())
    }

    async fn complete(&mut self) -> super::Result<()> {
        let mut all = Vec::with_capacity(self.total_len);
        for part in self.parts.drain(..) {
            all.extend_from_slice(&part);
        }
        self.source.put(&self.uri, Bytes::from(all), None).await
    }
}

#[async_trait]
impl ObjectSource for PythonOpenDALSource {
    async fn supports_range(&self, _uri: &str) -> super::Result<bool> {
        Ok(true)
    }

    async fn create_multipart_writer(
        self: Arc<Self>,
        uri: &str,
    ) -> super::Result<Option<Box<dyn MultipartWriter>>> {
        Ok(Some(Box::new(PythonOpenDALMultipartWriter {
            source: self,
            uri: uri.to_string(),
            parts: Vec::new(),
            total_len: 0,
        })))
    }

    async fn get(
        &self,
        uri: &str,
        range: Option<GetRange>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<GetResult> {
        let path = url_to_opendal_path(uri)?;

        let (offset, size) = match range {
            Some(GetRange::Bounded(r)) => (Some(r.start as u64), Some((r.end - r.start) as u64)),
            Some(GetRange::Offset(offset)) => (Some(offset as u64), None),
            Some(GetRange::Suffix(n)) => {
                let file_size = self.stat_metadata(uri, &path).await?.1;
                let start = file_size.saturating_sub(n as u64);
                (Some(start), Some(file_size - start))
            }
            None => (None, None),
        };

        let bytes = self
            .run_op(
                uri,
                "read",
                move |py| {
                    let args = PyTuple::new(py, [path.as_str()])?;
                    let kwargs = PyDict::new(py);
                    if let Some(offset) = offset {
                        kwargs.set_item("offset", offset)?;
                    }
                    if let Some(size) = size {
                        kwargs.set_item("size", size)?;
                    }
                    Ok((args.unbind(), kwargs.unbind()))
                },
                |_py, result| result.extract::<Vec<u8>>().map(Bytes::from),
            )
            .await?;

        let stream = Box::pin(stream::iter([Ok(bytes)]));
        let stream_with_stats = io_stats_on_bytestream(stream, io_stats);
        Ok(GetResult::Stream(
            stream_with_stats,
            size.map(|s| s as usize),
            None,
            None,
        ))
    }

    async fn put(
        &self,
        uri: &str,
        data: Bytes,
        _io_stats: Option<IOStatsRef>,
    ) -> super::Result<()> {
        let path = url_to_opendal_path(uri)?;
        self.run_op(
            uri,
            "write",
            move |py| {
                let path_arg = path.as_str().into_pyobject(py)?.into_any().unbind();
                let data_arg = PyBytes::new(py, data.as_ref()).into_any().unbind();
                let args = PyTuple::new(py, [path_arg, data_arg])?;
                Ok((args.unbind(), PyDict::new(py).unbind()))
            },
            |_py, _result| Ok(()),
        )
        .await
    }

    async fn get_size(&self, uri: &str, _io_stats: Option<IOStatsRef>) -> super::Result<usize> {
        let path = url_to_opendal_path(uri)?;
        let (is_dir, size) = self.stat_metadata(uri, &path).await?;
        if is_dir {
            return Err(super::Error::NotAFile {
                path: uri.to_string(),
            });
        }
        Ok(size as usize)
    }

    async fn glob(
        self: Arc<Self>,
        glob_path: &str,
        fanout_limit: Option<usize>,
        page_size: Option<i32>,
        limit: Option<usize>,
        io_stats: Option<IOStatsRef>,
        _file_format: Option<FileFormat>,
    ) -> super::Result<BoxStream<'static, super::Result<FileMetadata>>> {
        object_store_glob::glob(self, glob_path, fanout_limit, page_size, limit, io_stats).await
    }

    async fn ls(
        &self,
        path: &str,
        posix: bool,
        continuation_token: Option<&str>,
        _page_size: Option<i32>,
        _io_stats: Option<IOStatsRef>,
    ) -> super::Result<LSResult> {
        let opendal_path = url_to_opendal_path(path)?;

        let dir_path = if opendal_path.is_empty() || opendal_path.ends_with('/') {
            opendal_path
        } else {
            format!("{}/", opendal_path)
        };

        // Same semantics as the native backend: everything is listed in one go,
        // so a continuation token means we are already done.
        if continuation_token.is_some() {
            return Ok(LSResult {
                files: vec![],
                continuation_token: None,
                not_found_if_empty: false,
            });
        }

        let prefix = url_prefix(path)?;
        let dir_path_owned = dir_path.clone();
        let entries: Vec<(String, bool, Option<u64>)> = self
            .run_bridge(
                path,
                "list_entries",
                move |py, operator| {
                    PyTuple::new(
                        py,
                        [
                            operator.clone().into_any().unbind(),
                            PyString::new(py, &dir_path_owned).into_any().unbind(),
                            PyBool::new(py, !posix).to_owned().into_any().unbind(),
                        ],
                    )
                    .map(|t| t.unbind())
                },
                |_py, result| result.extract(),
            )
            .await?;

        // Skip the directory itself (OpenDAL lists it as an entry).
        let files = entries
            .into_iter()
            .filter(|(entry_path, _, _)| entry_path != &dir_path && !entry_path.is_empty())
            .map(|(entry_path, is_dir, size)| FileMetadata {
                filepath: format!("{prefix}/{entry_path}"),
                size,
                filetype: if is_dir {
                    FileType::Directory
                } else {
                    FileType::File
                },
            })
            .collect();

        Ok(LSResult {
            files,
            continuation_token: None,
            not_found_if_empty: false,
        })
    }

    async fn delete(&self, uri: &str, _io_stats: Option<IOStatsRef>) -> super::Result<()> {
        let path = url_to_opendal_path(uri)?;
        self.run_op(
            uri,
            "delete",
            move |py| {
                let args = PyTuple::new(py, [path.as_str()])?;
                Ok((args.unbind(), PyDict::new(py).unbind()))
            },
            |_py, _result| Ok(()),
        )
        .await
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
}
