use std::{
    any::Any,
    collections::HashMap,
    sync::{Arc, LazyLock, RwLock},
};

use async_trait::async_trait;
use bytes::Bytes;
use common_error::DaftError;
use common_file_formats::FileFormat;
use futures::stream::{self, BoxStream};
use pyo3::{
    exceptions::{
        PyFileNotFoundError, PyIsADirectoryError, PyNotImplementedError, PyPermissionError,
    },
    prelude::*,
    types::PyBytes,
};

use crate::{
    SourceType,
    multipart::MultipartWriter,
    object_io::{FileMetadata, FileType, GetResult, LSResult, ObjectSource},
    object_store_glob,
    range::GetRange,
    stats::IOStatsRef,
    stream_utils::io_stats_on_bytestream,
};

const MAX_BUFFERED_WRITE_BYTES: usize = 1024 * 1024 * 1024;

type ExtensionCacheKey = (SourceType, Vec<u8>);

static EXTENSION_CACHE: LazyLock<RwLock<HashMap<ExtensionCacheKey, Arc<PythonSource>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

pub struct PythonSource {
    extension: Py<PyAny>,
    source_type: SourceType,
}

impl PythonSource {
    pub fn from_serialized(serialized: &[u8], source_type: SourceType) -> super::Result<Arc<Self>> {
        let cache_key = (source_type.clone(), serialized.to_vec());
        if let Some(source) = EXTENSION_CACHE.read().unwrap().get(&cache_key) {
            return Ok(source.clone());
        }

        let extension =
            Python::attach(|py| common_py_serde::pickle_loads(py, serialized).map(Bound::unbind))
                .map_err(|source| super::Error::UnableToCreateClient {
                store: source_type.clone(),
                source: Box::new(source),
            })?;
        let source = Arc::new(Self {
            extension,
            source_type,
        });
        let mut cache = EXTENSION_CACHE.write().unwrap();
        Ok(cache.entry(cache_key).or_insert(source).clone())
    }

    pub fn gravitino(config: &common_io_config::GravitinoConfig) -> super::Result<Arc<Self>> {
        let source_type = SourceType::Gravitino;
        let extension = Python::attach(|py| {
            let module = py.import(pyo3::intern!(py, "daft.io.gravitino_filesystem"))?;
            let py_config = common_io_config::python::GravitinoConfig {
                config: config.clone(),
            };
            module
                .getattr(pyo3::intern!(py, "GravitinoIOExtension"))?
                .call1((py_config,))
                .map(Bound::unbind)
        })
        .map_err(|source| super::Error::UnableToCreateClient {
            store: source_type.clone(),
            source: Box::new(source),
        })?;
        Ok(Arc::new(Self {
            extension,
            source_type,
        }))
    }

    pub fn source_type(&self) -> &SourceType {
        &self.source_type
    }

    pub async fn resolve_url_and_config(
        &self,
        path: &str,
    ) -> super::Result<(String, common_io_config::IOConfig)> {
        let extension = Python::attach(|py| self.extension.clone_ref(py));
        let path_owned = path.to_string();
        let result = common_runtime::python::execute_python_coroutine::<
            _,
            (String, common_io_config::python::IOConfig),
        >(move |py| {
            Ok(extension
                .call_method1(py, pyo3::intern!(py, "resolve_url"), (path_owned,))?
                .into_bound(py))
        })
        .await
        .map_err(|error| self.translate_error(path, error))?;
        Ok((result.0, result.1.config))
    }

    fn translate_error(&self, path: &str, error: DaftError) -> super::Error {
        match error {
            DaftError::PyO3Error(error) => Python::attach(|py| {
                if error.is_instance_of::<PyFileNotFoundError>(py) {
                    super::Error::NotFound {
                        path: path.to_string(),
                        source: Box::new(error),
                    }
                } else if error.is_instance_of::<PyPermissionError>(py) {
                    super::Error::Unauthorized {
                        store: self.source_type.clone(),
                        path: path.to_string(),
                        source: Box::new(error),
                    }
                } else if error.is_instance_of::<PyIsADirectoryError>(py) {
                    super::Error::NotAFile {
                        path: path.to_string(),
                    }
                } else if error.is_instance_of::<PyNotImplementedError>(py) {
                    super::Error::NotImplementedMethod {
                        method: error.to_string(),
                    }
                } else {
                    super::Error::Generic {
                        store: self.source_type.clone(),
                        source: Box::new(error),
                    }
                }
            }),
            error => super::Error::Generic {
                store: self.source_type.clone(),
                source: Box::new(error),
            },
        }
    }
}

struct PythonMultipartWriter {
    source: Arc<PythonSource>,
    uri: String,
    parts: Vec<Bytes>,
    total_len: usize,
}

#[async_trait]
impl MultipartWriter for PythonMultipartWriter {
    fn part_size(&self) -> usize {
        5 * 1024 * 1024
    }

    async fn put_part(&mut self, data: Bytes) -> super::Result<()> {
        self.total_len += data.len();
        if self.total_len > MAX_BUFFERED_WRITE_BYTES {
            return Err(super::Error::InvalidArgument {
                msg: format!(
                    "Buffered write to '{}' exceeds the 1 GiB limit for Python IO extensions",
                    self.uri
                ),
            });
        }
        self.parts.push(data);
        Ok(())
    }

    async fn complete(&mut self) -> super::Result<()> {
        let mut data = Vec::with_capacity(self.total_len);
        for part in self.parts.drain(..) {
            data.extend_from_slice(&part);
        }
        self.source.put(&self.uri, Bytes::from(data), None).await
    }
}

#[async_trait]
impl ObjectSource for PythonSource {
    async fn supports_range(&self, uri: &str) -> super::Result<bool> {
        let extension = Python::attach(|py| self.extension.clone_ref(py));
        let uri_owned = uri.to_string();
        common_runtime::python::execute_python_coroutine::<_, bool>(move |py| {
            py.import(pyo3::intern!(py, "daft.io.extensions"))?
                .getattr(pyo3::intern!(py, "_supports_range"))?
                .call1((extension, uri_owned))
        })
        .await
        .map_err(|error| self.translate_error(uri, error))
    }

    async fn create_multipart_writer(
        self: Arc<Self>,
        uri: &str,
    ) -> super::Result<Option<Box<dyn MultipartWriter>>> {
        Ok(Some(Box::new(PythonMultipartWriter {
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
        if matches!(self.source_type, SourceType::Gravitino) {
            let (resolved_uri, io_config) = self.resolve_url_and_config(uri).await?;
            let io_client = crate::get_io_client(true, Arc::new(io_config)).map_err(|source| {
                super::Error::Generic {
                    store: self.source_type.clone(),
                    source: Box::new(source),
                }
            })?;
            return io_client
                .single_url_get(resolved_uri, range, io_stats)
                .await;
        }

        let extension = Python::attach(|py| self.extension.clone_ref(py));
        let uri_owned = uri.to_string();
        let (range_kind, range_start, range_end) = match range {
            Some(GetRange::Bounded(range)) => (
                Some("bounded"),
                Some(range.start as u64),
                Some(range.end as u64),
            ),
            Some(GetRange::Offset(offset)) => (Some("offset"), Some(offset as u64), None),
            Some(GetRange::Suffix(suffix)) => (Some("suffix"), Some(suffix as u64), None),
            None => (None, None, None),
        };
        let data = common_runtime::python::execute_python_coroutine::<_, Vec<u8>>(move |py| {
            py.import(pyo3::intern!(py, "daft.io.extensions"))?
                .getattr(pyo3::intern!(py, "_get"))?
                .call1((extension, uri_owned, range_kind, range_start, range_end))
        })
        .await
        .map_err(|error| self.translate_error(uri, error))?;

        if let Some(stats) = io_stats.as_ref() {
            stats.mark_get_requests(1);
        }
        let size = data.len();
        let stream = stream::iter([Ok(Bytes::from(data))]);
        Ok(GetResult::Stream(
            io_stats_on_bytestream(stream, io_stats),
            Some(size),
            None,
            None,
        ))
    }

    async fn put(&self, uri: &str, data: Bytes, io_stats: Option<IOStatsRef>) -> super::Result<()> {
        let extension = Python::attach(|py| self.extension.clone_ref(py));
        let uri_owned = uri.to_string();
        common_runtime::python::execute_python_coroutine_noreturn(move |py| {
            let data = PyBytes::new(py, &data);
            py.import(pyo3::intern!(py, "daft.io.extensions"))?
                .getattr(pyo3::intern!(py, "_put"))?
                .call1((extension, uri_owned, data))
        })
        .await
        .map_err(|error| self.translate_error(uri, error))?;
        if let Some(stats) = io_stats {
            stats.mark_put_requests(1);
        }
        Ok(())
    }

    async fn get_size(&self, uri: &str, io_stats: Option<IOStatsRef>) -> super::Result<usize> {
        if matches!(self.source_type, SourceType::Gravitino) {
            let (resolved_uri, io_config) = self.resolve_url_and_config(uri).await?;
            let io_client = crate::get_io_client(true, Arc::new(io_config)).map_err(|source| {
                super::Error::Generic {
                    store: self.source_type.clone(),
                    source: Box::new(source),
                }
            })?;
            return io_client.single_url_get_size(resolved_uri, io_stats).await;
        }

        let extension = Python::attach(|py| self.extension.clone_ref(py));
        let uri_owned = uri.to_string();
        let size = common_runtime::python::execute_python_coroutine::<_, usize>(move |py| {
            py.import(pyo3::intern!(py, "daft.io.extensions"))?
                .getattr(pyo3::intern!(py, "_get_size"))?
                .call1((extension, uri_owned))
        })
        .await
        .map_err(|error| self.translate_error(uri, error))?;
        if let Some(stats) = io_stats {
            stats.mark_head_requests(1);
        }
        Ok(size)
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
        let fanout_limit =
            if matches!(self.source_type, SourceType::Gravitino) && fanout_limit.is_some() {
                let (resolved_path, _) = self.resolve_url_and_config(glob_path).await?;
                let (backing_source_type, _) = crate::parse_url(&resolved_path)?;
                if matches!(backing_source_type, SourceType::File) {
                    None
                } else {
                    fanout_limit
                }
            } else {
                fanout_limit
            };
        object_store_glob::glob(self, glob_path, fanout_limit, page_size, limit, io_stats).await
    }

    async fn ls(
        &self,
        path: &str,
        posix: bool,
        continuation_token: Option<&str>,
        page_size: Option<i32>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<LSResult> {
        type PyListing = (Vec<(String, Option<u64>, String)>, Option<String>, bool);

        let extension = Python::attach(|py| self.extension.clone_ref(py));
        let path_owned = path.to_string();
        let continuation_token = continuation_token.map(str::to_string);
        let (files, continuation_token, not_found_if_empty): PyListing =
            common_runtime::python::execute_python_coroutine::<_, PyListing>(move |py| {
                py.import(pyo3::intern!(py, "daft.io.extensions"))?
                    .getattr(pyo3::intern!(py, "_ls"))?
                    .call1((extension, path_owned, posix, continuation_token, page_size))
            })
            .await
            .map_err(|error| self.translate_error(path, error))?;

        let files = files
            .into_iter()
            .map(|(filepath, size, file_type)| {
                let filetype = match file_type.as_str() {
                    "file" => Ok(FileType::File),
                    "directory" => Ok(FileType::Directory),
                    value => Err(super::Error::InvalidArgument {
                        msg: format!("Unknown IO extension file type: {value}"),
                    }),
                }?;
                Ok(FileMetadata {
                    filepath,
                    size,
                    filetype,
                })
            })
            .collect::<super::Result<_>>()?;
        if let Some(stats) = io_stats {
            stats.mark_list_requests(1);
        }
        Ok(LSResult {
            files,
            continuation_token,
            not_found_if_empty,
        })
    }

    async fn delete(&self, uri: &str, io_stats: Option<IOStatsRef>) -> super::Result<()> {
        let extension = Python::attach(|py| self.extension.clone_ref(py));
        let uri_owned = uri.to_string();
        common_runtime::python::execute_python_coroutine_noreturn(move |py| {
            py.import(pyo3::intern!(py, "daft.io.extensions"))?
                .getattr(pyo3::intern!(py, "_delete"))?
                .call1((extension, uri_owned))
        })
        .await
        .map_err(|error| self.translate_error(uri, error))?;
        if let Some(stats) = io_stats {
            stats.mark_delete_requests(1);
        }
        Ok(())
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
}
