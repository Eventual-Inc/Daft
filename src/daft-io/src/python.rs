pub use common_io_config::python::{AzureConfig, GCSConfig, IOConfig};
pub use py::register_modules;

mod py {
    use common_error::DaftResult;
    use common_runtime::get_io_runtime;
    use futures::TryStreamExt;
    use pyo3::{prelude::*, types::PyDict};

    use crate::{GetRange, get_io_client, parse_url, s3_like, stats::IOStatsContext};

    #[pyfunction(signature = (
        input,
        multithreaded_io=None,
        io_config=None,
        fanout_limit=None,
        page_size=None,
        limit=None
    ))]
    fn io_glob(
        py: Python,
        input: String,
        multithreaded_io: Option<bool>,
        io_config: Option<common_io_config::python::IOConfig>,
        fanout_limit: Option<usize>,
        page_size: Option<i32>,
        limit: Option<usize>,
    ) -> PyResult<Vec<Bound<PyDict>>> {
        let multithreaded_io = multithreaded_io.unwrap_or(true);
        let io_stats = IOStatsContext::new(format!("io_glob for {input}"));
        let io_stats_handle = io_stats;

        let lsr: DaftResult<Vec<_>> = py.detach(|| {
            let io_client = get_io_client(
                multithreaded_io,
                io_config.unwrap_or_default().config.into(),
            )?;
            let (_, path) = parse_url(&input)?;
            let runtime_handle = get_io_runtime(multithreaded_io);

            runtime_handle.block_on_current_thread(async {
                let source = io_client.get_source(&input).await?;
                let files = source
                    .glob(
                        path.as_ref(),
                        fanout_limit,
                        page_size,
                        limit,
                        Some(io_stats_handle),
                        None,
                    )
                    .await?
                    .try_collect()
                    .await?;
                Ok(files)
            })
        });
        let mut to_rtn = vec![];
        for file in lsr? {
            let dict = PyDict::new(py);
            dict.set_item("type", format!("{:?}", file.filetype))?;
            dict.set_item("path", file.filepath)?;
            dict.set_item("size", file.size)?;
            to_rtn.push(dict);
        }
        Ok(to_rtn)
    }

    /// Creates an S3Config from the current environment, auto-discovering variables such as
    /// credentials, regions and more.
    #[pyfunction]
    fn s3_config_from_env(py: Python) -> PyResult<common_io_config::python::S3Config> {
        let s3_config: DaftResult<common_io_config::S3Config> = py.detach(|| {
            let runtime = get_io_runtime(false);
            runtime.block_on_current_thread(async { Ok(s3_like::s3_config_from_env().await?) })
        });
        Ok(common_io_config::python::S3Config { config: s3_config? })
    }

    #[pyfunction(signature = (
        path,
        data,
        multithreaded_io=None,
        io_config=None
    ))]
    fn io_put(
        py: Python,
        path: String,
        data: &[u8],
        multithreaded_io: Option<bool>,
        io_config: Option<common_io_config::python::IOConfig>,
    ) -> PyResult<()> {
        let multithreaded_io = multithreaded_io.unwrap_or(true);
        let io_stats = IOStatsContext::new(format!("io_put for {path}"));
        let io_stats_handle = io_stats;

        let result: DaftResult<()> = py.detach(|| {
            let io_client = get_io_client(
                multithreaded_io,
                io_config.unwrap_or_default().config.into(),
            )
            .map_err(|e| common_error::DaftError::External(e.into()))?;

            // Check if we're already in a runtime context
            let data_bytes = bytes::Bytes::copy_from_slice(data);
            match tokio::runtime::Handle::try_current() {
                Ok(_handle) => {
                    // We're in an async context, spawn a blocking task
                    std::thread::spawn(move || {
                        let rt = tokio::runtime::Runtime::new().unwrap();
                        rt.block_on(async {
                            io_client
                                .single_url_put(&path, data_bytes, Some(io_stats_handle))
                                .await
                                .map_err(|e| common_error::DaftError::External(e.into()))
                        })
                    })
                    .join()
                    .map_err(|_| common_error::DaftError::External("Thread join failed".into()))?
                }
                Err(_) => {
                    // No runtime, create one
                    let runtime_handle = get_io_runtime(multithreaded_io);
                    runtime_handle.block_on_current_thread(async {
                        io_client
                            .single_url_put(&path, data_bytes, Some(io_stats_handle))
                            .await
                            .map_err(|e| common_error::DaftError::External(e.into()))
                    })
                }
            }
        });
        result?;
        Ok(())
    }

    #[pyfunction(signature = (
        path,
        multithreaded_io=None,
        io_config=None,
        range_start=None,
        range_end=None,
        suffix=None
    ))]
    fn io_get(
        py: Python,
        path: String,
        multithreaded_io: Option<bool>,
        io_config: Option<common_io_config::python::IOConfig>,
        range_start: Option<usize>,
        range_end: Option<usize>,
        suffix: Option<usize>,
    ) -> PyResult<Vec<u8>> {
        if suffix.is_some() && (range_start.is_some() || range_end.is_some()) {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "suffix cannot be combined with range_start or range_end",
            ));
        }
        if range_end.is_some() && range_start.is_none() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "range_end requires range_start",
            ));
        }
        let range = match (range_start, range_end, suffix) {
            (_, _, Some(suffix)) => Some(GetRange::Suffix(suffix)),
            (Some(start), Some(end), None) => Some(GetRange::Bounded(start..end)),
            (Some(start), None, None) => Some(GetRange::Offset(start)),
            (None, None, None) => None,
            _ => unreachable!(),
        };
        let multithreaded_io = multithreaded_io.unwrap_or(true);
        let io_stats = IOStatsContext::new(format!("io_get for {path}"));
        let result: DaftResult<Vec<u8>> = py.detach(|| {
            let io_client = get_io_client(
                multithreaded_io,
                io_config.unwrap_or_default().config.into(),
            )?;
            let runtime_handle = get_io_runtime(multithreaded_io);
            runtime_handle.block_on_current_thread(async {
                let result = io_client
                    .single_url_get(path, range, Some(io_stats))
                    .await?;
                Ok(result.bytes().await?.to_vec())
            })
        });
        Ok(result?)
    }

    #[pyfunction(signature = (path, multithreaded_io=None, io_config=None))]
    fn io_get_size(
        py: Python,
        path: String,
        multithreaded_io: Option<bool>,
        io_config: Option<common_io_config::python::IOConfig>,
    ) -> PyResult<usize> {
        let multithreaded_io = multithreaded_io.unwrap_or(true);
        let io_stats = IOStatsContext::new(format!("io_get_size for {path}"));
        let result: DaftResult<usize> = py.detach(|| {
            let io_client = get_io_client(
                multithreaded_io,
                io_config.unwrap_or_default().config.into(),
            )?;
            let runtime_handle = get_io_runtime(multithreaded_io);
            runtime_handle.block_on_current_thread(async {
                Ok(io_client.single_url_get_size(path, Some(io_stats)).await?)
            })
        });
        Ok(result?)
    }

    #[pyfunction(signature = (
        path,
        posix,
        continuation_token=None,
        page_size=None,
        multithreaded_io=None,
        io_config=None
    ))]
    fn io_ls(
        py: Python,
        path: String,
        posix: bool,
        continuation_token: Option<String>,
        page_size: Option<i32>,
        multithreaded_io: Option<bool>,
        io_config: Option<common_io_config::python::IOConfig>,
    ) -> PyResult<(Vec<Bound<PyDict>>, Option<String>, bool)> {
        let multithreaded_io = multithreaded_io.unwrap_or(true);
        let io_stats = IOStatsContext::new(format!("io_ls for {path}"));
        let result: DaftResult<crate::object_io::LSResult> = py.detach(|| {
            let io_client = get_io_client(
                multithreaded_io,
                io_config.unwrap_or_default().config.into(),
            )?;
            let runtime_handle = get_io_runtime(multithreaded_io);
            runtime_handle.block_on_current_thread(async {
                let (source, source_path) = io_client.get_source_and_path(&path).await?;
                Ok(source
                    .ls(
                        &source_path,
                        posix,
                        continuation_token.as_deref(),
                        page_size,
                        Some(io_stats),
                    )
                    .await?)
            })
        });
        let result = result?;
        let files = result
            .files
            .into_iter()
            .map(|file| {
                let dict = PyDict::new(py);
                dict.set_item("type", format!("{:?}", file.filetype))?;
                dict.set_item("path", file.filepath)?;
                dict.set_item("size", file.size)?;
                Ok(dict)
            })
            .collect::<PyResult<_>>()?;
        Ok((files, result.continuation_token, result.not_found_if_empty))
    }

    pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
        common_io_config::python::register_modules(parent)?;
        parent.add_function(wrap_pyfunction!(io_glob, parent)?)?;
        parent.add_function(wrap_pyfunction!(s3_config_from_env, parent)?)?;
        parent.add_function(wrap_pyfunction!(io_put, parent)?)?;
        parent.add_function(wrap_pyfunction!(io_get, parent)?)?;
        parent.add_function(wrap_pyfunction!(io_get_size, parent)?)?;
        parent.add_function(wrap_pyfunction!(io_ls, parent)?)?;
        Ok(())
    }
}
