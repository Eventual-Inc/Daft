pub use common_io_config::python::{AzureConfig, GCSConfig, IOConfig};
pub use py::register_modules;

mod py {
    use crate::{get_io_client, get_runtime, parse_url, s3_like, stats::IOStatsContext};
    use common_error::DaftResult;
    use futures::TryStreamExt;
    use pyo3::{
        prelude::*,
        types::{PyDict, PyList},
    };

    #[pyfunction]
    fn io_glob(
        py: Python,
        path: String,
        multithreaded_io: Option<bool>,
        io_config: Option<common_io_config::python::IOConfig>,
        fanout_limit: Option<usize>,
        page_size: Option<i32>,
        limit: Option<usize>,
    ) -> PyResult<&PyList> {
        let multithreaded_io = multithreaded_io.unwrap_or(true);
        let io_stats = IOStatsContext::new(format!("io_glob for {path}"));
        let io_stats_handle = io_stats.clone();

        let lsr: DaftResult<Vec<_>> = py.allow_threads(|| {
            let io_client = get_io_client(
                multithreaded_io,
                io_config.unwrap_or_default().config.into(),
            )?;
            let (scheme, path) = parse_url(&path)?;
            let runtime_handle = get_runtime(multithreaded_io)?;
            let _rt_guard = runtime_handle.enter();

            runtime_handle.block_on(async move {
                let source = io_client.get_source(&scheme).await?;
                let files = source
                    .glob(
                        path.as_ref(),
                        fanout_limit,
                        page_size,
                        limit,
                        Some(io_stats_handle),
                    )
                    .await?
                    .try_collect()
                    .await?;
                Ok(files)
            })
        });
        let lsr = lsr?;
        let mut to_rtn = vec![];
        for file in lsr {
            let dict = PyDict::new(py);
            dict.set_item("type", format!("{:?}", file.filetype))?;
            dict.set_item("path", file.filepath)?;
            dict.set_item("size", file.size)?;
            to_rtn.push(dict);
        }
        Ok(PyList::new(py, to_rtn))
    }

    #[pyfunction]
    fn set_io_pool_num_threads(num_threads: i64) -> PyResult<bool> {
        Ok(crate::set_io_pool_num_threads(num_threads as usize))
    }

    /// Creates an S3Config from the current environment, auto-discovering variables such as
    /// credentials, regions and more.
    #[pyfunction]
    fn s3_config_from_env(py: Python) -> PyResult<common_io_config::python::S3Config> {
        let s3_config: DaftResult<common_io_config::S3Config> = py.allow_threads(|| {
            let runtime = get_runtime(false)?;
            let runtime_handle = runtime.handle();
            let _rt_guard = runtime_handle.enter();
            runtime_handle.block_on(async { Ok(s3_like::s3_config_from_env().await?) })
        });
        Ok(common_io_config::python::S3Config { config: s3_config? })
    }

    pub fn register_modules(py: Python, parent: &PyModule) -> PyResult<()> {
        common_io_config::python::register_modules(py, parent)?;
        parent.add_function(wrap_pyfunction!(io_glob, parent)?)?;
        parent.add_function(wrap_pyfunction!(set_io_pool_num_threads, parent)?)?;
        parent.add_function(wrap_pyfunction!(s3_config_from_env, parent)?)?;
        Ok(())
    }
}
