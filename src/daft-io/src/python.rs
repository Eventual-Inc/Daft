pub use common_io_config::python::{AzureConfig, GCSConfig, IOConfig};
pub use py::register_modules;

mod py {
    use common_error::DaftResult;
    use common_runtime::get_io_runtime;
    use futures::TryStreamExt;
    use pyo3::{prelude::*, types::PyDict};

    use crate::{get_io_client, parse_url, s3_like, stats::IOStatsContext};

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

    pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
        common_io_config::python::register_modules(parent)?;
        parent.add_function(wrap_pyfunction!(io_glob, parent)?)?;
        parent.add_function(wrap_pyfunction!(s3_config_from_env, parent)?)?;
        Ok(())
    }
}
