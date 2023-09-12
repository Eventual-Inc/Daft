pub use common_io_config::python::{AzureConfig, GCSConfig, IOConfig};
pub use py::register_modules;

mod py {
    use crate::{get_io_client, get_runtime, object_io::recursive_iter, parse_url};
    use common_error::DaftResult;
    use futures::TryStreamExt;
    use pyo3::{
        prelude::*,
        types::{PyDict, PyList},
    };

    #[pyfunction]
    fn io_list(
        py: Python,
        path: String,
        recursive: bool,
        multithreaded_io: Option<bool>,
        io_config: Option<common_io_config::python::IOConfig>,
    ) -> PyResult<&PyList> {
        let lsr: DaftResult<Vec<_>> = py.allow_threads(|| {
            let io_client = get_io_client(
                multithreaded_io.unwrap_or(true),
                io_config.unwrap_or_default().config.into(),
            )?;
            let (scheme, path) = parse_url(&path)?;
            let runtime_handle = get_runtime(true)?;
            let _rt_guard = runtime_handle.enter();

            runtime_handle.block_on(async move {
                let source = io_client.get_source(&scheme).await?;
                let files = if recursive {
                    recursive_iter(source, &path)
                        .await?
                        .try_collect::<Vec<_>>()
                        .await?
                } else {
                    source
                        .iter_dir(&path, None, None)
                        .await?
                        .try_collect::<Vec<_>>()
                        .await?
                };

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

    pub fn register_modules(py: Python, parent: &PyModule) -> PyResult<()> {
        common_io_config::python::register_modules(py, parent)?;
        parent.add_function(wrap_pyfunction!(io_list, parent)?)?;
        parent.add_function(wrap_pyfunction!(set_io_pool_num_threads, parent)?)?;

        Ok(())
    }
}
