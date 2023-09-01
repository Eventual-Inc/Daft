use crate::{get_io_client, get_runtime, object_io::LSResult, parse_url};
use common_error::DaftResult;
use pyo3::{
    prelude::*,
    types::{PyDict, PyList},
};

pub use common_io_config::python::{AzureConfig, GCSConfig, IOConfig};

#[pyfunction]
fn io_list(
    py: Python,
    path: String,
    multithreaded_io: Option<bool>,
    io_config: Option<common_io_config::python::IOConfig>,
) -> PyResult<&PyList> {
    let lsr: DaftResult<LSResult> = py.allow_threads(|| {
        let io_client = get_io_client(
            multithreaded_io.unwrap_or(true),
            io_config.unwrap_or_default().config.into(),
        )?;
        let (scheme, path) = parse_url(&path)?;
        let runtime_handle = get_runtime(true)?;
        let _rt_guard = runtime_handle.enter();

        runtime_handle.block_on(async move {
            let source = io_client.get_source(&scheme).await?;
            Ok(source.ls(&path, None, None).await?)
        })
    });
    let lsr = lsr?;
    let mut to_rtn = vec![];
    for file in lsr.files {
        let dict = PyDict::new(py);
        dict.set_item("type", format!("{:?}", file.filetype))?;
        dict.set_item("path", file.filepath)?;
        dict.set_item("size", file.size)?;
        to_rtn.push(dict);
    }
    Ok(PyList::new(py, to_rtn))
}

pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_function(wrap_pyfunction!(io_list, parent)?)?;
    Ok(())
}
