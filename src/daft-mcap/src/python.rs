use common_runtime::get_io_runtime;
use daft_io::{IOStatsContext, get_io_client, python::IOConfig};
use daft_recordbatch::python::PyRecordBatch;
use pyo3::prelude::*;

use crate::{McapReadOptions, NativeMcapReader};

#[pyclass(unsendable)]
pub struct PyMcapReader {
    inner: NativeMcapReader,
}

#[pymethods]
impl PyMcapReader {
    #[new]
    #[pyo3(signature = (
        uri,
        io_config=None,
        batch_size=1000,
        start_time=None,
        end_time=None,
        topics=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        uri: String,
        io_config: Option<IOConfig>,
        batch_size: usize,
        start_time: Option<u64>,
        end_time: Option<u64>,
        topics: Option<Vec<String>>,
    ) -> PyResult<Self> {
        py.detach(|| {
            let io_client = get_io_client(true, io_config.unwrap_or_default().config.into())?;
            let io_stats = IOStatsContext::new(format!("read_mcap for {uri}"));
            let options = McapReadOptions {
                batch_size,
                start_time,
                end_time,
                topics,
            };
            let runtime = get_io_runtime(true);
            let inner = runtime.block_on_current_thread(NativeMcapReader::new(
                uri, io_client, io_stats, options,
            ))?;
            Ok(Self { inner })
        })
    }

    fn next_batch(&mut self, py: Python<'_>) -> PyResult<Option<PyRecordBatch>> {
        py.detach(|| {
            let runtime = get_io_runtime(true);
            Ok(runtime
                .block_on_current_thread(self.inner.next_batch())?
                .map(Into::into))
        })
    }

    #[getter]
    fn indexed(&self) -> bool {
        self.inner.indexed()
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyMcapReader>()?;
    Ok(())
}
