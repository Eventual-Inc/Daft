pub mod pylib {
    use daft_core::python::PySchema;
    use daft_io::{IOStatsContext, get_io_client, python::IOConfig};
    use daft_recordbatch::python::PyRecordBatch;
    use pyo3::{PyResult, Python, pyfunction};

    use crate::AvroError;

    impl From<AvroError> for pyo3::PyErr {
        fn from(value: AvroError) -> Self {
            let daft_error: common_error::DaftError = value.into();
            daft_error.into()
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[pyfunction(signature = (
        uri,
        io_config = None,
        multithreaded_io = None,
        column_projection = None,
        max_records = None
    ))]
    pub fn read_avro(
        py: Python,
        uri: &str,
        io_config: Option<IOConfig>,
        multithreaded_io: Option<bool>,
        column_projection: Option<Vec<String>>,
        max_records: Option<usize>,
    ) -> PyResult<PyRecordBatch> {
        py.detach(|| {
            let io_stats = IOStatsContext::new(format!("read_avro: for uri {uri}"));

            let io_client = get_io_client(
                multithreaded_io.unwrap_or(true),
                io_config.unwrap_or_default().config.into(),
            )?;

            let runtime_handle = common_runtime::get_io_runtime(multithreaded_io.unwrap_or(true));

            let record_batch = runtime_handle.block_on_current_thread(async {
                crate::read::read_avro(
                    uri,
                    io_client,
                    Some(io_stats),
                    column_projection,
                    max_records,
                )
                .await
            })?;

            Ok(PyRecordBatch::from(record_batch))
        })
    }

    #[pyfunction(signature = (
        uri,
        io_config = None,
        multithreaded_io = None,
    ))]
    pub fn read_avro_schema(
        py: Python,
        uri: &str,
        io_config: Option<IOConfig>,
        multithreaded_io: Option<bool>,
    ) -> PyResult<PySchema> {
        py.detach(|| {
            let io_stats = IOStatsContext::new(format!("read_avro_schema: for uri {uri}"));

            let io_client = get_io_client(
                multithreaded_io.unwrap_or(true),
                io_config.unwrap_or_default().config.into(),
            )?;

            let runtime_handle = common_runtime::get_io_runtime(true);

            let schema = runtime_handle.block_on_current_thread(async {
                crate::schema::read_avro_schema(uri, io_client, Some(io_stats)).await
            })?;

            Ok(PySchema { schema })
        })
    }
}
