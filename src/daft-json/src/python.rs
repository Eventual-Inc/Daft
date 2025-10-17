pub mod pylib {
    use std::sync::Arc;

    use daft_core::python::PySchema;
    use daft_io::{IOStatsContext, get_io_client, python::IOConfig};
    use daft_recordbatch::python::PyRecordBatch;
    use pyo3::{PyResult, Python, pyfunction};

    use crate::{JsonConvertOptions, JsonParseOptions, JsonReadOptions};

    #[allow(clippy::too_many_arguments)]
    #[pyfunction(signature = (
        uri,
        convert_options=None,
        parse_options=None,
        read_options=None,
        io_config=None,
        multithreaded_io=None,
        max_chunks_in_flight=None
    ))]
    pub fn read_json(
        py: Python,
        uri: &str,
        convert_options: Option<JsonConvertOptions>,
        parse_options: Option<JsonParseOptions>,
        read_options: Option<JsonReadOptions>,
        io_config: Option<IOConfig>,
        multithreaded_io: Option<bool>,
        max_chunks_in_flight: Option<usize>,
    ) -> PyResult<PyRecordBatch> {
        py.detach(|| {
            let io_stats = IOStatsContext::new(format!("read_json: for uri {uri}"));

            let io_client = get_io_client(
                multithreaded_io.unwrap_or(true),
                io_config.unwrap_or_default().config.into(),
            )?;
            Ok(crate::read::read_json(
                uri,
                convert_options,
                parse_options,
                read_options,
                io_client,
                Some(io_stats),
                multithreaded_io.unwrap_or(true),
                max_chunks_in_flight,
            )?
            .into())
        })
    }

    #[pyfunction(signature = (
        uri,
        parse_options=None,
        max_bytes=None,
        io_config=None,
        multithreaded_io=None
    ))]
    pub fn read_json_schema(
        py: Python,
        uri: &str,
        parse_options: Option<JsonParseOptions>,
        max_bytes: Option<usize>,
        io_config: Option<IOConfig>,
        multithreaded_io: Option<bool>,
    ) -> PyResult<PySchema> {
        py.detach(|| {
            let io_stats = IOStatsContext::new(format!("read_json_schema: for uri {uri}"));

            let io_client = get_io_client(
                multithreaded_io.unwrap_or(true),
                io_config.unwrap_or_default().config.into(),
            )?;

            let runtime_handle = common_runtime::get_io_runtime(true);

            let schema = runtime_handle.block_on_current_thread(async {
                crate::schema::read_json_schema(
                    uri,
                    parse_options,
                    max_bytes,
                    io_client,
                    Some(io_stats),
                )
                .await
            })?;

            Ok(Arc::new(schema).into())
        })
    }
}
