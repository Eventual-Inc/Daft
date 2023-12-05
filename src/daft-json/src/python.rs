pub mod pylib {
    use std::sync::Arc;

    use daft_core::python::schema::PySchema;
    use daft_io::{get_io_client, python::IOConfig, IOStatsContext};
    use daft_table::python::PyTable;
    use pyo3::{pyfunction, PyResult, Python};

    use crate::{JsonConvertOptions, JsonParseOptions, JsonReadOptions};

    #[allow(clippy::too_many_arguments)]
    #[pyfunction]
    pub fn read_json(
        py: Python,
        uri: &str,
        convert_options: Option<JsonConvertOptions>,
        parse_options: Option<JsonParseOptions>,
        read_options: Option<JsonReadOptions>,
        io_config: Option<IOConfig>,
        multithreaded_io: Option<bool>,
        max_chunks_in_flight: Option<usize>,
    ) -> PyResult<PyTable> {
        py.allow_threads(|| {
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

    #[pyfunction]
    pub fn read_json_schema(
        py: Python,
        uri: &str,
        parse_options: Option<JsonParseOptions>,
        max_bytes: Option<usize>,
        io_config: Option<IOConfig>,
        multithreaded_io: Option<bool>,
    ) -> PyResult<PySchema> {
        py.allow_threads(|| {
            let io_stats = IOStatsContext::new(format!("read_json_schema: for uri {uri}"));

            let io_client = get_io_client(
                multithreaded_io.unwrap_or(true),
                io_config.unwrap_or_default().config.into(),
            )?;
            let schema = crate::schema::read_json_schema(
                uri,
                parse_options,
                max_bytes,
                io_client,
                Some(io_stats),
            )?;
            Ok(Arc::new(schema).into())
        })
    }
}
