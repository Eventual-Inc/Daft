pub mod pylib {
    use std::sync::Arc;

    use daft_core::python::PySchema;
    use daft_io::{get_io_client, python::IOConfig, IOStatsContext};
    use daft_recordbatch::python::PyRecordBatch;
    use pyo3::{pyfunction, PyResult, Python};

    use crate::{CsvConvertOptions, CsvParseOptions, CsvReadOptions};

    #[pyfunction(signature = (
        uri,
        convert_options=None,
        parse_options=None,
        read_options=None,
        io_config=None,
        multithreaded_io=None
    ))]
    pub fn read_csv(
        py: Python,
        uri: &str,
        convert_options: Option<CsvConvertOptions>,
        parse_options: Option<CsvParseOptions>,
        read_options: Option<CsvReadOptions>,
        io_config: Option<IOConfig>,
        multithreaded_io: Option<bool>,
    ) -> PyResult<PyRecordBatch> {
        py.allow_threads(|| {
            let io_stats = IOStatsContext::new(format!("read_csv: for uri {uri}"));

            let io_client = get_io_client(
                multithreaded_io.unwrap_or(true),
                io_config.unwrap_or_default().config.into(),
            )?;
            Ok(crate::read::read_csv(
                uri,
                convert_options,
                parse_options,
                read_options,
                io_client,
                Some(io_stats),
                multithreaded_io.unwrap_or(true),
                None,
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
    pub fn read_csv_schema(
        py: Python,
        uri: &str,
        parse_options: Option<CsvParseOptions>,
        max_bytes: Option<usize>,
        io_config: Option<IOConfig>,
        multithreaded_io: Option<bool>,
    ) -> PyResult<PySchema> {
        py.allow_threads(|| {
            let io_stats = IOStatsContext::new(format!("read_csv_schema: for uri {uri}"));

            let io_client = get_io_client(
                multithreaded_io.unwrap_or(true),
                io_config.unwrap_or_default().config.into(),
            )?;

            let runtime = common_runtime::get_io_runtime(multithreaded_io.unwrap_or(true));

            let (schema, _) = runtime.block_on_current_thread(async move {
                crate::metadata::read_csv_schema(
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
