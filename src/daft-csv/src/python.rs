use pyo3::prelude::*;

pub mod pylib {
    use std::sync::Arc;

    use daft_core::python::schema::PySchema;
    use daft_io::{get_io_client, python::IOConfig, IOStatsContext};
    use daft_table::python::PyTable;
    use pyo3::{pyfunction, PyResult, Python};

    #[pyfunction]
    #[allow(clippy::too_many_arguments)]
    pub fn read_csv(
        py: Python,
        uri: &str,
        column_names: Option<Vec<&str>>,
        include_columns: Option<Vec<&str>>,
        num_rows: Option<usize>,
        has_header: Option<bool>,
        delimiter: Option<char>,
        double_quote: Option<bool>,
        quote: Option<char>,
        escape_char: Option<char>,
        comment: Option<char>,
        io_config: Option<IOConfig>,
        multithreaded_io: Option<bool>,
        schema: Option<PySchema>,
        buffer_size: Option<usize>,
        chunk_size: Option<usize>,
    ) -> PyResult<PyTable> {
        py.allow_threads(|| {
            let io_stats = IOStatsContext::new(format!("read_csv: for uri {uri}"));

            let io_client = get_io_client(
                multithreaded_io.unwrap_or(true),
                io_config.unwrap_or_default().config.into(),
            )?;
            Ok(crate::read::read_csv(
                uri,
                column_names,
                include_columns,
                num_rows,
                has_header.unwrap_or(true),
                delimiter,
                double_quote.unwrap_or(true),
                quote,
                escape_char,
                comment,
                io_client,
                Some(io_stats),
                multithreaded_io.unwrap_or(true),
                schema.map(|s| s.schema),
                buffer_size,
                chunk_size,
                None,
            )?
            .into())
        })
    }

    #[pyfunction]
    #[allow(clippy::too_many_arguments)]
    pub fn read_csv_schema(
        py: Python,
        uri: &str,
        has_header: Option<bool>,
        delimiter: Option<char>,
        double_quote: Option<bool>,
        quote: Option<char>,
        escape_char: Option<char>,
        comment: Option<char>,
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
            let (schema, _, _, _, _) = crate::metadata::read_csv_schema(
                uri,
                has_header.unwrap_or(true),
                delimiter,
                double_quote.unwrap_or(true),
                quote,
                escape_char,
                comment,
                max_bytes,
                io_client,
                Some(io_stats),
            )?;
            Ok(Arc::new(schema).into())
        })
    }
}

pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_wrapped(wrap_pyfunction!(pylib::read_csv))?;
    parent.add_wrapped(wrap_pyfunction!(pylib::read_csv_schema))?;
    Ok(())
}
