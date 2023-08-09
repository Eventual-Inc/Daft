use pyo3::prelude::*;

pub mod pylib {
    use std::sync::Arc;

    use daft_core::python::{datatype::PyTimeUnit, schema::PySchema, PySeries};
    use daft_io::{get_io_client, python::IOConfig};
    use daft_table::python::PyTable;
    use pyo3::{pyfunction, PyResult, Python};

    use crate::read::ParquetSchemaInferenceOptions;

    #[allow(clippy::too_many_arguments)]
    #[pyfunction]
    pub fn read_parquet(
        py: Python,
        uri: &str,
        columns: Option<Vec<&str>>,
        start_offset: Option<usize>,
        num_rows: Option<usize>,
        io_config: Option<IOConfig>,
        coerce_int96_timestamp_unit: Option<PyTimeUnit>,
    ) -> PyResult<PyTable> {
        py.allow_threads(|| {
            let io_client = get_io_client(io_config.unwrap_or_default().config.into())?;
            let schema_infer_options = ParquetSchemaInferenceOptions::new(
                coerce_int96_timestamp_unit.map(|tu| tu.timeunit),
            );
            Ok(crate::read::read_parquet(
                uri,
                columns.as_deref(),
                start_offset,
                num_rows,
                io_client,
                &schema_infer_options,
            )?
            .into())
        })
    }

    #[pyfunction]
    pub fn read_parquet_bulk(
        py: Python,
        uris: Vec<&str>,
        columns: Option<Vec<&str>>,
        start_offset: Option<usize>,
        num_rows: Option<usize>,
        io_config: Option<IOConfig>,
        coerce_int96_timestamp_unit: Option<PyTimeUnit>,
    ) -> PyResult<Vec<PyTable>> {
        py.allow_threads(|| {
            let io_client = get_io_client(io_config.unwrap_or_default().config.into())?;
            let schema_infer_options = ParquetSchemaInferenceOptions::new(
                coerce_int96_timestamp_unit.map(|tu| tu.timeunit),
            );
            Ok(crate::read::read_parquet_bulk(
                uris.as_ref(),
                columns.as_deref(),
                start_offset,
                num_rows,
                io_client,
                &schema_infer_options,
            )?
            .into_iter()
            .map(|v| v.into())
            .collect())
        })
    }

    #[pyfunction]
    pub fn read_parquet_schema(
        py: Python,
        uri: &str,
        io_config: Option<IOConfig>,
        coerce_int96_timestamp_unit: Option<PyTimeUnit>,
    ) -> PyResult<PySchema> {
        py.allow_threads(|| {
            let schema_infer_options = ParquetSchemaInferenceOptions::new(
                coerce_int96_timestamp_unit.map(|tu| tu.timeunit),
            );
            let io_client = get_io_client(io_config.unwrap_or_default().config.into())?;
            Ok(Arc::new(crate::read::read_parquet_schema(
                uri,
                io_client,
                &schema_infer_options,
            )?)
            .into())
        })
    }

    #[pyfunction]
    pub fn read_parquet_statistics(
        py: Python,
        uris: PySeries,
        io_config: Option<IOConfig>,
    ) -> PyResult<PyTable> {
        py.allow_threads(|| {
            let io_client = get_io_client(io_config.unwrap_or_default().config.into())?;
            Ok(crate::read::read_parquet_statistics(&uris.series, io_client)?.into())
        })
    }
}
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_wrapped(wrap_pyfunction!(pylib::read_parquet))?;
    parent.add_wrapped(wrap_pyfunction!(pylib::read_parquet_bulk))?;
    parent.add_wrapped(wrap_pyfunction!(pylib::read_parquet_schema))?;
    parent.add_wrapped(wrap_pyfunction!(pylib::read_parquet_statistics))?;
    Ok(())
}
