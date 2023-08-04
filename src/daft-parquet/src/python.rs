use pyo3::prelude::*;

pub mod pylib {
    use std::sync::Arc;

    use common_error::{DaftError, DaftResult};
    use daft_core::datatypes::TimeUnit;
    use daft_core::python::{schema::PySchema, PySeries};
    use daft_io::{get_io_client, python::IOConfig};
    use daft_table::python::PyTable;
    use pyo3::{pyfunction, PyResult, Python};

    fn time_unit_from_str(s: &str) -> DaftResult<TimeUnit> {
        match s {
            "ns" => Ok(TimeUnit::Nanoseconds),
            "us" => Ok(TimeUnit::Microseconds),
            "ms" => Ok(TimeUnit::Milliseconds),
            "s" => Ok(TimeUnit::Seconds),
            _ => Err(DaftError::ValueError(format!(
                "Unrecognized TimeUnit {}",
                s
            ))),
        }
    }

    #[pyfunction]
    pub fn read_parquet(
        py: Python,
        uri: &str,
        columns: Option<Vec<&str>>,
        start_offset: Option<usize>,
        num_rows: Option<usize>,
        int96_timestamps_coerce_to_unit: Option<&str>,
        io_config: Option<IOConfig>,
    ) -> PyResult<PyTable> {
        py.allow_threads(|| {
            let int96_timestamps_coerce_to_unit = int96_timestamps_coerce_to_unit
                .map_or(Ok(TimeUnit::Nanoseconds), time_unit_from_str)?;
            let io_client = get_io_client(io_config.unwrap_or_default().config.into())?;
            Ok(crate::read::read_parquet(
                uri,
                columns.as_deref(),
                start_offset,
                num_rows,
                &int96_timestamps_coerce_to_unit,
                io_client,
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
    ) -> PyResult<Vec<PyTable>> {
        py.allow_threads(|| {
            let io_client = get_io_client(io_config.unwrap_or_default().config.into())?;
            Ok(crate::read::read_parquet_bulk(
                uris.as_ref(),
                columns.as_deref(),
                start_offset,
                num_rows,
                io_client,
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
        int96_timestamps_coerce_to_unit: Option<&str>,
    ) -> PyResult<PySchema> {
        py.allow_threads(|| {
            let int96_timestamps_coerce_to_unit = int96_timestamps_coerce_to_unit
                .map_or(Ok(TimeUnit::Nanoseconds), time_unit_from_str)?;
            let io_client = get_io_client(io_config.unwrap_or_default().config.into())?;
            Ok(Arc::new(crate::read::read_parquet_schema(
                uri,
                io_client,
                &int96_timestamps_coerce_to_unit,
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
