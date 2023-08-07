use pyo3::prelude::*;

pub mod pylib {
    use std::sync::Arc;

    use common_error::{DaftError, DaftResult};
    use daft_core::datatypes::TimeUnit;
    use daft_core::python::datatype::PyTimeUnit;
    use daft_core::python::{schema::PySchema, PySeries};
    use daft_io::{get_io_client, python::IOConfig};
    use daft_table::python::PyTable;
    use pyo3::{pyclass, pyfunction, pymethods, PyResult, Python};

    use crate::read::{ParquetSchemaInferenceOptions, ParquetSchemaOptions};

    const DEFAULT_SCHEMA_OPTIONS: ParquetSchemaOptions =
        ParquetSchemaOptions::InferenceOptions(ParquetSchemaInferenceOptions {
            int96_timestamps_time_unit: TimeUnit::Nanoseconds,
        });

    /// Python wrapper for ParquetSchemaOptions
    ///
    /// Represents the options for parsing Schemas from Parquet files. If `schema` is provided, then
    /// all the `inference_option_*` will be ignored when parsing Schemas.
    #[pyclass]
    pub struct PyParquetSchemaOptions {
        schema: Option<PySchema>,
        inference_option_int96_timestamps_time_unit: Option<PyTimeUnit>,
    }

    #[pymethods]
    impl PyParquetSchemaOptions {
        #[new]
        fn new(
            schema: Option<PySchema>,
            inference_option_int96_timestamps_time_unit: Option<PyTimeUnit>,
        ) -> Self {
            PyParquetSchemaOptions {
                schema,
                inference_option_int96_timestamps_time_unit,
            }
        }
    }

    impl TryFrom<&PyParquetSchemaOptions> for ParquetSchemaOptions {
        type Error = DaftError;
        fn try_from(value: &PyParquetSchemaOptions) -> DaftResult<Self> {
            match &value.schema {
                Some(s) => Ok(ParquetSchemaOptions::UserProvidedSchema(s.schema.clone())),
                None => Ok(ParquetSchemaOptions::InferenceOptions(
                    ParquetSchemaInferenceOptions {
                        int96_timestamps_time_unit: value
                            .inference_option_int96_timestamps_time_unit
                            .as_ref()
                            .map_or(TimeUnit::Nanoseconds, |tu| tu.timeunit),
                    },
                )),
            }
        }
    }

    #[pyfunction]
    pub fn read_parquet(
        py: Python,
        uri: &str,
        columns: Option<Vec<&str>>,
        start_offset: Option<usize>,
        num_rows: Option<usize>,
        io_config: Option<IOConfig>,
        schema_options: Option<&PyParquetSchemaOptions>,
    ) -> PyResult<PyTable> {
        py.allow_threads(|| {
            let io_client = get_io_client(io_config.unwrap_or_default().config.into())?;
            let schema_options = match schema_options {
                None => DEFAULT_SCHEMA_OPTIONS,
                Some(opts) => opts.try_into()?,
            };
            Ok(crate::read::read_parquet(
                uri,
                columns.as_deref(),
                start_offset,
                num_rows,
                schema_options,
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
        schema_options: Option<&PyParquetSchemaOptions>,
    ) -> PyResult<PySchema> {
        py.allow_threads(|| {
            let schema_options = match schema_options {
                None => DEFAULT_SCHEMA_OPTIONS,
                Some(opts) => opts.try_into()?,
            };
            match schema_options {
                ParquetSchemaOptions::UserProvidedSchema(s) => Ok(PySchema { schema: s }),
                ParquetSchemaOptions::InferenceOptions(opts) => {
                    let io_client = get_io_client(io_config.unwrap_or_default().config.into())?;
                    Ok(Arc::new(crate::read::read_parquet_schema(uri, io_client, &opts)?).into())
                }
            }
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
    parent.add_class::<pylib::PyParquetSchemaOptions>()?;
    Ok(())
}
