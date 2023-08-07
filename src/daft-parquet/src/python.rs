use pyo3::prelude::*;

pub mod pylib {
    use std::sync::Arc;

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

    /// Python wrapper for ParquetSchemaInferenceOptions
    ///
    /// Represents the options for inferring Schemas from Parquet files
    #[pyclass]
    #[derive(Clone)]
    pub struct PyParquetSchemaInferenceOptions {
        int96_timestamps_time_unit: PyTimeUnit,
    }

    #[pymethods]
    impl PyParquetSchemaInferenceOptions {
        #[new]
        fn new(int96_timestamps_time_unit: PyTimeUnit) -> Self {
            PyParquetSchemaInferenceOptions {
                int96_timestamps_time_unit,
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[pyfunction]
    pub fn read_parquet(
        py: Python,
        uri: &str,
        columns: Option<Vec<&str>>,
        start_offset: Option<usize>,
        num_rows: Option<usize>,
        io_config: Option<IOConfig>,
        schema: Option<PySchema>,
        schema_inference_options: Option<PyParquetSchemaInferenceOptions>,
    ) -> PyResult<PyTable> {
        py.allow_threads(|| {
            let io_client = get_io_client(io_config.unwrap_or_default().config.into())?;
            let schema_options = match (schema, schema_inference_options) {
                (None, None) => DEFAULT_SCHEMA_OPTIONS,
                (Some(schema), _) => ParquetSchemaOptions::UserProvidedSchema(schema.schema),
                (_, Some(opts)) => {
                    ParquetSchemaOptions::InferenceOptions(ParquetSchemaInferenceOptions {
                        int96_timestamps_time_unit: opts.int96_timestamps_time_unit.timeunit,
                    })
                }
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
        schema: Option<PySchema>,
        schema_inference_options: Option<PyParquetSchemaInferenceOptions>,
    ) -> PyResult<PySchema> {
        py.allow_threads(|| {
            let schema_options = match (schema, schema_inference_options) {
                (None, None) => DEFAULT_SCHEMA_OPTIONS,
                (Some(schema), _) => ParquetSchemaOptions::UserProvidedSchema(schema.schema),
                (_, Some(opts)) => {
                    ParquetSchemaOptions::InferenceOptions(ParquetSchemaInferenceOptions {
                        int96_timestamps_time_unit: opts.int96_timestamps_time_unit.timeunit,
                    })
                }
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
    parent.add_class::<pylib::PyParquetSchemaInferenceOptions>()?;
    Ok(())
}
