use pyo3::prelude::*;

pub mod pylib {
    use daft_core::{
        ffi::field_to_py,
        python::{datatype::PyTimeUnit, schema::PySchema, PySeries},
    };
    use daft_dsl::python::PyExpr;
    use daft_io::{get_io_client, python::IOConfig, IOStatsContext};
    use daft_table::python::PyTable;
    use pyo3::{pyfunction, types::PyModule, PyResult, Python};
    use std::{collections::BTreeMap, sync::Arc};

    use crate::read::{ArrowChunk, ParquetSchemaInferenceOptions};
    use daft_core::ffi::to_py_array;
    #[allow(clippy::too_many_arguments)]
    #[pyfunction]
    pub fn read_parquet(
        py: Python,
        uri: &str,
        columns: Option<Vec<&str>>,
        start_offset: Option<usize>,
        num_rows: Option<usize>,
        row_groups: Option<Vec<i64>>,
        predicate: Option<PyExpr>,
        io_config: Option<IOConfig>,
        multithreaded_io: Option<bool>,
        coerce_int96_timestamp_unit: Option<PyTimeUnit>,
    ) -> PyResult<PyTable> {
        py.allow_threads(|| {
            let io_stats = IOStatsContext::new(format!("read_parquet: for uri {uri}"));

            let io_client = get_io_client(
                multithreaded_io.unwrap_or(true),
                io_config.unwrap_or_default().config.into(),
            )?;
            let schema_infer_options = ParquetSchemaInferenceOptions::new(
                coerce_int96_timestamp_unit.map(|tu| tu.timeunit),
            );
            let runtime_handle = daft_io::get_runtime(multithreaded_io.unwrap_or(true))?;

            let result = crate::read::read_parquet(
                uri,
                columns.as_deref(),
                start_offset,
                num_rows,
                row_groups,
                predicate.map(|e| e.expr),
                io_client,
                Some(io_stats.clone()),
                runtime_handle,
                schema_infer_options,
                None,
            )?
            .into();
            Ok(result)
        })
    }
    type PyArrowChunks = Vec<Vec<pyo3::PyObject>>;
    type PyArrowFields = Vec<pyo3::PyObject>;
    type PyArrowParquetType = (
        PyArrowFields,
        BTreeMap<String, String>,
        PyArrowChunks,
        usize,
    );
    fn convert_pyarrow_parquet_read_result_into_py(
        py: Python,
        schema: arrow2::datatypes::SchemaRef,
        all_arrays: Vec<ArrowChunk>,
        num_rows: usize,
        pyarrow: &PyModule,
    ) -> PyResult<PyArrowParquetType> {
        let converted_arrays = all_arrays
            .into_iter()
            .map(|v| {
                v.into_iter()
                    .map(|a| to_py_array(a, py, pyarrow))
                    .collect::<PyResult<Vec<_>>>()
            })
            .collect::<PyResult<Vec<_>>>()?;
        let fields = schema
            .fields
            .iter()
            .map(|f| field_to_py(f, py, pyarrow))
            .collect::<Result<Vec<_>, _>>()?;
        let metadata = &schema.metadata;
        Ok((fields, metadata.clone(), converted_arrays, num_rows))
    }

    #[allow(clippy::too_many_arguments)]
    #[pyfunction]
    pub fn read_parquet_into_pyarrow(
        py: Python,
        uri: &str,
        columns: Option<Vec<&str>>,
        start_offset: Option<usize>,
        num_rows: Option<usize>,
        row_groups: Option<Vec<i64>>,
        io_config: Option<IOConfig>,
        multithreaded_io: Option<bool>,
        coerce_int96_timestamp_unit: Option<PyTimeUnit>,
        file_timeout_ms: Option<i64>,
    ) -> PyResult<PyArrowParquetType> {
        let read_parquet_result = py.allow_threads(|| {
            let io_client = get_io_client(
                multithreaded_io.unwrap_or(true),
                io_config.unwrap_or_default().config.into(),
            )?;
            let schema_infer_options = ParquetSchemaInferenceOptions::new(
                coerce_int96_timestamp_unit.map(|tu| tu.timeunit),
            );

            let runtime_handle = daft_io::get_runtime(multithreaded_io.unwrap_or(true))?;

            crate::read::read_parquet_into_pyarrow(
                uri,
                columns.as_deref(),
                start_offset,
                num_rows,
                row_groups,
                io_client,
                None,
                runtime_handle,
                schema_infer_options,
                file_timeout_ms,
            )
        })?;
        let (schema, all_arrays, num_rows) = read_parquet_result;
        let pyarrow = py.import(pyo3::intern!(py, "pyarrow"))?;
        convert_pyarrow_parquet_read_result_into_py(py, schema, all_arrays, num_rows, pyarrow)
    }
    #[allow(clippy::too_many_arguments)]
    #[pyfunction]
    pub fn read_parquet_bulk(
        py: Python,
        uris: Vec<&str>,
        columns: Option<Vec<&str>>,
        start_offset: Option<usize>,
        num_rows: Option<usize>,
        row_groups: Option<Vec<Option<Vec<i64>>>>,
        predicate: Option<PyExpr>,
        io_config: Option<IOConfig>,
        num_parallel_tasks: Option<i64>,
        multithreaded_io: Option<bool>,
        coerce_int96_timestamp_unit: Option<PyTimeUnit>,
    ) -> PyResult<Vec<PyTable>> {
        py.allow_threads(|| {
            let io_stats = IOStatsContext::new("read_parquet_bulk");

            let io_client = get_io_client(
                multithreaded_io.unwrap_or(true),
                io_config.unwrap_or_default().config.into(),
            )?;
            let schema_infer_options = ParquetSchemaInferenceOptions::new(
                coerce_int96_timestamp_unit.map(|tu| tu.timeunit),
            );
            let runtime_handle = daft_io::get_runtime(multithreaded_io.unwrap_or(true))?;

            Ok(crate::read::read_parquet_bulk(
                uris.as_ref(),
                columns.as_deref(),
                start_offset,
                num_rows,
                row_groups,
                predicate.map(|e| e.expr),
                io_client,
                Some(io_stats),
                num_parallel_tasks.unwrap_or(128) as usize,
                runtime_handle,
                &schema_infer_options,
                None,
                None,
                None,
            )?
            .into_iter()
            .map(|v| v.into())
            .collect())
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyfunction]
    pub fn read_parquet_into_pyarrow_bulk(
        py: Python,
        uris: Vec<&str>,
        columns: Option<Vec<&str>>,
        start_offset: Option<usize>,
        num_rows: Option<usize>,
        row_groups: Option<Vec<Option<Vec<i64>>>>,
        io_config: Option<IOConfig>,
        num_parallel_tasks: Option<i64>,
        multithreaded_io: Option<bool>,
        coerce_int96_timestamp_unit: Option<PyTimeUnit>,
    ) -> PyResult<Vec<PyArrowParquetType>> {
        let parquet_read_results = py.allow_threads(|| {
            let io_client = get_io_client(
                multithreaded_io.unwrap_or(true),
                io_config.unwrap_or_default().config.into(),
            )?;
            let schema_infer_options = ParquetSchemaInferenceOptions::new(
                coerce_int96_timestamp_unit.map(|tu| tu.timeunit),
            );

            crate::read::read_parquet_into_pyarrow_bulk(
                uris.as_ref(),
                columns.as_deref(),
                start_offset,
                num_rows,
                row_groups,
                io_client,
                None,
                num_parallel_tasks.unwrap_or(128) as usize,
                multithreaded_io.unwrap_or(true),
                schema_infer_options,
            )
        })?;
        let pyarrow = py.import(pyo3::intern!(py, "pyarrow"))?;
        parquet_read_results
            .into_iter()
            .map(|(s, all_arrays, num_rows)| {
                convert_pyarrow_parquet_read_result_into_py(py, s, all_arrays, num_rows, pyarrow)
            })
            .collect::<PyResult<Vec<_>>>()
    }

    #[pyfunction]
    pub fn read_parquet_schema(
        py: Python,
        uri: &str,
        io_config: Option<IOConfig>,
        multithreaded_io: Option<bool>,
        coerce_int96_timestamp_unit: Option<PyTimeUnit>,
    ) -> PyResult<PySchema> {
        py.allow_threads(|| {
            let io_stats = IOStatsContext::new(format!("read_parquet_schema: for uri {uri}"));

            let schema_infer_options = ParquetSchemaInferenceOptions::new(
                coerce_int96_timestamp_unit.map(|tu| tu.timeunit),
            );
            let io_client = get_io_client(
                multithreaded_io.unwrap_or(true),
                io_config.unwrap_or_default().config.into(),
            )?;
            Ok(Arc::new(
                crate::read::read_parquet_schema(
                    uri,
                    io_client,
                    Some(io_stats),
                    schema_infer_options,
                    None, // TODO: allow passing in of field_id_mapping through Python API?
                )?
                .0,
            )
            .into())
        })
    }

    #[pyfunction]
    pub fn read_parquet_statistics(
        py: Python,
        uris: PySeries,
        io_config: Option<IOConfig>,
        multithreaded_io: Option<bool>,
    ) -> PyResult<PyTable> {
        py.allow_threads(|| {
            let io_stats = IOStatsContext::new("read_parquet_statistics");

            let io_client = get_io_client(
                multithreaded_io.unwrap_or(true),
                io_config.unwrap_or_default().config.into(),
            )?;
            Ok(
                crate::read::read_parquet_statistics(
                    &uris.series,
                    io_client,
                    Some(io_stats),
                    None,
                )?
                .into(),
            )
        })
    }
}
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_wrapped(wrap_pyfunction!(pylib::read_parquet))?;
    parent.add_wrapped(wrap_pyfunction!(pylib::read_parquet_into_pyarrow))?;
    parent.add_wrapped(wrap_pyfunction!(pylib::read_parquet_into_pyarrow_bulk))?;
    parent.add_wrapped(wrap_pyfunction!(pylib::read_parquet_bulk))?;
    parent.add_wrapped(wrap_pyfunction!(pylib::read_parquet_schema))?;
    parent.add_wrapped(wrap_pyfunction!(pylib::read_parquet_statistics))?;
    Ok(())
}
