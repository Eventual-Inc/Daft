use pyo3::prelude::*;

pub mod pylib {
    use std::{collections::BTreeMap, sync::Arc};

    use common_arrow_ffi::{field_to_py, to_py_array};
    use daft_core::python::{PySchema, PySeries, PyTimeUnit};
    use daft_dsl::python::PyExpr;
    use daft_io::{get_io_client, python::IOConfig, IOStatsContext};
    use daft_recordbatch::python::PyRecordBatch;
    use pyo3::{pyfunction, types::PyModule, Bound, PyResult, Python};

    use crate::read::{
        ArrowChunk, ParquetSchemaInferenceOptions, ParquetSchemaInferenceOptionsBuilder,
    };
    #[allow(clippy::too_many_arguments)]
    #[pyfunction(signature = (
        uri,
        columns=None,
        start_offset=None,
        num_rows=None,
        row_groups=None,
        predicate=None,
        io_config=None,
        multithreaded_io=None,
        coerce_int96_timestamp_unit=None
    ))]
    pub fn read_parquet(
        py: Python,
        uri: &str,
        columns: Option<Vec<String>>,
        start_offset: Option<usize>,
        num_rows: Option<usize>,
        row_groups: Option<Vec<i64>>,
        predicate: Option<PyExpr>,
        io_config: Option<IOConfig>,
        multithreaded_io: Option<bool>,
        coerce_int96_timestamp_unit: Option<PyTimeUnit>,
    ) -> PyResult<PyRecordBatch> {
        py.allow_threads(|| {
            let io_stats = IOStatsContext::new(format!("read_parquet: for uri {uri}"));

            let io_client = get_io_client(
                multithreaded_io.unwrap_or(true),
                io_config.unwrap_or_default().config.into(),
            )?;
            let schema_infer_options = ParquetSchemaInferenceOptions::new(
                coerce_int96_timestamp_unit.map(|tu| tu.timeunit),
            );

            let result = crate::read::read_parquet(
                uri,
                columns,
                start_offset,
                num_rows,
                row_groups,
                predicate.map(|e| e.expr),
                io_client,
                Some(io_stats),
                multithreaded_io.unwrap_or(true),
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
        pyarrow: &Bound<PyModule>,
    ) -> PyResult<PyArrowParquetType> {
        let converted_arrays = all_arrays
            .into_iter()
            .map(|v| {
                v.into_iter()
                    .map(|a| to_py_array(py, a, pyarrow).map(pyo3::Bound::unbind))
                    .collect::<PyResult<Vec<_>>>()
            })
            .collect::<PyResult<Vec<_>>>()?;
        let fields = schema
            .fields
            .iter()
            .map(|f| field_to_py(py, f, pyarrow))
            .collect::<Result<Vec<_>, _>>()?;
        let metadata = &schema.metadata;
        Ok((fields, metadata.clone(), converted_arrays, num_rows))
    }

    #[allow(clippy::too_many_arguments)]
    #[pyfunction(signature = (
        uri,
        string_encoding,
        columns=None,
        start_offset=None,
        num_rows=None,
        row_groups=None,
        io_config=None,
        multithreaded_io=None,
        coerce_int96_timestamp_unit=None,
        file_timeout_ms=None
    ))]
    pub fn read_parquet_into_pyarrow(
        py: Python,
        uri: &str,
        string_encoding: String,
        columns: Option<Vec<String>>,
        start_offset: Option<usize>,
        num_rows: Option<usize>,
        row_groups: Option<Vec<i64>>,
        io_config: Option<IOConfig>,
        multithreaded_io: Option<bool>,
        coerce_int96_timestamp_unit: Option<PyTimeUnit>,
        file_timeout_ms: Option<i64>,
    ) -> PyResult<PyArrowParquetType> {
        let (schema, all_arrays, num_rows) = py.allow_threads(|| {
            let io_client = get_io_client(
                multithreaded_io.unwrap_or(true),
                io_config.unwrap_or_default().config.into(),
            )?;
            let schema_infer_options = ParquetSchemaInferenceOptionsBuilder {
                coerce_int96_timestamp_unit,
                string_encoding,
            }
            .build()?;

            crate::read::read_parquet_into_pyarrow(
                uri,
                columns,
                start_offset,
                num_rows,
                row_groups,
                io_client,
                None,
                multithreaded_io.unwrap_or(true),
                schema_infer_options,
                file_timeout_ms,
            )
        })?;
        let pyarrow = py.import(pyo3::intern!(py, "pyarrow"))?;
        convert_pyarrow_parquet_read_result_into_py(py, schema, all_arrays, num_rows, &pyarrow)
    }
    #[allow(clippy::too_many_arguments)]
    #[pyfunction(signature = (
        uris,
        columns=None,
        start_offset=None,
        num_rows=None,
        row_groups=None,
        predicate=None,
        io_config=None,
        num_parallel_tasks=None,
        multithreaded_io=None,
        coerce_int96_timestamp_unit=None
    ))]
    pub fn read_parquet_bulk(
        py: Python,
        uris: Vec<String>,
        columns: Option<Vec<String>>,
        start_offset: Option<usize>,
        num_rows: Option<usize>,
        row_groups: Option<Vec<Option<Vec<i64>>>>,
        predicate: Option<PyExpr>,
        io_config: Option<IOConfig>,
        num_parallel_tasks: Option<i64>,
        multithreaded_io: Option<bool>,
        coerce_int96_timestamp_unit: Option<PyTimeUnit>,
    ) -> PyResult<Vec<PyRecordBatch>> {
        py.allow_threads(|| {
            let io_stats = IOStatsContext::new("read_parquet_bulk");

            let io_client = get_io_client(
                multithreaded_io.unwrap_or(true),
                io_config.unwrap_or_default().config.into(),
            )?;
            let schema_infer_options = ParquetSchemaInferenceOptions::new(
                coerce_int96_timestamp_unit.map(|tu| tu.timeunit),
            );
            Ok(crate::read::read_parquet_bulk(
                uris.iter().map(AsRef::as_ref).collect::<Vec<_>>().as_ref(),
                columns.as_deref(),
                start_offset,
                num_rows,
                row_groups,
                predicate.map(|e| e.expr),
                io_client,
                Some(io_stats),
                num_parallel_tasks.unwrap_or(128) as usize,
                multithreaded_io.unwrap_or(true),
                &schema_infer_options,
                None,
                None,
                None,
                None,
            )?
            .into_iter()
            .map(std::convert::Into::into)
            .collect())
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyfunction(signature = (
        uris,
        columns=None,
        start_offset=None,
        num_rows=None,
        row_groups=None,
        io_config=None,
        num_parallel_tasks=None,
        multithreaded_io=None,
        coerce_int96_timestamp_unit=None
    ))]
    pub fn read_parquet_into_pyarrow_bulk(
        py: Python,
        uris: Vec<String>,
        columns: Option<Vec<String>>,
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
                uris.iter().map(AsRef::as_ref).collect::<Vec<_>>().as_ref(),
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
                convert_pyarrow_parquet_read_result_into_py(py, s, all_arrays, num_rows, &pyarrow)
            })
            .collect::<PyResult<Vec<_>>>()
    }

    #[pyfunction(signature = (
        uri,
        io_config=None,
        multithreaded_io=None,
        coerce_int96_timestamp_unit=None
    ))]
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

            let runtime_handle = common_runtime::get_io_runtime(true);

            let task = async move {
                crate::read::read_parquet_schema_and_metadata(
                    uri,
                    io_client,
                    Some(io_stats),
                    schema_infer_options,
                    None, // TODO: allow passing in of field_id_mapping through Python API?
                )
                .await
            };

            let (schema, _) = runtime_handle.block_on_current_thread(task)?;

            Ok(Arc::new(schema).into())
        })
    }

    #[pyfunction(signature = (uris, io_config=None, multithreaded_io=None))]
    pub fn read_parquet_statistics(
        py: Python,
        uris: PySeries,
        io_config: Option<IOConfig>,
        multithreaded_io: Option<bool>,
    ) -> PyResult<PyRecordBatch> {
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
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction!(pylib::read_parquet, parent)?)?;
    parent.add_function(wrap_pyfunction!(pylib::read_parquet_into_pyarrow, parent)?)?;
    parent.add_function(wrap_pyfunction!(
        pylib::read_parquet_into_pyarrow_bulk,
        parent
    )?)?;
    parent.add_function(wrap_pyfunction!(pylib::read_parquet_bulk, parent)?)?;
    parent.add_function(wrap_pyfunction!(pylib::read_parquet_schema, parent)?)?;
    parent.add_function(wrap_pyfunction!(pylib::read_parquet_statistics, parent)?)?;
    Ok(())
}
