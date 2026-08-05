use pyo3::prelude::*;

pub mod pylib {
    use std::sync::Arc;

    use common_arrow_ffi::ToPyArrow;
    use daft_core::python::{PySchema, PySeries, PyTimeUnit};
    use daft_dsl::python::PyExpr;
    use daft_io::{IOStatsContext, get_io_client, python::IOConfig};
    use daft_recordbatch::python::PyRecordBatch;
    use pyo3::{PyResult, Python, pyfunction};

    use crate::read::{
        ArrowChunk, ParquetBulkReadOptions, ParquetReadOptions, ParquetSchemaInferenceOptions,
        PerFileOptions,
    };

    /// Expand a bulk `row_groups` argument into one `PerFileOptions` per uri.
    fn per_file_from_row_groups(
        row_groups: Option<&[Option<Vec<i64>>]>,
        uris_len: usize,
    ) -> Vec<PerFileOptions> {
        let Some(rgs) = row_groups else {
            return Vec::new();
        };
        assert_eq!(rgs.len(), uris_len, "row_groups length mismatch");
        rgs.iter()
            .map(|r| PerFileOptions {
                row_groups: r.clone(),
                ..Default::default()
            })
            .collect()
    }

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
        py.detach(|| {
            let io_stats = IOStatsContext::new(format!("read_parquet: for uri {uri}"));
            let multithreaded_io = multithreaded_io.unwrap_or(true);
            let io_client = get_io_client(
                multithreaded_io,
                io_config.unwrap_or_default().config.into(),
            )?;
            let opts = ParquetReadOptions {
                columns,
                start_offset,
                num_rows,
                row_groups,
                predicate: predicate.map(|e| e.expr),
                schema_infer: ParquetSchemaInferenceOptions::new(
                    coerce_int96_timestamp_unit.map(|tu| tu.timeunit),
                ),
                ..Default::default()
            };
            let runtime = common_runtime::get_io_runtime(multithreaded_io);
            let table = runtime.block_on_current_thread(
                crate::read::read_parquet_into_recordbatch(uri, io_client, Some(io_stats), opts),
            )?;
            Ok(table.into())
        })
    }

    type PyArrowChunks = Vec<Vec<pyo3::Py<pyo3::PyAny>>>;
    type PyArrowFields = Vec<pyo3::Py<pyo3::PyAny>>;
    type PyArrowParquetType = (
        PyArrowFields,
        std::collections::BTreeMap<String, String>,
        PyArrowChunks,
        usize,
    );
    fn convert_pyarrow_parquet_read_result_into_py(
        py: Python,
        schema: arrow::datatypes::SchemaRef,
        all_arrays: Vec<ArrowChunk>,
        num_rows: usize,
    ) -> PyResult<PyArrowParquetType> {
        let converted_arrays = all_arrays
            .into_iter()
            .map(|v| {
                v.into_iter()
                    .map(|a| Ok(a.to_data().to_pyarrow(py)?.unbind()))
                    .collect::<PyResult<Vec<_>>>()
            })
            .collect::<PyResult<Vec<_>>>()?;
        let fields = schema
            .fields
            .iter()
            .map(|f| Ok(f.to_pyarrow(py)?.unbind()))
            .collect::<PyResult<Vec<_>>>()?;
        Ok((
            fields,
            schema.metadata.clone().into_iter().collect(),
            converted_arrays,
            num_rows,
        ))
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
        let (schema, all_arrays, num_rows) = py.detach(|| {
            let multithreaded_io = multithreaded_io.unwrap_or(true);
            let io_client = get_io_client(
                multithreaded_io,
                io_config.unwrap_or_default().config.into(),
            )?;
            let opts = ParquetReadOptions {
                columns,
                start_offset,
                num_rows,
                row_groups,
                schema_infer: ParquetSchemaInferenceOptions::from_python(
                    coerce_int96_timestamp_unit,
                    &string_encoding,
                )?,
                ..Default::default()
            };
            crate::read::read_parquet_into_pyarrow(
                uri,
                io_client,
                None,
                multithreaded_io,
                opts,
                file_timeout_ms,
            )
        })?;
        convert_pyarrow_parquet_read_result_into_py(py, schema, all_arrays, num_rows)
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
        py.detach(|| {
            let io_stats = IOStatsContext::new("read_parquet_bulk");
            let multithreaded_io = multithreaded_io.unwrap_or(true);
            let io_client = get_io_client(
                multithreaded_io,
                io_config.unwrap_or_default().config.into(),
            )?;
            let per_file = per_file_from_row_groups(row_groups.as_deref(), uris.len());
            let opts = ParquetBulkReadOptions {
                columns,
                start_offset,
                num_rows,
                predicate: predicate.map(|e| e.expr),
                schema_infer: ParquetSchemaInferenceOptions::new(
                    coerce_int96_timestamp_unit.map(|tu| tu.timeunit),
                ),
                num_parallel_tasks: num_parallel_tasks.unwrap_or(128) as usize,
                per_file,
                ..Default::default()
            };
            let uri_refs: Vec<&str> = uris.iter().map(String::as_str).collect();
            let tables = crate::read::read_parquet_bulk_sync(
                &uri_refs,
                io_client,
                Some(io_stats),
                multithreaded_io,
                opts,
            )?;
            Ok(tables.into_iter().map(Into::into).collect())
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
        let results = py.detach(|| {
            let multithreaded_io = multithreaded_io.unwrap_or(true);
            let io_client = get_io_client(
                multithreaded_io,
                io_config.unwrap_or_default().config.into(),
            )?;
            let per_file = per_file_from_row_groups(row_groups.as_deref(), uris.len());
            let opts = ParquetBulkReadOptions {
                columns,
                start_offset,
                num_rows,
                schema_infer: ParquetSchemaInferenceOptions::new(
                    coerce_int96_timestamp_unit.map(|tu| tu.timeunit),
                ),
                num_parallel_tasks: num_parallel_tasks.unwrap_or(128) as usize,
                per_file,
                ..Default::default()
            };
            let uri_refs: Vec<&str> = uris.iter().map(String::as_str).collect();
            crate::read::read_parquet_into_pyarrow_bulk(
                &uri_refs,
                io_client,
                None,
                multithreaded_io,
                opts,
            )
        })?;
        results
            .into_iter()
            .map(|(s, all_arrays, n)| {
                convert_pyarrow_parquet_read_result_into_py(py, s, all_arrays, n)
            })
            .collect()
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
        py.detach(|| {
            let io_stats = IOStatsContext::new(format!("read_parquet_schema: for uri {uri}"));
            let schema_infer = ParquetSchemaInferenceOptions::new(
                coerce_int96_timestamp_unit.map(|tu| tu.timeunit),
            );
            let io_client = get_io_client(
                multithreaded_io.unwrap_or(true),
                io_config.unwrap_or_default().config.into(),
            )?;
            let runtime = common_runtime::get_io_runtime(true);
            let (schema, _) =
                runtime.block_on_current_thread(crate::read::read_parquet_schema_and_metadata(
                    uri,
                    io_client,
                    Some(io_stats),
                    schema_infer,
                    None,
                ))?;
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
        py.detach(|| {
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
