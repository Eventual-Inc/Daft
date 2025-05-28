use pyo3::prelude::*;

pub mod pylib {
    use common_daft_config::PyDaftExecutionConfig;
    use common_file_formats::{FileFormat, WriteMode};
    use daft_dsl::python::PyExpr;
    use daft_io::python::IOConfig;
    use daft_logical_plan::OutputFileInfo;
    use daft_micropartition::python::PyMicroPartition;
    use daft_recordbatch::python::PyRecordBatch;
    use pyo3::{pyfunction, Bound, PyAny, PyResult, Python};

    use crate::make_physical_writer_factory;

    #[allow(clippy::too_many_arguments)]
    #[pyfunction(signature = (micropartition, root_dir, write_mode, partition_cols, compression, io_config, execution_cfg))]
    pub fn write_micropartition_to_parquet<'py>(
        py: Python<'py>,
        micropartition: PyMicroPartition,
        root_dir: String,
        write_mode: String,
        partition_cols: Option<Vec<PyExpr>>,
        compression: Option<String>,
        io_config: Option<IOConfig>,
        execution_cfg: &PyDaftExecutionConfig,
    ) -> PyResult<Bound<'py, PyAny>> {
        let partition_cols =
            partition_cols.map(|cols| cols.into_iter().map(Into::into).collect::<Vec<_>>());
        let io_config = io_config.unwrap_or_default().config.into();
        let file_info = OutputFileInfo::new(
            root_dir,
            WriteMode::from_str(&write_mode)?,
            FileFormat::Parquet,
            partition_cols,
            compression,
            io_config,
        );
        let micropartition = micropartition.inner;
        let cfg = execution_cfg.config.clone();

        let future = async move {
            let writer_factory =
                make_physical_writer_factory(&file_info, &micropartition.schema(), &cfg)?;
            let mut writer = writer_factory.create_writer(0, None)?;
            writer.write(micropartition).await?;
            let record_batches = writer.close().await?;
            Ok(record_batches
                .into_iter()
                .map(|batch| PyRecordBatch {
                    record_batch: batch,
                })
                .collect::<Vec<_>>())
        };

        let locals = pyo3_async_runtimes::TaskLocals::with_running_loop(py)?.copy_context(py)?;
        pyo3_async_runtimes::tokio::future_into_py_with_locals(py, locals, future)
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction!(
        pylib::write_micropartition_to_parquet,
        parent
    )?)?;
    Ok(())
}
