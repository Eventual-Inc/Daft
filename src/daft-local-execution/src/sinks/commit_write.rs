use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_file_formats::{FileFormat, WriteMode};
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::OutputFileInfo;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_writers::WriterFactory;
use itertools::Itertools;
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeOutput, BlockingSinkFinalizeResult, BlockingSinkSinkResult,
    BlockingSinkStatus,
};
use crate::{ExecutionTaskSpawner, pipeline::NodeName};

pub(crate) struct CommitWriteState {
    written_file_path_record_batches: Vec<RecordBatch>,
}

impl CommitWriteState {
    pub fn new() -> Self {
        Self {
            written_file_path_record_batches: vec![],
        }
    }

    pub fn append(&mut self, input: impl IntoIterator<Item = RecordBatch>) {
        self.written_file_path_record_batches.extend(input);
    }
}

pub(crate) struct CommitWriteSink {
    data_schema: SchemaRef,
    file_schema: SchemaRef,
    file_info: OutputFileInfo<BoundExpr>,
}

impl CommitWriteSink {
    pub(crate) fn new(
        data_schema: SchemaRef,
        file_schema: SchemaRef,
        file_info: OutputFileInfo<BoundExpr>,
    ) -> Self {
        Self {
            data_schema,
            file_schema,
            file_info,
        }
    }
}

impl BlockingSink for CommitWriteSink {
    type State = CommitWriteState;

    #[instrument(skip_all, name = "CommitWriteSink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        _spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        state.append(input.record_batches().iter().cloned());
        Ok(BlockingSinkStatus::NeedMoreInput(state)).into()
    }

    #[instrument(skip_all, name = "WriteSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult<Self> {
        let data_schema = self.data_schema.clone();
        let file_schema = self.file_schema.clone();
        let file_info = self.file_info.clone();
        spawner
            .spawn(
                async move {
                    let mut written_file_path_record_batches = states
                        .into_iter()
                        .flat_map(|mut state| {
                            std::mem::take(&mut state.written_file_path_record_batches)
                        })
                        .collect::<Vec<_>>();

                    // If nothing was written, create an empty file for Parquet/CSV when not partitioned
                    if written_file_path_record_batches.is_empty()
                        && file_info.partition_cols.is_none()
                    {
                        match file_info.file_format {
                            FileFormat::Parquet | FileFormat::Csv => {
                                let writer_factory =
                                    daft_writers::physical::PhysicalWriterFactory::new(
                                        file_info.clone(),
                                        data_schema.clone(),
                                        true,
                                    )?;
                                let mut writer = writer_factory.create_writer(0, None)?;
                                let empty_rb = RecordBatch::empty(Some(data_schema.clone()));
                                let empty_mp = Arc::new(MicroPartition::new_loaded(
                                    data_schema.clone(),
                                    vec![empty_rb].into(),
                                    None,
                                ));
                                writer.write(empty_mp).await?;
                                if let Some(rb) = writer.close().await? {
                                    written_file_path_record_batches.push(rb);
                                }
                            }
                            _ => {
                                log::warn!(
                                    "No data written for {:?} file format, and not empty file created.",
                                    file_info.file_format
                                );
                            }
                        }
                    }

                    if matches!(
                        file_info.write_mode,
                        WriteMode::Overwrite | WriteMode::OverwritePartitions
                    ) {
                        #[cfg(feature = "python")]
                        {
                            use pyo3::{prelude::*, types::PyList};

                            Python::attach(|py| {
                                let fs = py.import(pyo3::intern!(py, "daft.filesystem"))?;
                                let overwrite_files = fs.getattr("overwrite_files")?;
                                let file_paths = written_file_path_record_batches
                                    .iter()
                                    .flat_map(|res| {
                                        let path_index = res
                                            .schema
                                            .get_index("path")
                                            .expect("path to be a column");

                                        let s = res.get_column(path_index);
                                        s.utf8()
                                            .expect("path to be utf8")
                                            .into_iter()
                                            .filter_map(|s| s.map(|s| s.to_string()))
                                    })
                                    .collect::<Vec<_>>();
                                let file_paths = PyList::new(py, file_paths).expect("file_paths");
                                let root_dir = file_info.root_dir.clone();
                                let py_io_config = file_info
                                    .io_config
                                    .clone()
                                    .map(|io_conf| daft_io::python::IOConfig { config: io_conf });
                                let overwrite_partitions =
                                    matches!(file_info.write_mode, WriteMode::OverwritePartitions);
                                overwrite_files.call1((
                                    file_paths,
                                    root_dir,
                                    py_io_config,
                                    overwrite_partitions,
                                ))?;

                                PyResult::Ok(())
                            })
                            .map_err(DaftError::PyO3Error)?;
                        }
                        #[cfg(not(feature = "python"))]
                        {
                            unimplemented!(
                                "Overwrite mode is not supported without the Python feature."
                            )
                        }
                    }
                    let written_file_paths_mp = MicroPartition::new_loaded(
                        file_schema,
                        written_file_path_record_batches.into(),
                        None,
                    );
                    Ok(BlockingSinkFinalizeOutput::Finished(vec![Arc::new(
                        written_file_paths_mp,
                    )]))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "Commit Write".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::CommitWrite
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        Ok(CommitWriteState::new())
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut lines = vec![];
        lines.push("Commit Write".to_string());
        if matches!(self.file_info.write_mode, WriteMode::Overwrite) {
            lines.push("Overwrite".to_string());
        } else if matches!(self.file_info.write_mode, WriteMode::OverwritePartitions) {
            lines.push(format!(
                "OverwritePartitions: {:?}",
                self.file_info
                    .partition_cols
                    .as_ref()
                    .expect("Partition_cols to be present for OverwritePartitions")
                    .iter()
                    .map(|col| col.to_string())
                    .join(", ")
            ));
        }
        lines
    }

    fn max_concurrency(&self) -> usize {
        1
    }
}
