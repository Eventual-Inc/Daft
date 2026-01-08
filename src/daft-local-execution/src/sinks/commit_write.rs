use std::{collections::HashSet, sync::Arc};

use common_error::DaftResult;
use common_file_formats::{FileFormat, WriteMode};
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_io::{Error, IOClient, get_io_client, parse_url};
use daft_logical_plan::OutputFileInfo;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_writers::WriterFactory;
use futures::TryStreamExt;
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
                        let (_, root_uri) = parse_url(&file_info.root_dir)?;
                        let scheme = root_uri.split("://").next().unwrap_or("file");

                        let written_paths: Vec<String> = written_file_path_record_batches
                            .iter()
                            .flat_map(|res| {
                                let path_index =
                                    res.schema.get_index("path").expect("path to be a column");
                                let s = res.get_column(path_index);
                                s.utf8()
                                    .expect("path to be utf8")
                                    .into_iter()
                                    .filter_map(|s| s.map(|s| s.to_string()))
                            })
                            .map(|p| {
                                if p.contains("://") {
                                    p
                                } else {
                                    format!("{}://{}", scheme, p)
                                }
                            })
                            .collect();

                        overwrite_files(
                            root_uri.to_string(),
                            written_paths,
                            get_io_client(true, file_info.io_config.unwrap_or_default().into())?,
                            matches!(file_info.write_mode, WriteMode::OverwritePartitions),
                        )
                        .await?;
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

async fn glob_files(
    source: Arc<dyn daft_io::ObjectSource>,
    glob_path: &str,
) -> DaftResult<Vec<String>> {
    let glob_pattern = format!("{}/**", glob_path.trim_end_matches('/'));
    let mut out: Vec<String> = vec![];
    let mut stream = source
        .glob(
            glob_pattern.as_str(),
            Some(1024),
            Some(1000),
            None,
            None,
            None,
        )
        .await?;
    match stream.try_next().await {
        Ok(Some(first)) => {
            if matches!(first.filetype, daft_io::FileType::File) {
                out.push(first.filepath);
            }

            while let Some(meta) = stream.try_next().await? {
                if matches!(meta.filetype, daft_io::FileType::File) {
                    out.push(meta.filepath);
                }
            }
        }
        Ok(None) => {
            log::debug!("No files found for glob pattern: {:?}", glob_pattern);
        }
        Err(Error::NotFound { .. }) => {
            log::debug!("No files found for glob pattern: {:?}", glob_pattern);
        }
        Err(err) => return Err(err.into()),
    }
    Ok(out)
}

async fn overwrite_files(
    root_uri: String,
    new_files: Vec<String>,
    io_client: Arc<IOClient>,
    overwrite_partitions: bool,
) -> DaftResult<()> {
    let source = io_client.get_source(&root_uri).await?;
    let mut all_files: Vec<String> = vec![];

    if overwrite_partitions {
        let mut partition_dirs: HashSet<String> = HashSet::new();
        for f in &new_files {
            if let Some(idx) = f.rfind('/') {
                partition_dirs.insert(f[..idx].to_string());
            }
        }
        for dir in partition_dirs {
            let mut files = glob_files(source.clone(), &dir).await?;
            all_files.append(&mut files);
        }
    } else {
        let mut files = glob_files(source.clone(), &root_uri).await?;
        all_files.append(&mut files);
    }

    // Delete files that are not among new_files
    let written_set: HashSet<&str> = new_files.iter().map(|s| s.as_str()).collect();
    for f in all_files {
        if !written_set.contains(f.as_str()) {
            source.delete(&f, None).await?;
        }
    }

    Ok(())
}
