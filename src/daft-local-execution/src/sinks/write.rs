use std::sync::Arc;

use common_error::DaftResult;
use common_runtime::get_compute_pool_num_threads;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_writers::{AsyncFileWriter, WriterFactory};
use tracing::{instrument, Span};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult, BlockingSinkState,
    BlockingSinkStatus,
};
use crate::{
    dispatcher::{DispatchSpawner, PartitionedDispatcher, UnorderedDispatcher},
    ExecutionRuntimeContext, ExecutionTaskSpawner,
};

#[derive(Debug)]
pub enum WriteFormat {
    Parquet,
    PartitionedParquet,
    Csv,
    PartitionedCsv,
    Json,
    PartitionedJson,
    Iceberg,
    PartitionedIceberg,
    Deltalake,
    PartitionedDeltalake,
    Lance,
    DataSink,
}

struct WriteState {
    writer: Box<dyn AsyncFileWriter<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>>,
}

impl WriteState {
    pub fn new(
        writer: Box<dyn AsyncFileWriter<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>>,
    ) -> Self {
        Self { writer }
    }
}

impl BlockingSinkState for WriteState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub(crate) struct WriteSink {
    write_format: WriteFormat,
    writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>>,
    partition_by: Option<Vec<BoundExpr>>,
    file_schema: SchemaRef,
}

impl WriteSink {
    pub(crate) fn new(
        write_format: WriteFormat,
        writer_factory: Arc<
            dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>,
        >,
        partition_by: Option<Vec<BoundExpr>>,
        file_schema: SchemaRef,
    ) -> Self {
        Self {
            write_format,
            writer_factory,
            partition_by,
            file_schema,
        }
    }
}

impl BlockingSink for WriteSink {
    #[instrument(skip_all, name = "WriteSink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn BlockingSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult {
        spawner
            .spawn(
                async move {
                    state
                        .as_any_mut()
                        .downcast_mut::<WriteState>()
                        .expect("WriteSink should have WriteState")
                        .writer
                        .write(input)
                        .await?;
                    Ok(BlockingSinkStatus::NeedMoreInput(state))
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "WriteSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let file_schema = self.file_schema.clone();
        spawner
            .spawn(
                async move {
                    let mut results = vec![];
                    for mut state in states {
                        let state = state
                            .as_any_mut()
                            .downcast_mut::<WriteState>()
                            .expect("State type mismatch");
                        results.extend(state.writer.close().await?);
                    }
                    let mp = Arc::new(MicroPartition::new_loaded(
                        file_schema,
                        results.into(),
                        None,
                    ));
                    Ok(Some(mp))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> &'static str {
        match self.write_format {
            WriteFormat::Parquet => "ParquetSink",
            WriteFormat::PartitionedParquet => "PartitionedParquetSink",
            WriteFormat::Csv => "CsvSink",
            WriteFormat::PartitionedCsv => "PartitionedCsvSink",
            WriteFormat::Json => "JsonSink",
            WriteFormat::PartitionedJson => "PartitionedJsonSink",
            WriteFormat::Iceberg => "IcebergSink",
            WriteFormat::PartitionedIceberg => "PartitionedIcebergSink",
            WriteFormat::Deltalake => "DeltalakeSink",
            WriteFormat::PartitionedDeltalake => "PartitionedDeltalakeSink",
            WriteFormat::Lance => "LanceSink",
            WriteFormat::DataSink => "DataSink",
        }
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        let writer = self.writer_factory.create_writer(0, None)?;
        Ok(Box::new(WriteState::new(writer)) as Box<dyn BlockingSinkState>)
    }

    fn dispatch_spawner(
        &self,
        _runtime_handle: &ExecutionRuntimeContext,
    ) -> Arc<dyn DispatchSpawner> {
        if let Some(partition_by) = &self.partition_by {
            Arc::new(PartitionedDispatcher::new(partition_by.clone()))
        } else {
            // Unnecessary to buffer by morsel size because we are writing.
            // Writers also have their own internal buffering.
            Arc::new(UnorderedDispatcher::unbounded())
        }
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut lines = vec![];
        lines.push(format!("Write: {:?}", self.write_format));
        if let Some(partition_by) = &self.partition_by {
            lines.push(format!("Partition by: {:?}", partition_by));
        }
        lines
    }

    fn max_concurrency(&self) -> usize {
        if self.partition_by.is_some() {
            get_compute_pool_num_threads()
        } else {
            1
        }
    }
}
