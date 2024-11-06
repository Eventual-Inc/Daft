use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_table::Table;
use daft_writers::{FileWriter, WriterFactory};
use tracing::instrument;

use super::blocking_sink::{BlockingSink, BlockingSinkState, BlockingSinkStatus};
use crate::{
    dispatcher::{Dispatcher, PartitionedDispatcher, RoundRobinBufferedDispatcher},
    pipeline::PipelineResultType,
    NUM_CPUS,
};

pub enum WriteFormat {
    Parquet,
    PartitionedParquet,
    Csv,
    PartitionedCsv,
    Iceberg,
    PartitionedIceberg,
    Deltalake,
    PartitionedDeltalake,
}

struct WriteState {
    writer: Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Vec<Table>>>,
}

impl WriteState {
    pub fn new(
        writer: Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Vec<Table>>>,
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
    writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<Table>>>,
    partition_by: Option<Vec<ExprRef>>,
    file_schema: SchemaRef,
}

impl WriteSink {
    pub(crate) fn new(
        write_format: WriteFormat,
        writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<Table>>>,
        partition_by: Option<Vec<ExprRef>>,
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
        input: &Arc<MicroPartition>,
        mut state: Box<dyn BlockingSinkState>,
    ) -> DaftResult<BlockingSinkStatus> {
        state
            .as_any_mut()
            .downcast_mut::<WriteState>()
            .expect("WriteSink should have WriteState")
            .writer
            .write(input)?;
        Ok(BlockingSinkStatus::NeedMoreInput(state))
    }

    #[instrument(skip_all, name = "WriteSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
    ) -> DaftResult<Option<PipelineResultType>> {
        let mut results = vec![];
        for mut state in states {
            let state = state
                .as_any_mut()
                .downcast_mut::<WriteState>()
                .expect("State type mismatch");
            results.extend(state.writer.close()?);
        }
        let mp = Arc::new(MicroPartition::new_loaded(
            self.file_schema.clone(),
            results.into(),
            None,
        ));
        Ok(Some(mp.into()))
    }

    fn name(&self) -> &'static str {
        match self.write_format {
            WriteFormat::Parquet => "ParquetSink",
            WriteFormat::PartitionedParquet => "PartitionedParquetSink",
            WriteFormat::Csv => "CsvSink",
            WriteFormat::PartitionedCsv => "PartitionedCsvSink",
            WriteFormat::Iceberg => "IcebergSink",
            WriteFormat::PartitionedIceberg => "PartitionedIcebergSink",
            WriteFormat::Deltalake => "DeltalakeSink",
            WriteFormat::PartitionedDeltalake => "PartitionedDeltalakeSink",
        }
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        let writer = self.writer_factory.create_writer(0, None)?;
        Ok(Box::new(WriteState::new(writer)) as Box<dyn BlockingSinkState>)
    }

    fn make_dispatcher(
        &self,
        runtime_handle: &crate::ExecutionRuntimeHandle,
    ) -> Arc<dyn Dispatcher> {
        if let Some(partition_by) = &self.partition_by {
            Arc::new(PartitionedDispatcher::new(partition_by.clone()))
        } else {
            Arc::new(RoundRobinBufferedDispatcher::new(
                runtime_handle.default_morsel_size(),
            ))
        }
    }

    fn max_concurrency(&self) -> usize {
        if self.partition_by.is_some() {
            *NUM_CPUS
        } else {
            1
        }
    }
}
