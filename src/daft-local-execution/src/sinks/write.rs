use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_table::Table;
use daft_writers::{FileWriter, WriterFactory};
use tracing::instrument;

use super::blocking_sink::{
    BlockingSink, BlockingSinkState, BlockingSinkStatus, DynBlockingSinkState,
};
use crate::{
    dispatcher::{Dispatcher, PartitionedDispatcher, RoundRobinBufferedDispatcher},
    pipeline::PipelineResultType,
    NUM_CPUS,
};

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

impl DynBlockingSinkState for WriteState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub(crate) struct WriteSink {
    name: &'static str,
    writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<Table>>>,
    partition_by: Option<Vec<ExprRef>>,
    file_schema: SchemaRef,
}

impl WriteSink {
    pub(crate) fn new(
        name: &'static str,
        writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<Table>>>,
        partition_by: Option<Vec<ExprRef>>,
        file_schema: SchemaRef,
    ) -> Self {
        Self {
            name,
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
        state_handle: &BlockingSinkState,
    ) -> DaftResult<BlockingSinkStatus> {
        state_handle.with_state_mut::<WriteState, _, _>(|state| {
            state.writer.write(input)?;
            Ok(BlockingSinkStatus::NeedMoreInput)
        })
    }

    #[instrument(skip_all, name = "WriteSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn DynBlockingSinkState>>,
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
        self.name
    }

    fn make_state(&self) -> DaftResult<Box<dyn DynBlockingSinkState>> {
        let writer = self.writer_factory.create_writer(0, None)?;
        Ok(Box::new(WriteState::new(writer)) as Box<dyn DynBlockingSinkState>)
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
