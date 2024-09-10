use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_micropartition::MicroPartition;
use daft_plan::OutputFileInfo;
use tracing::instrument;

use crate::pipeline::PipelineResultType;

use super::blocking_sink::{BlockingSink, BlockingSinkState, BlockingSinkStatus};

enum PhysicalWriteState {
    Writing(Vec<Arc<MicroPartition>>),
    Done(Vec<Arc<MicroPartition>>),
}

impl BlockingSinkState for PhysicalWriteState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
    fn finalize(&mut self) {
        if let PhysicalWriteState::Writing(parts) = self {
            *self = PhysicalWriteState::Done(std::mem::take(parts));
        }
    }
}

pub struct PhysicalWriteSink {
    schema: SchemaRef,
    file_info: OutputFileInfo,
}

impl PhysicalWriteSink {
    pub fn new(file_info: OutputFileInfo, schema: SchemaRef) -> Self {
        Self { schema, file_info }
    }

    pub fn arced(self) -> Arc<dyn BlockingSink> {
        Arc::new(self)
    }

    #[cfg(feature = "python")]
    fn write(&self, input: &Arc<MicroPartition>) -> DaftResult<Arc<MicroPartition>> {
        use common_error::DaftError;
        use pyo3::Python;
        Python::with_gil(|py| {
            daft_micropartition::python::write_tabular(
                py,
                input,
                &self.file_info.file_format,
                &self.schema,
                &self.file_info.root_dir,
                &self.file_info.compression,
                &self.file_info.partition_cols,
                &self.file_info.io_config,
            )
            .map_err(DaftError::PyO3Error)
        })
        .map(|p| p.into())
    }
}

impl BlockingSink for PhysicalWriteSink {
    #[instrument(skip_all, name = "PhysicalWriteSink::sink")]
    fn sink(
        &self,
        input: &Arc<MicroPartition>,
        state: &mut dyn BlockingSinkState,
    ) -> DaftResult<BlockingSinkStatus> {
        let write_result = self.write(input)?;
        let write_state = state
            .as_any_mut()
            .downcast_mut::<PhysicalWriteState>()
            .unwrap();
        if let PhysicalWriteState::Writing(parts) = write_state {
            parts.push(write_result);
            Ok(BlockingSinkStatus::NeedMoreInput)
        } else {
            panic!("PhysicalWriteSink should be in Accumulating state");
        }
    }

    #[instrument(skip_all, name = "PhysicalWriteSink::finalize")]
    fn finalize(
        &self,
        states: &[&dyn BlockingSinkState],
    ) -> DaftResult<Option<PipelineResultType>> {
        let parts = states
            .iter()
            .flat_map(|state| {
                if let PhysicalWriteState::Done(parts) = state.as_any().downcast_ref().unwrap() {
                    parts
                } else {
                    panic!("PhysicalWriteSink should be in Done state");
                }
            })
            .collect::<Vec<_>>();
        let concated =
            MicroPartition::concat(&parts.iter().map(|x| x.as_ref()).collect::<Vec<_>>())?;
        Ok(Some(PipelineResultType::Data(Arc::new(concated))))
    }

    fn name(&self) -> &'static str {
        "PhysicalWriteSink"
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(PhysicalWriteState::Writing(vec![])))
    }
}
