use std::sync::Arc;

use common_error::DaftResult;
use daft_core::schema::SchemaRef;
use daft_micropartition::MicroPartition;
use daft_plan::OutputFileInfo;
use tracing::instrument;

use crate::pipeline::PipelineResultType;

use super::blocking_sink::{BlockingSink, BlockingSinkStatus};

enum PhysicalWriteState {
    Accumulating(Vec<Arc<MicroPartition>>),
    #[allow(dead_code)]
    Done(Arc<MicroPartition>),
}
#[cfg(feature = "python")]
pub struct PhysicalWriteSink {
    schema: SchemaRef,
    file_info: OutputFileInfo,
    state: PhysicalWriteState,
}

#[cfg(feature = "python")]
impl PhysicalWriteSink {
    pub fn new(file_info: OutputFileInfo, schema: SchemaRef) -> Self {
        Self {
            schema,
            file_info,
            state: PhysicalWriteState::Accumulating(vec![]),
        }
    }

    pub fn boxed(self) -> Box<dyn BlockingSink> {
        Box::new(self)
    }

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

#[cfg(feature = "python")]
impl BlockingSink for PhysicalWriteSink {
    #[instrument(skip_all, name = "PhysicalWriteSink::sink")]
    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<BlockingSinkStatus> {
        if let PhysicalWriteState::Accumulating(parts) = &mut self.state {
            parts.push(input.clone());
            Ok(BlockingSinkStatus::NeedMoreInput)
        } else {
            panic!("PhysicalWriteSink should be in Accumulating state");
        }
    }

    #[instrument(skip_all, name = "PhysicalWriteSink::finalize")]
    fn finalize(&mut self) -> DaftResult<Option<PipelineResultType>> {
        if let PhysicalWriteState::Accumulating(parts) = &mut self.state {
            assert!(
                !parts.is_empty(),
                "We can not finalize PhysicalWriteSink with no data"
            );
            let concated =
                MicroPartition::concat(&parts.iter().map(|x| x.as_ref()).collect::<Vec<_>>())?;
            let written_file_paths = self.write(&Arc::new(concated))?;
            self.state = PhysicalWriteState::Done(written_file_paths.clone());
            Ok(Some(PipelineResultType::Data(written_file_paths)))
        } else {
            panic!("PhysicalWriteSink should be in Accumulating state");
        }
    }
    fn name(&self) -> &'static str {
        "PhysicalWriteSink"
    }
}
