use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;

use crate::{pipeline::PipelineResultType, NUM_CPUS};

mod buffer;

pub mod aggregate;
pub mod filter;
pub mod hash_join_probe;
pub mod intermediate_node;
pub mod project;

pub(crate) trait IntermediateOperatorState: Send + Sync {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

pub(crate) struct DefaultIntermediateOperatorState {}

impl IntermediateOperatorState for DefaultIntermediateOperatorState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub(crate) enum IntermediateOperatorResult {
    NeedMoreInput(Option<Arc<MicroPartition>>),
    #[allow(dead_code)]
    HasMoreOutput(Arc<MicroPartition>),
}

pub(crate) trait IntermediateOperator: Send + Sync {
    fn execute(
        &self,
        idx: usize,
        input: &PipelineResultType,
        state: &mut dyn IntermediateOperatorState,
    ) -> DaftResult<IntermediateOperatorResult>;
    fn finalize(
        &self,
        _states: Vec<Box<dyn IntermediateOperatorState>>,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        Ok(None)
    }
    fn name(&self) -> &'static str;
    fn make_state(&self) -> Box<dyn IntermediateOperatorState> {
        Box::new(DefaultIntermediateOperatorState {})
    }
    fn max_concurrency(&self) -> usize {
        *NUM_CPUS
    }
}
