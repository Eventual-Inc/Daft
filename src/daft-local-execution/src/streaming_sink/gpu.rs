use std::{any::Any, sync::Arc, vec};

use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use pyo3::{PyObject, Python};
use tracing::instrument;

use crate::{
    dispatcher::{DispatchSpawner, UnorderedDispatcher},
    ops::NodeType,
    pipeline::NodeName,
    streaming_sink::base::{
        StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeResult,
        StreamingSinkOutput, StreamingSinkState,
    },
    ExecutionRuntimeContext, ExecutionTaskSpawner,
};

pub struct GPUState {
    // Python manager that handles active and pending GPU (CUDA, MPS, etc) streams
    // py_manager: PyObject,
}

impl GPUState {
    pub fn new() -> Self {
        Python::with_gil(|py| {});

        Self {}
    }
}

impl StreamingSinkState for GPUState {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct GPUOperator {
    name: String,
    passthrough_columns: Vec<BoundExpr>,
    output_schema: SchemaRef,
}

impl GPUOperator {
    pub fn new(
        name: String,
        passthrough_columns: Vec<BoundExpr>,
        output_schema: &SchemaRef,
    ) -> Self {
        Self {
            name,
            passthrough_columns,
            output_schema: output_schema.clone(),
        }
    }
}

impl StreamingSink for GPUOperator {
    #[instrument(skip_all, name = "GPUOperator::sink")]
    fn execute(
        &self,
        mut input: Arc<MicroPartition>,
        mut state: Box<dyn StreamingSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult {
        let mut input_num_rows = input.len();

        let gpu_state = state
            .as_any_mut()
            .downcast_mut::<GPUState>()
            .expect("GPUOperator should have GPUState")
            .as_any_mut();

        todo!()
    }

    fn finalize(
        &self,
        _states: Vec<Box<dyn StreamingSinkState>>,
        _spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult {
        Ok(None).into()
    }

    fn name(&self) -> NodeName {
        format!("GPU {}", self.name).into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::GPUProject
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![format!("GPU: {}", self.name)]
    }

    fn max_concurrency(&self) -> usize {
        1
    }

    fn make_state(&self) -> Box<dyn StreamingSinkState> {
        Box::new(GPUState::new())
    }

    fn dispatch_spawner(
        &self,
        _runtime_handle: &ExecutionRuntimeContext,
        _maintain_order: bool,
    ) -> Arc<dyn DispatchSpawner> {
        // To buffer the input, we need small batches.
        Arc::new(UnorderedDispatcher::new(0, 32))
    }
}
