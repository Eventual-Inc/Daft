use std::{any::Any, sync::Arc};

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use tracing::{instrument, Span};

use super::blocking_sink::{BlockingSinkSinkResult, BlockingSinkState, BlockingSinkStatus};
use crate::ExecutionTaskSpawner;

/// Shared state for a single partition in window operations
#[derive(Default)]
pub struct SinglePartitionWindowState {
    pub partitions: Vec<RecordBatch>,
}

/// Base state for window operations
pub enum WindowBaseState {
    Accumulating {
        inner_states: Vec<Option<SinglePartitionWindowState>>,
    },
    Done,
}

impl WindowBaseState {
    pub fn new(num_partitions: usize) -> Self {
        let inner_states = (0..num_partitions).map(|_| None).collect::<Vec<_>>();
        Self::Accumulating { inner_states }
    }

    pub fn make_base_state(num_partitions: usize) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(Self::new(num_partitions)))
    }

    pub fn push(
        &mut self,
        input: Arc<MicroPartition>,
        partition_by: &[BoundExpr],
        sink_name: &str,
    ) -> DaftResult<()> {
        if let Self::Accumulating {
            ref mut inner_states,
        } = self
        {
            let partitioned = input.partition_by_hash(partition_by, inner_states.len())?;
            for (p, state) in partitioned.into_iter().zip(inner_states.iter_mut()) {
                let state = state.get_or_insert_with(SinglePartitionWindowState::default);
                for table in p.get_tables()?.iter() {
                    state.partitions.push(table.clone());
                }
            }
        } else {
            panic!("{} should be in Accumulating state", sink_name);
        }
        Ok(())
    }

    pub fn finalize(&mut self, sink_name: &str) -> Vec<Option<SinglePartitionWindowState>> {
        let res = if let Self::Accumulating {
            ref mut inner_states,
        } = self
        {
            std::mem::take(inner_states)
        } else {
            panic!("{} should be in Accumulating state", sink_name);
        };
        *self = Self::Done;
        res
    }
}

impl BlockingSinkState for WindowBaseState {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Base trait for window sink params
#[allow(dead_code)]
pub trait WindowSinkParams: Send + Sync {
    fn original_schema(&self) -> &SchemaRef;
    fn partition_by(&self) -> &[BoundExpr];
    fn name(&self) -> &'static str;
}

/// Sink method implementations for window operations
#[instrument(skip_all, name = "WindowBaseSink::window_sink")]
pub fn base_sink<P: WindowSinkParams + 'static>(
    params: Arc<P>,
    input: Arc<MicroPartition>,
    mut state: Box<dyn BlockingSinkState>,
    spawner: &ExecutionTaskSpawner,
) -> BlockingSinkSinkResult {
    let sink_name = params.name().to_string();
    spawner
        .spawn(
            async move {
                let window_state = state
                    .as_any_mut()
                    .downcast_mut::<WindowBaseState>()
                    .unwrap_or_else(|| panic!("{} should have WindowBaseState", sink_name));

                window_state.push(input, params.partition_by(), &sink_name)?;
                Ok(BlockingSinkStatus::NeedMoreInput(state))
            },
            Span::current(),
        )
        .into()
}
