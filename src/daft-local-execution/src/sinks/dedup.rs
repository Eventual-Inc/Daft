//! Custom sink operator for deduplication
//! The current strategy is to:
//! sink():
//!   1. Partition input micro-partitions into N pieces, where N is # of workers
//!   2. Deduplicate each piece separately and store in partitioned state
//!
//! finalize():
//!   1. For each of the N partitions:
//!      - Concatenate the partially deduped pieces
//!      - Re-deduplicate for each of the concatenated pieces (don't need to across partitions)
//!   2. Concatenate all partition outputs and return
//!
//! Current method works well for high-cardinality inputs
//! TODO: Better support for low-cardinality by avoiding partitioning
//! TODO: Store Hash Table as state per partition and reuse across micro-partitions

use std::sync::Arc;

use common_error::DaftResult;
use common_runtime::get_compute_pool_num_threads;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{instrument, Span};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeOutput, BlockingSinkFinalizeResult, BlockingSinkSinkResult,
    BlockingSinkState, BlockingSinkStatus,
};
use crate::ExecutionTaskSpawner;

#[derive(Default)]
struct SinglePartitionDedupState {
    partially_deduped: Vec<MicroPartition>,
}

enum DedupState {
    Accumulating {
        inner_states: Vec<SinglePartitionDedupState>,
    },
    Done,
}

impl DedupState {
    fn new(num_partitions: usize) -> Self {
        let inner_states = (0..num_partitions)
            .map(|_| SinglePartitionDedupState::default())
            .collect::<Vec<_>>();
        Self::Accumulating { inner_states }
    }

    fn push(&mut self, input: Arc<MicroPartition>, columns: &[BoundExpr]) -> DaftResult<()> {
        let Self::Accumulating {
            ref mut inner_states,
        } = self
        else {
            panic!("DropDuplicatesSink should be in Accumulating state");
        };

        let partitioned = input.partition_by_hash(columns, inner_states.len())?;
        for (p, state) in partitioned.into_iter().zip(inner_states.iter_mut()) {
            // TODO: Deduplicate in parallel?
            let deduped = p.dedup(columns)?;
            state.partially_deduped.push(deduped);
        }
        Ok(())
    }

    fn finalize(&mut self) -> Vec<SinglePartitionDedupState> {
        let res = if let Self::Accumulating {
            ref mut inner_states,
        } = self
        {
            std::mem::take(inner_states)
        } else {
            panic!("DropDuplicatesSink should be in Accumulating state");
        };
        *self = Self::Done;
        res
    }
}

impl BlockingSinkState for DedupState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct DedupSink {
    columns: Arc<Vec<BoundExpr>>,
}

impl DedupSink {
    pub fn new(columns: &[BoundExpr]) -> DaftResult<Self> {
        Ok(Self {
            columns: Arc::new(columns.to_vec()),
        })
    }

    fn num_partitions(&self) -> usize {
        self.max_concurrency()
    }
}

impl BlockingSink for DedupSink {
    #[instrument(skip_all, name = "DedupSink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn BlockingSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult {
        let columns = self.columns.clone();
        spawner
            .spawn(
                async move {
                    let dedup_state = state
                        .as_any_mut()
                        .downcast_mut::<DedupState>()
                        .expect("DedupSink should have DedupState");

                    dedup_state.push(input, &columns)?;
                    Ok(BlockingSinkStatus::NeedMoreInput(state))
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "DedupSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let columns = self.columns.clone();
        let num_partitions = self.num_partitions();
        spawner
            .spawn(
                async move {
                    let mut state_iters = states
                        .into_iter()
                        .map(|mut state| {
                            state
                                .as_any_mut()
                                .downcast_mut::<DedupState>()
                                .expect("DedupSink should have DedupState")
                                .finalize()
                                .into_iter()
                        })
                        .collect::<Vec<_>>();

                    let mut per_partition_finalize_tasks = tokio::task::JoinSet::new();
                    for _ in 0..num_partitions {
                        // Collect the partially deduped micro-partitions (MPs) from all of the sub-states
                        // for the current partition
                        let per_partition_micros = state_iters
                            .iter_mut()
                            .flat_map(|state| {
                                state
                                    .next()
                                    .expect("DedupSink should have SinglePartitionDedupState")
                                    .partially_deduped
                            })
                            .collect::<Vec<_>>();

                        // Merge the partially deduped MPs
                        // Do this concurrently across all of the partitions
                        let columns = columns.clone();
                        per_partition_finalize_tasks.spawn(async move {
                            MicroPartition::concat(&per_partition_micros)?.dedup(&columns)
                        });
                    }
                    // Join the tasks and collect the deduped partitions
                    let results = per_partition_finalize_tasks
                        .join_all()
                        .await
                        .into_iter()
                        .collect::<DaftResult<Vec<_>>>()?;

                    // Concatenate the results and return
                    let concated = MicroPartition::concat(&results)?;
                    Ok(BlockingSinkFinalizeOutput::Finished(vec![Arc::new(
                        concated,
                    )]))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> &'static str {
        "Dedup"
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut display = vec![];
        display.push(format!(
            "Dedup: On Columns: {}",
            self.columns.iter().map(|e| e.to_string()).join(", ")
        ));
        display
    }

    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(DedupState::new(self.num_partitions())))
    }
}
