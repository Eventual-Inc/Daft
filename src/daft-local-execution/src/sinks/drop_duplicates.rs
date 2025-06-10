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

use std::sync::Arc;

use arrow_row::Rows;
use common_error::DaftResult;
use common_runtime::get_compute_pool_num_threads;
use daft_core::datatypes::DaftPrimitiveType;
use daft_core::prelude::{AsArrow, DaftArrayType, Int64Array, SeriesLike, UInt64Array};
use daft_core::series::IntoSeries;
use daft_core::utils::identity_hash_set::IndexHash;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_core::series::array_impl::ArrayWrapper;
use hashbrown::{HashSet, hash_set::Entry::{Occupied, Vacant}};
use itertools::Itertools;
use log::Record;
use tracing::{instrument, Span};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult, BlockingSinkState,
    BlockingSinkStatus,
};
use crate::ExecutionTaskSpawner;

struct SingleColState<T: DaftArrayType + AsArrow> {
    dedup_map: HashSet<T>
}

impl<T> SingleColState<T> where T: DaftArrayType + AsArrow {
    fn new() -> Self {
        todo!()
    }

    fn push_batch(&mut self, batch: &RecordBatch, columns: &[BoundExpr]) -> DaftResult<()> {
        let distinct_on = batch.eval_expression_list(columns)?;
        let distinct_on = distinct_on.get_column(0).downcast::<T>().unwrap().as_arrow();

        for (idx, val) in distinct_on.iter().enumerate() {}
        todo!()
    }
}

struct MultiColState {
    distinct_rows: Rows,
    dedup_map: HashSet<IndexHash>,
}

impl MultiColState {}

enum DropDuplicatesState<T> {
    Accumulating {
        state: T,
        partially_deduped: Vec<RecordBatch>,
    },
    Done,
}

impl DropDuplicatesState {
    fn new() -> Self {
        Self::Accumulating { dedup_map: HashSet::<Option<i64>>::with_capacity(16384), partially_deduped: vec![] }
    }

    fn push(&mut self, input: Arc<MicroPartition>, columns: &[BoundExpr]) -> DaftResult<()> {
        assert_eq!(columns.len(), 1); // TODO: Support multiple columns
        let Self::Accumulating {
            ref mut dedup_map,
            ref mut partially_deduped,
        } = self
        else {
            panic!("DropDuplicatesSink should be in Accumulating state");
        };

        let batches = input.get_tables()?;
        // let mut idxs = Vec::with_capacity(input.len() / batches.len());
        for batch in batches.iter() {
            let distinct_on = batch.eval_expression_list(columns)?;
            let distinct_on = distinct_on.get_column(0);
            let distinct_on = distinct_on.inner.as_any().downcast_ref::<ArrayWrapper<Int64Array>>().unwrap().0.as_arrow();

            let mut idxs = Vec::with_capacity(input.len() / batches.len());
            // idxs.clear();
            for (idx, val) in distinct_on.iter().enumerate() {
                match dedup_map.entry(val.cloned()) {
                    Vacant(e) => {
                        e.insert();
                        idxs.push(idx as u64);
                    }
                    Occupied(_) => {}
                }
            }

            let idxs_series = UInt64Array::from(("idxs", idxs)).into_series();
            let batch = batch.take(&idxs_series)?;
            partially_deduped.push(batch);
        }

        // let deduped = input.dedup(columns)?;
        // partially_deduped.push(deduped);
        // let partitioned = input.partition_by_hash(columns, inner_states.len())?;
        // for (p, state) in partitioned.into_iter().zip(inner_states.iter_mut()) {
        //     // TODO: Deduplicate in parallel?
        //     let deduped = p.dedup(columns)?;
        //     state.partially_deduped.push(deduped);
        // }
        Ok(())
    }

    fn finalize(&mut self) -> Vec<RecordBatch> {
        let res = if let Self::Accumulating {
            ref mut partially_deduped,
            ..
        } = self
        {
            std::mem::take(partially_deduped)
        } else {
            panic!("DropDuplicatesSink should be in Accumulating state");
        };
        *self = Self::Done;
        res
    }
}

impl BlockingSinkState for DropDuplicatesState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct DropDuplicatesSink {
    columns: Arc<Vec<BoundExpr>>,
}

impl DropDuplicatesSink {
    pub fn new(columns: &[BoundExpr]) -> DaftResult<Self> {
        Ok(Self {
            columns: Arc::new(columns.to_vec()),
        })
    }

    fn num_partitions(&self) -> usize {
        self.max_concurrency()
    }
}

impl BlockingSink for DropDuplicatesSink {
    #[instrument(skip_all, name = "DropDuplicatesSink::sink")]
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
                        .downcast_mut::<DropDuplicatesState>()
                        .expect("DropDuplicatesSink should have DropDuplicatesState");

                    dedup_state.push(input, &columns)?;
                    Ok(BlockingSinkStatus::NeedMoreInput(state))
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "DropDuplicatesSink::finalize")]
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
                    let partially_deduped = states
                        .into_iter()
                        .flat_map(|mut state| {
                            state
                                .as_any_mut()
                                .downcast_mut::<DropDuplicatesState>()
                                .expect("DropDuplicatesSink should have DropDuplicatesState")
                                .finalize()
                        })
                        .collect::<Vec<_>>();

                    let concated = RecordBatch::concat(&partially_deduped)?;
                    let partitioned = concated.partition_by_hash(&columns, num_partitions)?;

                    let mut per_partition_finalize_tasks = tokio::task::JoinSet::new();
                    for partition in partitioned.into_iter() {
                        let columns = columns.clone();
                        per_partition_finalize_tasks.spawn(async move {
                            partition.dedup(&columns)?;
                            Ok(partition)
                        });
                    }

                    // Join the tasks and collect the deduped partitions
                    let results = per_partition_finalize_tasks
                        .join_all()
                        .await
                        .into_iter()
                        .collect::<DaftResult<Vec<_>>>()?;

                    // let mut per_partition_finalize_tasks = tokio::task::JoinSet::new();
                    // for _ in 0..num_partitions {
                    //     // Collect the partially deduped micro-partitions (MPs) from all of the sub-states
                    //     // for the current partition
                    //     let per_partition_micros =
                    //         state_iters
                    //             .iter_mut()
                    //             .flat_map(|state| {
                    //                 state.next().expect(
                    //                 "DropDuplicatesSink should have SinglePartitionDedupState",
                    //             ).partially_deduped
                    //             })
                    //             .collect::<Vec<_>>();

                    //     // Merge the partially deduped MPs
                    //     // Do this concurrently across all of the partitions
                    //     let columns = columns.clone();
                    //     per_partition_finalize_tasks.spawn(async move {
                    //         MicroPartition::concat(&per_partition_micros)?.dedup(&columns)
                    //     });
                    // }
                    // // Join the tasks and collect the deduped partitions
                    // let results = per_partition_finalize_tasks
                    //     .join_all()
                    //     .await
                    //     .into_iter()
                    //     .collect::<DaftResult<Vec<_>>>()?;

                    // Concatenate the results and return
                    let concated = MicroPartition::new_loaded(
                        results[0].schema.clone(),
                        Arc::new(results),
                        None,
                    );
                    Ok(Some(Arc::new(concated)))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> &'static str {
        "DropDuplicates"
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut display = vec![];
        display.push(format!(
            "DropDuplicates: On Columns: {}",
            self.columns.iter().map(|e| e.to_string()).join(", ")
        ));
        display
    }

    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(DropDuplicatesState::new(self.num_partitions())))
    }
}
