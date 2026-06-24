//! Custom sink operator for deduplication
//! The current strategy is to:
//! sink():
//!   1. Partition input micro-partitions into N pieces, where N is # of workers
//!   2. Deduplicate each piece separately and store in partitioned state
//!
//! finalize():
//!   1. For each of the N partitions:
//!      - Concatenate the partially deduped pieces (in-memory + spilled)
//!      - Re-deduplicate for each of the concatenated pieces (don't need to across partitions)
//!   2. Concatenate all partition outputs and return
//!
//! Current method works well for high-cardinality inputs
//! TODO: Better support for low-cardinality by avoiding partitioning
//! TODO: Store Hash Table as state per partition and reuse across micro-partitions

use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_metrics::ops::NodeType;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkOutput, BlockingSinkSinkResult,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
    resource_manager::{MemoryReservation, SpillableBuckets, get_or_init_memory_manager,
        reconcile_reservation},
    spill::{SpillConfig, SpillStore, SpillWriter},
};

#[derive(Default)]
pub(crate) struct SinglePartitionDedupState {
    partially_deduped: Vec<MicroPartition>,
    bytes: usize,
}

pub(crate) enum DedupState {
    Accumulating {
        inner_states: Vec<SinglePartitionDedupState>,
        spill_dirs: Option<Vec<String>>,
        spill_writer: Option<SpillWriter>,
        reservation: MemoryReservation,
        cap: Option<u64>,
    },
    Done,
}

impl DedupState {
    fn new(num_partitions: usize, spill_dirs: Option<Vec<String>>, cap: Option<u64>) -> Self {
        let inner_states = (0..num_partitions)
            .map(|_| SinglePartitionDedupState::default())
            .collect::<Vec<_>>();
        Self::Accumulating {
            inner_states,
            spill_dirs,
            spill_writer: None,
            reservation: get_or_init_memory_manager().reservation(),
            cap,
        }
    }

    fn push(&mut self, input: MicroPartition, columns: &[BoundExpr]) -> DaftResult<()> {
        let Self::Accumulating {
            inner_states,
            spill_dirs,
            spill_writer,
            reservation,
            cap,
        } = self
        else {
            panic!("DropDuplicatesSink should be in Accumulating state");
        };

        let partitioned = input.partition_by_hash(columns, inner_states.len())?;
        for (p, state) in partitioned.into_iter().zip(inner_states.iter_mut()) {
            // TODO: Deduplicate in parallel?
            let deduped = p.dedup(columns)?;
            state.bytes += deduped.size_bytes();
            state.partially_deduped.push(deduped);
        }

        if let Some(dirs) = spill_dirs.as_ref() {
            let mut buckets = DedupBuckets {
                inner_states,
                spill_writer,
                spill_dirs: dirs.clone(),
                columns,
            };
            reconcile_reservation(&mut buckets, reservation, *cap)?;
        }
        Ok(())
    }
}

/// Adapter so `reconcile_reservation` can spill dedup buckets. Dedup is idempotent, so spilled
/// partially-deduped batches are re-deduped against in-memory ones at finalize.
struct DedupBuckets<'a> {
    inner_states: &'a mut [SinglePartitionDedupState],
    spill_writer: &'a mut Option<SpillWriter>,
    spill_dirs: Vec<String>,
    columns: &'a [BoundExpr],
}

impl SpillableBuckets for DedupBuckets<'_> {
    fn resident_bytes(&self) -> u64 {
        self.inner_states.iter().map(|st| st.bytes as u64).sum()
    }

    fn spill_largest_bucket(&mut self) -> DaftResult<bool> {
        let num_buckets = self.inner_states.len();
        let Some((p, _)) = self
            .inner_states
            .iter()
            .enumerate()
            .filter(|(_, st)| st.bytes > 0)
            .max_by_key(|(_, st)| st.bytes)
        else {
            return Ok(false);
        };
        let st = &mut self.inner_states[p];
        // Compact this bucket to a single deduped MicroPartition before spilling.
        let pieces = std::mem::take(&mut st.partially_deduped);
        st.bytes = 0;
        if pieces.is_empty() {
            return Ok(true);
        }
        let compacted = MicroPartition::concat(pieces)?.dedup(self.columns)?;
        if compacted.len() == 0 {
            return Ok(true);
        }
        let writer = match self.spill_writer {
            Some(w) => w,
            None => {
                let schema = compacted.schema();
                let w = SpillWriter::new(
                    num_buckets,
                    &schema,
                    self.spill_dirs.clone(),
                    "daft_dedup_spill_",
                )?;
                *self.spill_writer = Some(w);
                self.spill_writer.as_mut().unwrap()
            }
        };
        for rb in compacted.record_batches() {
            writer.write_batch(p, rb)?;
        }
        Ok(true)
    }
}

pub struct DedupSink {
    columns: Arc<Vec<BoundExpr>>,
    spill_config: Option<SpillConfig>,
}

impl DedupSink {
    pub fn new(columns: &[BoundExpr], spill_config: Option<SpillConfig>) -> DaftResult<Self> {
        Ok(Self {
            columns: Arc::new(columns.to_vec()),
            spill_config,
        })
    }

    fn num_partitions(&self) -> usize {
        self.max_concurrency()
    }
}

impl BlockingSink for DedupSink {
    type State = DedupState;

    #[instrument(skip_all, name = "DedupSink::sink")]
    fn sink(
        &self,
        input: MicroPartition,
        mut state: Self::State,
        _runtime_stats: Arc<Self::Stats>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        let columns = self.columns.clone();
        spawner
            .spawn(
                async move {
                    state.push(input, &columns)?;
                    Ok(state)
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "DedupSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let columns = self.columns.clone();
        let num_partitions = self.num_partitions();
        spawner
            .spawn(
                async move {
                    // Seal each state: take in-memory buckets, finish spill writer into a store.
                    let mut inners: Vec<Vec<SinglePartitionDedupState>> = Vec::new();
                    let mut stores: Vec<Option<SpillStore>> = Vec::new();
                    for state in states {
                        let DedupState::Accumulating {
                            inner_states,
                            spill_writer,
                            ..
                        } = state
                        else {
                            panic!("DropDuplicatesSink should be in Accumulating state");
                        };
                        inners.push(inner_states);
                        stores.push(match spill_writer {
                            Some(w) => Some(w.finish()?),
                            None => None,
                        });
                    }
                    let stores = Arc::new(stores);

                    let mut tasks = tokio::task::JoinSet::new();
                    for p in 0..num_partitions {
                        let mut pieces: Vec<MicroPartition> = Vec::new();
                        for inner in inners.iter_mut() {
                            if let Some(st) = inner.get_mut(p) {
                                pieces.append(&mut st.partially_deduped);
                            }
                        }
                        let stores = stores.clone();
                        let columns = columns.clone();
                        tasks.spawn(async move {
                            for store in stores.iter().flatten() {
                                if store.is_spilled(p) {
                                    let batches = store.read_bucket(p)?;
                                    if !batches.is_empty() {
                                        let schema = batches[0].schema.clone();
                                        pieces.push(MicroPartition::new_loaded(
                                            schema,
                                            Arc::new(batches),
                                            None,
                                        ));
                                    }
                                }
                            }
                            if pieces.is_empty() {
                                return Ok::<Option<MicroPartition>, DaftError>(None);
                            }
                            Ok(Some(MicroPartition::concat(pieces)?.dedup(&columns)?))
                        });
                    }
                    let mut results = vec![];
                    while let Some(res) = tasks.join_next().await {
                        if let Some(mp) =
                            res.map_err(|e| DaftError::InternalError(e.to_string()))??
                        {
                            results.push(mp);
                        }
                    }
                    Ok(BlockingSinkOutput::Partitions(results))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "Dedup".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Dedup
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut display = vec![];
        display.push(format!(
            "Dedup: On Columns: {}",
            self.columns.iter().map(|e| e.to_string()).join(", ")
        ));
        if self.spill_config.is_some() {
            display.push("Spill: enabled (grace dedup)".to_string());
        }
        display
    }

    fn make_state(&self, _input_id: InputId) -> DaftResult<Self::State> {
        Ok(DedupState::new(
            self.num_partitions(),
            self.spill_config.as_ref().map(|sc| sc.spill_dirs.clone()),
            self.spill_config.as_ref().and_then(|sc| sc.cap()),
        ))
    }
}
