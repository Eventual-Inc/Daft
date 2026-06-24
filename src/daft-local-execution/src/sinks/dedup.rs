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

pub(crate) struct DedupState {
    inner_states: Vec<SinglePartitionDedupState>,
    spill_dirs: Option<Vec<String>>,
    spill_writer: Option<SpillWriter>,
    reservation: MemoryReservation,
    cap: Option<u64>,
}

impl DedupState {
    fn new(num_partitions: usize, spill_dirs: Option<Vec<String>>, cap: Option<u64>) -> Self {
        let inner_states = (0..num_partitions)
            .map(|_| SinglePartitionDedupState::default())
            .collect::<Vec<_>>();
        Self {
            inner_states,
            spill_dirs,
            spill_writer: None,
            reservation: get_or_init_memory_manager().reservation(),
            cap,
        }
    }

    fn push(&mut self, input: MicroPartition, columns: &[BoundExpr]) -> DaftResult<()> {
        let Self {
            inner_states,
            spill_dirs,
            spill_writer,
            reservation,
            cap,
        } = self;

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

/// Maximum recursive sub-partitioning depth for an oversized dedup bucket at finalize.
const MAX_DEDUP_RECURSION: u64 = 4;

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
        debug_assert!(!pieces.is_empty(), "dedup bucket selected with bytes>0 but no resident data (counter drift)");
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
        // No spill → usize::MAX budget → no recursion (identical to previous behavior).
        let budget = match &self.spill_config {
            Some(sc) => (sc.pool_bytes / num_partitions.max(1)).max(1),
            None => usize::MAX,
        };
        spawner
            .spawn(
                async move {
                    // Seal each state: take in-memory buckets, finish spill writer into a store.
                    let mut inners: Vec<Vec<SinglePartitionDedupState>> = Vec::new();
                    let mut stores: Vec<Option<SpillStore>> = Vec::new();
                    for state in states {
                        let DedupState {
                            inner_states,
                            spill_writer,
                            ..
                        } = state;
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
                            combine_deduped(pieces, &columns, budget, 0)
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

/// Concatenate + dedup `pieces` for one bucket. If the combined size exceeds `budget`, recursively
/// sub-partition by the dedup `columns` (sub-buckets hold disjoint key groups, so dedup is exact)
/// with a per-level hash seed, bounding peak memory. Returns `None` if empty.
fn combine_deduped(
    pieces: Vec<MicroPartition>,
    columns: &[BoundExpr],
    budget: usize,
    seed: u64,
) -> DaftResult<Option<MicroPartition>> {
    if pieces.is_empty() {
        return Ok(None);
    }
    let total: usize = pieces.iter().map(MicroPartition::size_bytes).sum();
    if seed >= MAX_DEDUP_RECURSION || total <= budget.max(1) {
        return Ok(Some(MicroPartition::concat(pieces)?.dedup(columns)?));
    }
    let sub_n = ((total / budget.max(1)) + 1).clamp(2, 64);
    let mut subs: Vec<Vec<MicroPartition>> = (0..sub_n).map(|_| vec![]).collect();
    for piece in pieces {
        for (i, sub) in piece
            .partition_by_hash_seeded(columns, sub_n, seed)?
            .into_iter()
            .enumerate()
        {
            if sub.len() > 0 {
                subs[i].push(sub);
            }
        }
    }
    let mut results = vec![];
    for sub in subs {
        if let Some(c) = combine_deduped(sub, columns, budget, seed + 1)? {
            results.push(c);
        }
    }
    if results.is_empty() {
        Ok(None)
    } else {
        // Sub-buckets hold disjoint keys, so concatenation is the combined deduped result.
        Ok(Some(MicroPartition::concat(results)?))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_core::prelude::{DataType, Field, Int32Array, Schema};
    use daft_core::series::IntoSeries;
    use daft_dsl::expr::bound_col;
    use daft_dsl::expr::bound_expr::BoundExpr;
    use daft_micropartition::MicroPartition;
    use daft_recordbatch::RecordBatch;

    use super::combine_deduped;

    /// Build a single-column MicroPartition of Int32 values from `vals`.
    fn make_mp(vals: &[i32]) -> MicroPartition {
        let field = Field::new("id", DataType::Int32);
        let series = Int32Array::from_field_and_values(field.clone(), vals.iter().copied()).into_series();
        let schema = Arc::new(Schema::new(vec![field]));
        let rb = RecordBatch::new_unchecked(schema.clone(), vec![series.into()], vals.len());
        MicroPartition::new_loaded(schema.into(), vec![rb].into(), None)
    }

    /// Bound expression referencing column index 0 ("id").
    fn bound_id_col() -> BoundExpr {
        BoundExpr::new_unchecked(bound_col(0, Field::new("id", DataType::Int32)))
    }

    #[test]
    fn combine_deduped_recursion_branch_exact_distinct_set() {
        // Each piece covers the full range 0..50 — 5 pieces → 250 rows, 50 distinct.
        // Budget is set to 1 byte to force recursion into combine_deduped's sub-partitioning path.
        let columns = vec![bound_id_col()];
        let vals: Vec<i32> = (0..50).collect();
        let pieces: Vec<MicroPartition> = (0..5).map(|_| make_mp(&vals)).collect();

        let result = combine_deduped(pieces, &columns, 1, 0)
            .expect("combine_deduped must succeed")
            .expect("non-empty input must yield Some");

        assert_eq!(
            result.len(),
            50,
            "Expected exactly 50 distinct rows, got {}",
            result.len()
        );

        // Membership assertion: the exact distinct set must be 0..50.
        let mut actual_values: Vec<i32> = result
            .record_batches()
            .iter()
            .flat_map(|rb| {
                rb.get_column(0)
                    .i32()
                    .expect("column must be Int32")
                    .values()
                    .to_vec()
            })
            .collect();
        actual_values.sort_unstable();
        let expected_values: Vec<i32> = (0..50).collect();
        assert_eq!(
            actual_values, expected_values,
            "Result must contain exactly the values 0..50"
        );
    }
}
