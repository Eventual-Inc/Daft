use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_runtime::get_compute_pool_num_threads;
use daft_core::{array::ops::build_multi_array_compare, datatypes::UInt64Array, prelude::*};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_groupby::IntoGroups;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;

use super::blocking_sink::BlockingSinkOutput;
use crate::{
    resource_manager::{MemoryReservation, SpillableBuckets, get_or_init_memory_manager,
        reconcile_reservation},
    spill::{SpillConfig, SpillStore, SpillWriter},
};

/// Shared state for a single partition (hash bucket) in window operations.
#[derive(Default)]
pub struct SinglePartitionWindowState {
    pub partitions: Vec<RecordBatch>,
    /// Resident bytes of `partitions`, used to decide when to spill this bucket.
    pub bytes: usize,
}

/// Base state for window operations. Input is hash-partitioned by the window `partition_by` key
/// into `num_partitions` buckets; each bucket accumulates its rows until finalize. Window functions
/// are not decomposable (a partition group must be seen whole), so spilling relieves memory
/// *between* buckets — a single `partition_by` group must still fit in memory to be computed.
pub struct WindowBaseState {
    inner_states: Vec<Option<SinglePartitionWindowState>>,
    /// Spill destinations; `None` disables spilling for this sink.
    spill_dirs: Option<Vec<String>>,
    /// Lazily created on first spill (one IPC file per hash bucket).
    spill_writer: Option<SpillWriter>,
    /// Shared memory reservation from the pool-based spill manager.
    reservation: MemoryReservation,
    /// Optional cap on resident bytes before spilling is triggered.
    cap: Option<u64>,
}

impl WindowBaseState {
    pub fn make_base_state(
        num_partitions: usize,
        spill_dirs: Option<Vec<String>>,
        cap: Option<u64>,
    ) -> DaftResult<Self> {
        let inner_states = (0..num_partitions).map(|_| None).collect::<Vec<_>>();
        Ok(Self {
            inner_states,
            spill_dirs,
            spill_writer: None,
            reservation: get_or_init_memory_manager().reservation(),
            cap,
        })
    }

    pub fn push(
        &mut self,
        input: MicroPartition,
        partition_by: &[BoundExpr],
        _sink_name: &str,
    ) -> DaftResult<()> {
        let Self {
            inner_states,
            spill_dirs,
            spill_writer,
            reservation,
            cap,
        } = self;

        let partitioned = input.partition_by_hash(partition_by, inner_states.len())?;
        for (p, state) in partitioned.into_iter().zip(inner_states.iter_mut()) {
            let state = state.get_or_insert_with(SinglePartitionWindowState::default);
            for table in p.record_batches() {
                state.bytes += table.size_bytes();
                state.partitions.push(table.clone());
            }
        }

        if let Some(dirs) = spill_dirs.as_ref() {
            let mut buckets = WindowBuckets {
                inner_states,
                spill_writer,
                spill_dirs: dirs.clone(),
            };
            reconcile_reservation(&mut buckets, reservation, *cap)?;
        }
        Ok(())
    }

}

/// Adapter so `reconcile_reservation` can spill window buckets (raw rows; window fns aren't
/// decomposable). Spills the single heaviest bucket per call.
struct WindowBuckets<'a> {
    inner_states: &'a mut [Option<SinglePartitionWindowState>],
    spill_writer: &'a mut Option<SpillWriter>,
    spill_dirs: Vec<String>,
}

impl SpillableBuckets for WindowBuckets<'_> {
    fn resident_bytes(&self) -> u64 {
        self.inner_states
            .iter()
            .flatten()
            .map(|st| st.bytes as u64)
            .sum()
    }

    fn spill_largest_bucket(&mut self) -> DaftResult<bool> {
        let num_buckets = self.inner_states.len();
        let Some(p) = (0..num_buckets)
            .filter(|&p| self.inner_states[p].as_ref().is_some_and(|st| st.bytes > 0))
            .max_by_key(|&p| self.inner_states[p].as_ref().unwrap().bytes)
        else {
            return Ok(false);
        };
        let st = self.inner_states[p].as_mut().unwrap();
        let batches = std::mem::take(&mut st.partitions);
        st.bytes = 0;
        debug_assert!(!batches.is_empty(), "window bucket selected with bytes>0 but no resident data (counter drift)");
        if batches.is_empty() {
            return Ok(true);
        }
        let writer = match self.spill_writer {
            Some(w) => w,
            None => {
                let schema = batches[0].schema.clone();
                let w = SpillWriter::new(
                    num_buckets,
                    &schema,
                    self.spill_dirs.clone(),
                    "daft_window_spill_",
                )?;
                *self.spill_writer = Some(w);
                self.spill_writer.as_mut().unwrap()
            }
        };
        for b in &batches {
            writer.write_batch(p, b)?;
        }
        Ok(true)
    }
}

impl WindowBaseState {
    /// Consume the state for finalize: take the per-bucket in-memory rows and seal the spill writer
    /// into an immutable store.
    fn into_finalize_parts(
        self,
    ) -> DaftResult<(Vec<Option<SinglePartitionWindowState>>, Option<SpillStore>)> {
        let Self {
            inner_states,
            spill_writer,
            ..
        } = self;
        let store = match spill_writer {
            Some(w) => Some(w.finish()?),
            None => None,
        };
        Ok((inner_states, store))
    }
}

/// Finalize all hash buckets for a partitioned window sink with bounded concurrency, reading each
/// bucket's spilled rows on demand so peak memory stays bounded. `compute_bucket` turns one bucket's
/// gathered rows into the window result for that bucket.
pub(super) async fn finalize_partitioned_windows<F>(
    states: Vec<WindowBaseState>,
    num_partitions: usize,
    original_schema: SchemaRef,
    compute_bucket: F,
) -> DaftResult<BlockingSinkOutput>
where
    F: Fn(Vec<RecordBatch>) -> DaftResult<RecordBatch> + Send + Sync + 'static,
{
    // Seal each input state: take its in-memory buckets, finish its spill file into a store.
    let mut inners: Vec<Vec<Option<SinglePartitionWindowState>>> = Vec::with_capacity(states.len());
    let mut stores: Vec<Option<SpillStore>> = Vec::with_capacity(states.len());
    for state in states {
        let (inner, store) = state.into_finalize_parts()?;
        inners.push(inner);
        stores.push(store);
    }
    let stores = Arc::new(stores);

    // Gather, per bucket, the in-memory rows across all input states.
    let mut per_bucket_inmem: Vec<Vec<RecordBatch>> = (0..num_partitions)
        .map(|p| {
            let mut v = vec![];
            for inner in inners.iter_mut() {
                if let Some(Some(st)) = inner.get_mut(p).map(Option::take) {
                    v.extend(st.partitions);
                }
            }
            v
        })
        .collect();

    let compute = Arc::new(compute_bucket);
    let max_inflight = get_compute_pool_num_threads().max(1);
    let mut tasks: tokio::task::JoinSet<DaftResult<Option<RecordBatch>>> =
        tokio::task::JoinSet::new();
    let mut next = 0usize;

    let mut spawn_next = |tasks: &mut tokio::task::JoinSet<DaftResult<Option<RecordBatch>>>,
                          next: &mut usize| {
        if *next >= num_partitions {
            return;
        }
        let p = *next;
        *next += 1;
        let inmem = std::mem::take(&mut per_bucket_inmem[p]);
        let stores = stores.clone();
        let compute = compute.clone();
        tasks.spawn(async move {
            let mut all = inmem;
            for store in stores.iter().flatten() {
                if store.is_spilled(p) {
                    all.extend(store.read_bucket(p)?);
                }
            }
            if all.is_empty() {
                return Ok(None);
            }
            Ok(Some(compute(all)?))
        });
    };

    while tasks.len() < max_inflight && next < num_partitions {
        spawn_next(&mut tasks, &mut next);
    }

    let mut results = vec![];
    while let Some(res) = tasks.join_next().await {
        if let Some(batch) = res.map_err(|e| DaftError::InternalError(e.to_string()))?? {
            results.push(batch);
        }
        spawn_next(&mut tasks, &mut next);
    }

    if results.is_empty() {
        return Ok(BlockingSinkOutput::Partitions(vec![MicroPartition::empty(
            Some(original_schema),
        )]));
    }
    Ok(BlockingSinkOutput::Partitions(vec![
        MicroPartition::new_loaded(original_schema, results.into(), None),
    ]))
}

/// Build the optional spill dirs (None when spilling disabled) to seed a `WindowBaseState`.
pub(super) fn window_spill_dirs(spill_config: &Option<SpillConfig>) -> Option<Vec<String>> {
    spill_config.as_ref().map(|sc| sc.spill_dirs.clone())
}

/// Base trait for window sink params
#[allow(dead_code)]
pub trait WindowSinkParams: Send + Sync {
    fn original_schema(&self) -> &SchemaRef;
    fn partition_by(&self) -> &[BoundExpr];
    fn name(&self) -> &'static str;
}

/// Groups rows by the partition_by columns, returning one Vec<u64> of row indices per group.
pub(super) fn partition_into_groups(
    all_partitions: &[RecordBatch],
    partition_by: &[BoundExpr],
) -> DaftResult<Vec<Vec<u64>>> {
    let partition_key_batches: Vec<RecordBatch> = all_partitions
        .iter()
        .map(|p| p.eval_expression_list(partition_by))
        .collect::<DaftResult<_>>()?;
    let partition_keys = RecordBatch::concat(&partition_key_batches)?;
    let (_, groups) = partition_keys.make_groups()?;
    Ok(groups
        .into_iter()
        .map(|g| g.into_iter().collect())
        .collect())
}

/// Sorts each group by the order_by columns and materializes them as RecordBatches,
/// skipping empty groups.
pub(super) fn sort_and_materialize_groups(
    groups: Vec<Vec<u64>>,
    full_data: RecordBatch,
    order_by: &[BoundExpr],
    descending: &[bool],
    nulls_first: &[bool],
) -> DaftResult<Vec<RecordBatch>> {
    let order_keys = full_data.eval_expression_list(order_by)?;
    let order_key_series: Vec<Series> = order_keys
        .columns()
        .iter()
        .map(|c| c.as_materialized_series().clone())
        .collect();
    let cmp = build_multi_array_compare(&order_key_series, descending, nulls_first)?;

    groups
        .into_iter()
        .filter(|g| !g.is_empty())
        .map(|mut group_idxs| {
            group_idxs.sort_by(|&a, &b| cmp(a as usize, b as usize));
            full_data.take(&UInt64Array::from_vec("idx", group_idxs))
        })
        .collect()
}
