use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::JoinType;
use daft_micropartition::MicroPartition;
use daft_recordbatch::{ProbeState, RecordBatch, make_probeable_builder};
use indexmap::IndexSet;
use itertools::Itertools;
use tracing::{Span, info_span};

use crate::{
    ExecutionTaskSpawner,
    join::{
        anti_semi_join::{finalize_anti_semi, probe_anti_semi, probe_anti_semi_with_bitmap},
        index_bitmap::IndexBitmapBuilder,
        inner_join::probe_inner,
        join_operator::{
            BuildStateResult, FinalizeBuildResult, JoinOperator, ProbeFinalizeResult, ProbeOutput,
            ProbeResult,
        },
        left_right_join::{
            finalize_left, finalize_right, probe_left_right, probe_left_right_with_bitmap,
        },
        outer_join::{finalize_outer, probe_outer},
    },
    pipeline::NodeName,
    resource_manager::{MemoryManager, MemoryReservation, SpillableBuckets,
        get_or_init_memory_manager, reconcile_reservation},
    spill::{SpillConfig, SpillStore, SpillWriter},
};

// ─── Build state ─────────────────────────────────────────────────────────────

struct PartitionedBuildData {
    partition_count: usize,
    per_partition_tables: Vec<Vec<RecordBatch>>,
    per_partition_size_bytes: Vec<usize>,
    spill_writer: SpillWriter,
}

/// Spill context: present whenever spill is configured for this operator. Holds the shared
/// memory reservation used to detect real memory pressure before escalating to the partitioned
/// build layout.
struct SpillState {
    config: SpillConfig,
    reservation: MemoryReservation,
}

pub(crate) struct HashJoinBuildState {
    // Accumulated build batches (raw). The probe hash table is built once at `finalize` from these,
    // not incrementally — so escalation to the partitioned layout wastes no probe-table work.
    tables: Vec<RecordBatch>,
    // Spill context: `Some` whenever spill is enabled (whether or not we've escalated yet).
    spill: Option<SpillState>,
    // Partitioned+spill layout. `None` until real memory pressure forces escalation.
    partitioned: Option<PartitionedBuildData>,
}

// ─── Finalized build state ────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct SpillableProbeState {
    pub in_memory_probes: Vec<Option<ProbeState>>,
    pub spill_store: Option<Arc<SpillStore>>,
    pub partition_count: usize,
    pub params: Arc<HashJoinParams>,
}

// ─── Probe state ─────────────────────────────────────────────────────────────

pub(crate) struct HashJoinProbeState {
    // Primary fields used by existing finalize helpers (outer_join.rs etc.).
    pub(crate) probe_state: ProbeState,
    pub(crate) bitmap_builder: Option<IndexBitmapBuilder>,
    // Spill-path extensions (empty/None when partition_count == 1).
    pub(crate) in_memory_probes: Vec<Option<ProbeState>>,
    pub(crate) in_memory_bitmap_builders: Vec<Option<IndexBitmapBuilder>>,
    /// Probe rows destined for spilled build partitions are written straight to disk (one bucket per
    /// partition) instead of buffered in memory, keeping the probe side memory-bounded regardless of
    /// its size. Created lazily on the first spilled-partition probe row.
    pub(crate) probe_spill_writer: Option<SpillWriter>,
    /// Spill directories for `probe_spill_writer` (empty when partition_count == 1).
    pub(crate) spill_dirs: Vec<String>,
    pub(crate) spill_store: Option<Arc<SpillStore>>,
    pub(crate) partition_count: usize,
}

// ─── HashJoinBuildState impl ──────────────────────────────────────────────────

impl HashJoinBuildState {
    fn new_in_memory() -> Self {
        Self {
            tables: Vec::new(),
            spill: None,
            partitioned: None,
        }
    }

    /// Spill-enabled build state. Starts in the lean in-memory layout (no hashing/scattering);
    /// escalates to the partitioned+spill layout only when `add_tables` detects real memory
    /// pressure (the shared spill reservation is denied).
    fn new_spillable(config: SpillConfig, manager: &Arc<MemoryManager>) -> Self {
        Self {
            tables: Vec::new(),
            spill: Some(SpillState {
                config,
                reservation: manager.reservation(),
            }),
            partitioned: None,
        }
    }

    fn add_tables(&mut self, input: &MicroPartition, params: &HashJoinParams) -> DaftResult<()> {
        if let Some(ref mut pd) = self.partitioned {
            for table in input.record_batches() {
                let sub_batches =
                    table.partition_by_hash(&params.build_on, pd.partition_count)?;
                for (p, sub_batch) in sub_batches.into_iter().enumerate() {
                    if sub_batch.is_empty() {
                        continue;
                    }
                    pd.per_partition_size_bytes[p] += sub_batch.size_bytes();
                    pd.per_partition_tables[p].push(sub_batch);
                }
            }
            let Self { partitioned, spill, .. } = self;
            let pd = partitioned.as_mut().expect("checked Some above");
            let spill_state = spill.as_mut().expect("partitioned implies spill enabled");
            let PartitionedBuildData {
                per_partition_tables,
                per_partition_size_bytes,
                spill_writer,
                ..
            } = pd;
            let mut buckets = JoinBuildBuckets {
                per_partition_tables,
                per_partition_size_bytes,
                spill_writer,
            };
            reconcile_reservation(&mut buckets, &mut spill_state.reservation, spill_state.config.cap())?;
        } else {
            // Accumulate raw batches only; the probe table is built once at finalize (an empty
            // build is handled there by seeding a schema-carrying empty batch).
            for table in input.record_batches() {
                self.tables.push(table.clone());
            }

            let denied = if let Some(spill_state) = &mut self.spill {
                let added = input.size_bytes() as u64;
                let over_cap = spill_state
                    .config
                    .cap()
                    .is_some_and(|c| spill_state.reservation.held() + added > c);
                over_cap || !spill_state.reservation.try_grow(added)
            } else {
                false
            };
            if denied {
                self.escalate(params)?;
            }
        }
        Ok(())
    }

    /// Escalate from the in-memory layout to the partitioned+spill layout. Called once, the first
    /// time the shared spill reservation is denied (real memory pressure).
    fn escalate(&mut self, params: &HashJoinParams) -> DaftResult<()> {
        let spill_state = self
            .spill
            .as_ref()
            .expect("escalate only called when spill is enabled");
        let partition_count = spill_state.config.partition_count();
        let spill_dirs = spill_state.config.spill_dirs.clone();
        let cap = spill_state.config.cap();

        let build_schema = if params.build_on_left {
            &params.left_schema
        } else {
            &params.right_schema
        };
        let spill_writer = SpillWriter::new(
            partition_count,
            build_schema,
            spill_dirs,
            "daft_join_spill_",
        )?;

        let mut per_partition_tables: Vec<Vec<RecordBatch>> = vec![Vec::new(); partition_count];
        let mut per_partition_size_bytes: Vec<usize> = vec![0; partition_count];
        for table in std::mem::take(&mut self.tables) {
            let sub_batches = table.partition_by_hash(&params.build_on, partition_count)?;
            for (p, sub_batch) in sub_batches.into_iter().enumerate() {
                if sub_batch.is_empty() {
                    continue;
                }
                per_partition_size_bytes[p] += sub_batch.size_bytes();
                per_partition_tables[p].push(sub_batch);
            }
        }

        let mut spill_writer = spill_writer;
        {
            let mut buckets = JoinBuildBuckets {
                per_partition_tables: &mut per_partition_tables,
                per_partition_size_bytes: &mut per_partition_size_bytes,
                spill_writer: &mut spill_writer,
            };
            let spill_state = self
                .spill
                .as_mut()
                .expect("escalate only called when spill is enabled");
            reconcile_reservation(&mut buckets, &mut spill_state.reservation, cap)?;
        }

        self.partitioned = Some(PartitionedBuildData {
            partition_count,
            per_partition_tables,
            per_partition_size_bytes,
            spill_writer,
        });
        Ok(())
    }

    fn finalize(self, op: &HashJoinOperator) -> DaftResult<SpillableProbeState> {
        if let Some(mut pd) = self.partitioned {
            // Flush remaining in-memory data for already-spilled partitions.
            for p in 0..pd.partition_count {
                if pd.spill_writer.has_spilled(p) && !pd.per_partition_tables[p].is_empty() {
                    let batches = std::mem::take(&mut pd.per_partition_tables[p]);
                    for b in &batches {
                        pd.spill_writer.write_batch(p, b)?;
                    }
                }
            }
            let spill_store = Arc::new(pd.spill_writer.finish()?);
            let mut in_memory_probes: Vec<Option<ProbeState>> =
                Vec::with_capacity(pd.partition_count);
            for p in 0..pd.partition_count {
                if spill_store.is_spilled(p) {
                    in_memory_probes.push(None);
                } else {
                    let probe_state = build_probe_state_from_batches(&pd.per_partition_tables[p], op)?;
                    in_memory_probes.push(Some(probe_state));
                }
            }
            Ok(SpillableProbeState {
                in_memory_probes,
                spill_store: Some(spill_store),
                partition_count: pd.partition_count,
                params: op.params.clone(),
            })
        } else {
            let probe_state = build_probe_state_from_batches(&self.tables, op)?;
            Ok(SpillableProbeState {
                in_memory_probes: vec![Some(probe_state)],
                spill_store: None,
                partition_count: 1,
                params: op.params.clone(),
            })
        }
    }
}

/// Adapter so `reconcile_reservation` can spill hash-join build partitions: spills the heaviest
/// in-memory partition's batches to its bucket file.
struct JoinBuildBuckets<'a> {
    per_partition_tables: &'a mut Vec<Vec<RecordBatch>>,
    per_partition_size_bytes: &'a mut Vec<usize>,
    spill_writer: &'a mut SpillWriter,
}

impl SpillableBuckets for JoinBuildBuckets<'_> {
    fn resident_bytes(&self) -> u64 {
        self.per_partition_size_bytes.iter().map(|b| *b as u64).sum()
    }

    fn spill_largest_bucket(&mut self) -> DaftResult<bool> {
        let Some((p, _)) = self
            .per_partition_size_bytes
            .iter()
            .enumerate()
            .filter(|(_, b)| **b > 0)
            .max_by_key(|(_, b)| **b)
        else {
            return Ok(false);
        };
        let batches = std::mem::take(&mut self.per_partition_tables[p]);
        for b in &batches {
            self.spill_writer.write_batch(p, b)?;
        }
        self.per_partition_size_bytes[p] = 0;
        Ok(true)
    }
}

fn build_probe_state_from_batches(
    tables: &[RecordBatch],
    op: &HashJoinOperator,
) -> DaftResult<ProbeState> {
    let mut builder = make_probeable_builder(
        op.params.key_schema.clone(),
        op.params.nulls_equal_aware.as_ref(),
        op.params.track_indices,
    )?;
    for table in tables {
        let keys = table.eval_expression_list(&op.params.build_on)?;
        builder.add_table(&keys)?;
    }
    let pt = builder.build();
    // A build partition can receive zero rows while the matching probe partition has rows
    // (e.g. probe keys that hash here but have no build-side match). The downstream probe
    // (`GrowableRecordBatch::new`) requires at least one build table to carry the schema, so
    // seed a single empty batch when this partition is empty. Probing against it yields no
    // matches — correct for every join type — and mirrors how the non-partitioned build path
    // (`add_tables`) seeds an empty table when it receives no input.
    let resident = if tables.is_empty() {
        vec![RecordBatch::empty(Some(op.build_schema().clone()))]
    } else {
        tables.to_vec()
    };
    Ok(ProbeState::new(pt, resident))
}

// ─── Params + operator ───────────────────────────────────────────────────────

pub struct HashJoinParams {
    pub key_schema: SchemaRef,
    pub build_on: Vec<BoundExpr>,
    pub probe_on: Vec<BoundExpr>,
    pub nulls_equal_aware: Option<Vec<bool>>,
    pub track_indices: bool,
    pub join_type: JoinType,
    pub build_on_left: bool,
    pub left_schema: SchemaRef,
    pub right_schema: SchemaRef,
    pub common_join_cols: IndexSet<String>,
    pub output_schema: SchemaRef,
    pub spill_config: Option<SpillConfig>,
}

pub struct HashJoinOperator {
    params: Arc<HashJoinParams>,
}

impl HashJoinOperator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        key_schema: SchemaRef,
        build_on: Vec<BoundExpr>,
        probe_on: Vec<BoundExpr>,
        nulls_equal_aware: Option<Vec<bool>>,
        track_indices: bool,
        join_type: JoinType,
        build_on_left: bool,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        common_join_cols: IndexSet<String>,
        output_schema: SchemaRef,
        spill_config: Option<SpillConfig>,
    ) -> DaftResult<Self> {
        Ok(Self {
            params: Arc::new(HashJoinParams {
                key_schema,
                build_on,
                probe_on,
                nulls_equal_aware,
                track_indices,
                join_type,
                build_on_left,
                left_schema,
                right_schema,
                common_join_cols,
                output_schema,
                spill_config,
            }),
        })
    }

    fn build_schema(&self) -> &SchemaRef {
        if self.params.build_on_left {
            &self.params.left_schema
        } else {
            &self.params.right_schema
        }
    }

    /// Whether the probe phase needs a bitmap to keep track of matched rows.
    fn needs_bitmap(&self) -> bool {
        matches!(self.params.join_type, JoinType::Outer)
            || (self.params.join_type == JoinType::Right && !self.params.build_on_left)
            || (self.params.join_type == JoinType::Left && self.params.build_on_left)
            || (matches!(self.params.join_type, JoinType::Anti | JoinType::Semi)
                && self.params.build_on_left)
    }

    fn finalize_probe_in_memory(
        &self,
        states: Vec<HashJoinProbeState>,
        spawner: &ExecutionTaskSpawner,
    ) -> ProbeFinalizeResult {
        let params = self.params.clone();
        match self.params.join_type {
            JoinType::Outer => spawner
                .spawn(
                    async move {
                        let output = finalize_outer(states, &params).await?;
                        Ok(output)
                    },
                    Span::current(),
                )
                .into(),
            JoinType::Left => {
                debug_assert!(self.params.build_on_left);
                spawner
                    .spawn(
                        async move {
                            let output = finalize_left(states, &params).await?;
                            Ok(output)
                        },
                        Span::current(),
                    )
                    .into()
            }
            JoinType::Right => {
                debug_assert!(!self.params.build_on_left);
                spawner
                    .spawn(
                        async move {
                            let output = finalize_right(states, &params).await?;
                            Ok(output)
                        },
                        Span::current(),
                    )
                    .into()
            }
            JoinType::Anti | JoinType::Semi => {
                debug_assert!(self.params.build_on_left);
                let is_semi = self.params.join_type == JoinType::Semi;
                spawner
                    .spawn(
                        async move {
                            let output = finalize_anti_semi(states, is_semi).await?;
                            Ok(output)
                        },
                        Span::current(),
                    )
                    .into()
            }
            _ => unreachable!(
                "Hash join probe finalization not expected for join type {:?}",
                self.params.join_type
            ),
        }
    }
}

// ─── JoinOperator impl ────────────────────────────────────────────────────────

impl JoinOperator for HashJoinOperator {
    type BuildState = HashJoinBuildState;
    type FinalizedBuildState = SpillableProbeState;
    type ProbeState = HashJoinProbeState;

    fn make_build_state(&self) -> DaftResult<Self::BuildState> {
        tracing::debug!("make_build_state: spill_config={:?}", self.params.spill_config);
        match &self.params.spill_config {
            None => Ok(HashJoinBuildState::new_in_memory()),
            Some(config) => Ok(HashJoinBuildState::new_spillable(
                config.clone(),
                get_or_init_memory_manager(),
            )),
        }
    }

    fn build(
        &self,
        input: MicroPartition,
        mut state: Self::BuildState,
        spawner: &ExecutionTaskSpawner,
    ) -> BuildStateResult<Self> {
        let params = self.params.clone();
        spawner
            .spawn(
                async move {
                    state.add_tables(&input, &params)?;
                    Ok(state)
                },
                info_span!("HashJoinOperator::build_state"),
            )
            .into()
    }

    fn finalize_build(&self, state: Self::BuildState) -> FinalizeBuildResult<Self> {
        state.finalize(self).into()
    }

    fn make_probe_state(
        &self,
        finalized: Self::FinalizedBuildState,
    ) -> Self::ProbeState {
        let needs_bitmap = self.needs_bitmap();
        let partition_count = finalized.partition_count;

        if partition_count == 1 {
            let probe_state = finalized.in_memory_probes[0]
                .clone()
                .expect("single partition must be in memory");
            let record_batches = probe_state.get_record_batches().to_vec();
            HashJoinProbeState {
                bitmap_builder: if needs_bitmap {
                    Some(IndexBitmapBuilder::new(&record_batches))
                } else {
                    None
                },
                probe_state,
                in_memory_probes: vec![],
                in_memory_bitmap_builders: vec![],
                probe_spill_writer: None,
                spill_dirs: vec![],
                spill_store: None,
                partition_count: 1,
            }
        } else {
            let representative_probe_state = finalized
                .in_memory_probes
                .iter()
                .find_map(|ps| ps.as_ref())
                .cloned()
                .unwrap_or_else(|| {
                    let builder = make_probeable_builder(
                        finalized.params.key_schema.clone(),
                        finalized.params.nulls_equal_aware.as_ref(),
                        finalized.params.track_indices,
                    )
                    .expect("make_probeable_builder should not fail");
                    ProbeState::new(builder.build(), vec![])
                });

            let in_memory_bitmap_builders = if needs_bitmap {
                finalized
                    .in_memory_probes
                    .iter()
                    .map(|opt_ps| {
                        opt_ps
                            .as_ref()
                            .map(|ps| IndexBitmapBuilder::new(ps.get_record_batches()))
                    })
                    .collect()
            } else {
                (0..partition_count).map(|_| None).collect()
            };

            let spill_dirs = finalized
                .params
                .spill_config
                .as_ref()
                .map(|c| c.spill_dirs.clone())
                .unwrap_or_default();
            HashJoinProbeState {
                probe_state: representative_probe_state,
                bitmap_builder: None,
                in_memory_probes: finalized.in_memory_probes,
                in_memory_bitmap_builders,
                probe_spill_writer: None,
                spill_dirs,
                spill_store: finalized.spill_store,
                partition_count,
            }
        }
    }

    fn probe(
        &self,
        input: MicroPartition,
        mut state: Self::ProbeState,
        spawner: &ExecutionTaskSpawner,
    ) -> ProbeResult<Self> {
        if input.is_empty() {
            let empty = MicroPartition::empty(Some(self.params.output_schema.clone()));
            return Ok((state, ProbeOutput::NeedMoreInput(Some(empty)))).into();
        }

        let needs_bitmap = self.needs_bitmap();
        let params = self.params.clone();

        if state.partition_count == 1 {
            spawner
                .spawn(
                    async move {
                        let result = probe_dispatch(
                            &input,
                            &state.probe_state,
                            state.bitmap_builder.as_mut(),
                            &params,
                            needs_bitmap,
                        )?;
                        Ok((state, ProbeOutput::NeedMoreInput(result)))
                    },
                    Span::current(),
                )
                .into()
        } else {
            spawner
                .spawn(
                    async move {
                        let mut outputs: Vec<MicroPartition> = Vec::new();
                        for table in input.record_batches() {
                            let sub_batches = table
                                .partition_by_hash(&params.probe_on, state.partition_count)?;
                            for (p, sub_batch) in sub_batches.into_iter().enumerate() {
                                if sub_batch.is_empty() {
                                    continue;
                                }
                                if let Some(probe_state) = state.in_memory_probes[p].as_ref() {
                                    let sub_mp = MicroPartition::new_loaded(
                                        sub_batch.schema.clone(),
                                        Arc::new(vec![sub_batch]),
                                        None,
                                    );
                                    if let Some(out) = probe_dispatch(
                                        &sub_mp,
                                        probe_state,
                                        state.in_memory_bitmap_builders[p].as_mut(),
                                        &params,
                                        needs_bitmap,
                                    )? {
                                        outputs.push(out);
                                    }
                                } else {
                                    // Build partition p is on disk; write the probe rows straight to
                                    // disk too (streamed back at finalize) instead of buffering them
                                    // in memory, so the probe side stays memory-bounded.
                                    if state.probe_spill_writer.is_none() {
                                        state.probe_spill_writer = Some(SpillWriter::new(
                                            state.partition_count,
                                            &sub_batch.schema,
                                            state.spill_dirs.clone(),
                                            "daft_join_probe_spill_",
                                        )?);
                                    }
                                    state
                                        .probe_spill_writer
                                        .as_mut()
                                        .expect("just created")
                                        .write_batch(p, &sub_batch)?;
                                }
                            }
                        }
                        let combined = if outputs.is_empty() {
                            Some(MicroPartition::empty(Some(params.output_schema.clone())))
                        } else {
                            Some(MicroPartition::concat(outputs)?)
                        };
                        Ok((state, ProbeOutput::NeedMoreInput(combined)))
                    },
                    Span::current(),
                )
                .into()
        }
    }

    fn finalize_probe(
        &self,
        mut states: Vec<Self::ProbeState>,
        spawner: &ExecutionTaskSpawner,
    ) -> ProbeFinalizeResult {
        debug_assert!(
            self.needs_probe_finalization(),
            "Hash join probe finalize should only be called if the probe phase needs finalization"
        );

        let partition_count = states.first().map_or(1, |s| s.partition_count);

        if partition_count == 1 {
            if !self.needs_bitmap() {
                // Spill is configured but this join stayed in the lean in-memory layout
                // (partition_count == 1, nothing spilled). A non-bitmap join (e.g. Inner) has no
                // unmatched rows to emit and no spilled partitions to drain, so finalization is a
                // no-op. This case only became reachable with adaptive spill: previously a
                // spill-enabled join was always partitioned (partition_count >= 2), so only
                // bitmap-tracking join types ever reached the in-memory finalize path.
                return spawner
                    .spawn(async move { Ok(None::<MicroPartition>) }, Span::current())
                    .into();
            }
            return self.finalize_probe_in_memory(states, spawner);
        }

        // Multi-partition spill path.
        let params = self.params.clone();
        let needs_bitmap = self.needs_bitmap();
        spawner
            .spawn(
                async move {
                    let mut all_outputs: Vec<MicroPartition> = Vec::new();
                    let pc = states[0].partition_count;

                    // 1. Finalize in-memory partitions with bitmaps.
                    if needs_bitmap {
                        for p in 0..pc {
                            if states[0].in_memory_probes[p].is_none() {
                                continue;
                            }
                            let probe_state = states[0].in_memory_probes[p]
                                .clone()
                                .expect("in-memory probe state missing");
                            let synthetic_states: Vec<HashJoinProbeState> = states
                                .iter_mut()
                                .map(|s| HashJoinProbeState {
                                    probe_state: probe_state.clone(),
                                    bitmap_builder: s.in_memory_bitmap_builders[p].take(),
                                    in_memory_probes: vec![],
                                    in_memory_bitmap_builders: vec![],
                                    probe_spill_writer: None,
                                    spill_dirs: vec![],
                                    spill_store: None,
                                    partition_count: 1,
                                })
                                .collect();
                            if let Some(out) =
                                finalize_one_bitmap_partition(synthetic_states, &params).await?
                            {
                                all_outputs.push(out);
                            }
                        }
                    }

                    // 2. Finalize spilled partitions. Probe rows for spilled partitions were written
                    // to disk during the probe phase; close each worker's probe spill writer and
                    // stream the rows back one batch at a time so the probe side stays bounded.
                    let probe_stores: Vec<Option<SpillStore>> = {
                        let mut v = Vec::with_capacity(states.len());
                        for s in states.iter_mut() {
                            v.push(match s.probe_spill_writer.take() {
                                Some(w) => Some(w.finish()?),
                                None => None,
                            });
                        }
                        v
                    };
                    let spill_store = states[0].spill_store.clone();
                    if let Some(store) = spill_store {
                        for p in 0..pc {
                            if !store.is_spilled(p) {
                                continue;
                            }
                            let build_batches = store.read_bucket(p)?;
                            if build_batches.is_empty() {
                                continue;
                            }

                            let mut builder = make_probeable_builder(
                                params.key_schema.clone(),
                                params.nulls_equal_aware.as_ref(),
                                params.track_indices,
                            )?;
                            for b in &build_batches {
                                let keys = b.eval_expression_list(&params.build_on)?;
                                builder.add_table(&keys)?;
                            }
                            let pt = builder.build();
                            let probe_state = ProbeState::new(pt, build_batches.clone());

                            let mut bitmap_builder = if needs_bitmap {
                                Some(IndexBitmapBuilder::new(&build_batches))
                            } else {
                                None
                            };

                            // Stream each worker's spilled probe rows for partition p from disk,
                            // probing one batch at a time (only one probe batch resident).
                            for probe_store in probe_stores.iter().flatten() {
                                let Some(reader) = probe_store.open_bucket(p)? else {
                                    continue;
                                };
                                for batch_res in reader {
                                    let probe_batch = batch_res?;
                                    let mp = MicroPartition::new_loaded(
                                        probe_batch.schema.clone(),
                                        Arc::new(vec![probe_batch]),
                                        None,
                                    );
                                    if let Some(out) = probe_dispatch(
                                        &mp,
                                        &probe_state,
                                        bitmap_builder.as_mut(),
                                        &params,
                                        needs_bitmap,
                                    )? {
                                        all_outputs.push(out);
                                    }
                                }
                            }

                            if needs_bitmap {
                                let fake_state = HashJoinProbeState {
                                    probe_state,
                                    bitmap_builder,
                                    in_memory_probes: vec![],
                                    in_memory_bitmap_builders: vec![],
                                    probe_spill_writer: None,
                                    spill_dirs: vec![],
                                    spill_store: None,
                                    partition_count: 1,
                                };
                                if let Some(out) =
                                    finalize_one_bitmap_partition(vec![fake_state], &params)
                                        .await?
                                {
                                    all_outputs.push(out);
                                }
                            }
                        }
                    }

                    if all_outputs.is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some(MicroPartition::concat(all_outputs)?))
                    }
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        match self.params.join_type {
            JoinType::Inner => "Hash Join (Inner)".into(),
            JoinType::Left => "Hash Join (Left)".into(),
            JoinType::Right => "Hash Join (Right)".into(),
            JoinType::Outer => "Hash Join (Outer)".into(),
            JoinType::Anti => "Hash Join (Anti)".into(),
            JoinType::Semi => "Hash Join (Semi)".into(),
        }
    }

    fn op_type(&self) -> NodeType {
        NodeType::HashJoin
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut display = vec![];
        display.push(format!("Hash Join ({:?}):", self.params.join_type));
        display.push(format!("Build on left: {}", self.params.build_on_left));
        display.push(format!("Track Indices: {}", self.params.track_indices));
        display.push(format!(
            "Key Schema: {}",
            self.params.key_schema.short_string()
        ));
        if let Some(null_equals_nulls) = &self.params.nulls_equal_aware {
            display.push(format!(
                "Null equals Nulls = [{}]",
                null_equals_nulls.iter().map(|b| b.to_string()).join(", ")
            ));
        }
        if let Some(sc) = &self.params.spill_config {
            display.push(format!(
                "Spill: pool={}B, partitions={}",
                sc.pool_bytes,
                sc.partition_count()
            ));
        }
        display
    }

    fn needs_probe_finalization(&self) -> bool {
        // Need finalization if we track bitmaps (left/right/outer/anti/semi), OR if spill is
        // configured (we must drain spilled probe rows against spilled build partitions).
        self.needs_bitmap() || self.params.spill_config.is_some()
    }
}

// ─── Private helpers ──────────────────────────────────────────────────────────

fn probe_dispatch(
    input: &MicroPartition,
    probe_state: &ProbeState,
    bitmap_builder: Option<&mut IndexBitmapBuilder>,
    params: &HashJoinParams,
    needs_bitmap: bool,
) -> DaftResult<Option<MicroPartition>> {
    let out = match params.join_type {
        JoinType::Inner => Some(probe_inner(input, probe_state, params)?),
        JoinType::Left | JoinType::Right if needs_bitmap => {
            let bm = bitmap_builder.expect("bitmap required for left/right join with bitmap");
            Some(probe_left_right_with_bitmap(input, bm, probe_state, params)?)
        }
        JoinType::Left | JoinType::Right => Some(probe_left_right(input, probe_state, params)?),
        JoinType::Outer => {
            let bm = bitmap_builder.expect("bitmap required for outer join");
            Some(probe_outer(input, probe_state, Some(bm), params)?)
        }
        JoinType::Anti | JoinType::Semi if needs_bitmap => {
            let bm = bitmap_builder.expect("bitmap required for anti/semi join with bitmap");
            probe_anti_semi_with_bitmap(input, bm, probe_state, params)?;
            None
        }
        JoinType::Anti | JoinType::Semi => Some(probe_anti_semi(input, probe_state, params)?),
    };
    Ok(out)
}

async fn finalize_one_bitmap_partition(
    states: Vec<HashJoinProbeState>,
    params: &HashJoinParams,
) -> DaftResult<Option<MicroPartition>> {
    match params.join_type {
        JoinType::Outer => finalize_outer(states, params).await,
        JoinType::Left => finalize_left(states, params).await,
        JoinType::Right => finalize_right(states, params).await,
        JoinType::Anti => finalize_anti_semi(states, false).await,
        JoinType::Semi => finalize_anti_semi(states, true).await,
        _ => Ok(None),
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_core::{
        prelude::{DataType, Field, Int64Array, Schema},
        series::IntoSeries,
    };
    use daft_dsl::{expr::bound_expr::BoundExpr, resolved_col};
    use daft_micropartition::MicroPartition;
    use daft_recordbatch::RecordBatch;
    use indexmap::IndexSet;

    use super::*;
    use crate::{resource_manager::MemoryManager, spill::SpillConfig};

    /// Build a tiny single-column `k: Int64` HashJoinOperator + a small MicroPartition of build
    /// rows, sharing the schema/expr plumbing needed by both regression tests below.
    fn small_join_fixture() -> (HashJoinOperator, MicroPartition) {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64)]));

        let build_on_expr = vec![
            BoundExpr::try_new(resolved_col("k"), &schema).expect("bind build_on"),
        ];
        let probe_on_expr = vec![
            BoundExpr::try_new(resolved_col("k"), &schema).expect("bind probe_on"),
        ];

        let op = HashJoinOperator::new(
            schema.clone(),
            build_on_expr,
            probe_on_expr,
            None,
            false,
            JoinType::Inner,
            true,
            schema.clone(),
            schema.clone(),
            IndexSet::new(),
            schema.clone(),
            None, // spill_config set per-test via direct HashJoinBuildState construction
        )
        .expect("construct HashJoinOperator");

        let series = Int64Array::from_vec("k", vec![1, 2, 3]).into_series();
        let batch = RecordBatch::from_nonempty_columns(vec![series]).expect("build record batch");
        let mp = MicroPartition::new_loaded(schema, Arc::new(vec![batch]), None);

        (op, mp)
    }

    #[test]
    fn spill_enabled_small_build_stays_in_memory() {
        let (op, mp) = small_join_fixture();

        let manager = Arc::new(MemoryManager::new());
        // Huge pool: the reservation should never be denied for this tiny build.
        manager.set_spill_pool_bytes(1 << 40);

        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let config = SpillConfig::new(1 << 40, vec![temp_dir.path().to_string_lossy().into_owned()]);

        let mut state = HashJoinBuildState::new_spillable(config, &manager);

        state.add_tables(&mp, &op.params).expect("add_tables");

        let finalized = state.finalize(&op).expect("finalize");

        // Regression: with spill enabled but no real memory pressure, the build must stay on the
        // lean in-memory (single-partition) path, not eagerly hash-partition every batch.
        assert_eq!(finalized.partition_count, 1);
    }

    #[test]
    fn spill_enabled_escalates_under_pressure() {
        let (op, mp) = small_join_fixture();

        let manager = Arc::new(MemoryManager::new());
        // Tiny pool: the very first reservation grow attempt must be denied, forcing escalation.
        manager.set_spill_pool_bytes(1);

        let temp_dir = tempfile::tempdir().expect("create temp dir");
        // pool_bytes = 1<<30 => partition_count() == 4 (see SpillConfig::partition_count).
        let config = SpillConfig::new(1 << 30, vec![temp_dir.path().to_string_lossy().into_owned()]);
        assert_eq!(config.partition_count(), 4);

        let mut state = HashJoinBuildState::new_spillable(config, &manager);

        state.add_tables(&mp, &op.params).expect("add_tables");

        let finalized = state.finalize(&op).expect("finalize");

        // Under real memory pressure, the build must escalate to the partitioned+spill layout.
        assert!(finalized.partition_count > 1);
    }
}
