use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::JoinType;
use daft_micropartition::MicroPartition;
use daft_recordbatch::{ProbeState, ProbeableBuilder, RecordBatch, make_probeable_builder};
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
    spill::{SpillConfig, SpillStore, SpillWriter},
};

// ─── Build state ─────────────────────────────────────────────────────────────

struct PartitionedBuildData {
    partition_count: usize,
    per_partition_tables: Vec<Vec<RecordBatch>>,
    per_partition_size_bytes: Vec<usize>,
    threshold_per_partition: usize,
    spill_writer: SpillWriter,
}

pub(crate) struct HashJoinBuildState {
    // In-memory (single partition) path.
    probe_table_builder: Box<dyn ProbeableBuilder>,
    tables: Vec<RecordBatch>,
    // Spill path (None = in-memory only).
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
    pub(crate) probe_spill_buffers: Vec<Vec<RecordBatch>>,
    pub(crate) spill_store: Option<Arc<SpillStore>>,
    pub(crate) partition_count: usize,
}

// ─── HashJoinBuildState impl ──────────────────────────────────────────────────

impl HashJoinBuildState {
    fn new_in_memory(
        key_schema: &SchemaRef,
        nulls_equal_aware: Option<&Vec<bool>>,
        track_indices: bool,
    ) -> DaftResult<Self> {
        Ok(Self {
            probe_table_builder: make_probeable_builder(
                key_schema.clone(),
                nulls_equal_aware,
                track_indices,
            )?,
            tables: Vec::new(),
            partitioned: None,
        })
    }

    fn new_partitioned(
        key_schema: &SchemaRef,
        nulls_equal_aware: Option<&Vec<bool>>,
        track_indices: bool,
        build_schema: &SchemaRef,
        config: &SpillConfig,
    ) -> DaftResult<Self> {
        let partition_count = config.partition_count();
        let threshold_per_partition = (config.threshold_bytes / partition_count).max(1);
        let spill_writer = SpillWriter::new(
            partition_count,
            build_schema,
            config.spill_dirs.clone(),
            "daft_join_spill_",
        )?;
        Ok(Self {
            probe_table_builder: make_probeable_builder(
                key_schema.clone(),
                nulls_equal_aware,
                track_indices,
            )?,
            tables: Vec::new(),
            partitioned: Some(PartitionedBuildData {
                partition_count,
                per_partition_tables: vec![Vec::new(); partition_count],
                per_partition_size_bytes: vec![0; partition_count],
                threshold_per_partition,
                spill_writer,
            }),
        })
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
                    let byte_size = sub_batch.size_bytes();
                    pd.per_partition_size_bytes[p] += byte_size;
                    pd.per_partition_tables[p].push(sub_batch);
                    if pd.per_partition_size_bytes[p] > pd.threshold_per_partition {
                        let batches = std::mem::take(&mut pd.per_partition_tables[p]);
                        for b in &batches {
                            pd.spill_writer.write_batch(p, b)?;
                        }
                        pd.per_partition_size_bytes[p] = 0;
                    }
                }
            }
        } else {
            let input_tables = input.record_batches();
            if input_tables.is_empty() {
                let empty_table = RecordBatch::empty(Some(input.schema()));
                let join_keys = empty_table.eval_expression_list(&params.build_on)?;
                self.probe_table_builder.add_table(&join_keys)?;
                self.tables.push(empty_table);
            } else {
                for table in input_tables {
                    self.tables.push(table.clone());
                    let join_keys = table.eval_expression_list(&params.build_on)?;
                    self.probe_table_builder.add_table(&join_keys)?;
                }
            }
        }
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
            let pt = self.probe_table_builder.build();
            let probe_state = ProbeState::new(pt, self.tables);
            Ok(SpillableProbeState {
                in_memory_probes: vec![Some(probe_state)],
                spill_store: None,
                partition_count: 1,
                params: op.params.clone(),
            })
        }
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
    Ok(ProbeState::new(pt, tables.to_vec()))
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
            None => HashJoinBuildState::new_in_memory(
                &self.params.key_schema,
                self.params.nulls_equal_aware.as_ref(),
                self.params.track_indices,
            ),
            Some(config) => HashJoinBuildState::new_partitioned(
                &self.params.key_schema,
                self.params.nulls_equal_aware.as_ref(),
                self.params.track_indices,
                self.build_schema(),
                config,
            ),
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
                probe_spill_buffers: vec![],
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

            HashJoinProbeState {
                probe_state: representative_probe_state,
                bitmap_builder: None,
                in_memory_probes: finalized.in_memory_probes,
                in_memory_bitmap_builders,
                probe_spill_buffers: vec![Vec::new(); partition_count],
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
                                    state.probe_spill_buffers[p].push(sub_batch);
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
                                    probe_spill_buffers: vec![],
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

                    // 2. Finalize spilled partitions.
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

                            let probe_batches: Vec<RecordBatch> = states
                                .iter_mut()
                                .flat_map(|s| std::mem::take(&mut s.probe_spill_buffers[p]))
                                .collect();

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

                            for probe_batch in &probe_batches {
                                let mp = MicroPartition::new_loaded(
                                    probe_batch.schema.clone(),
                                    Arc::new(vec![probe_batch.clone()]),
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

                            if needs_bitmap {
                                let fake_state = HashJoinProbeState {
                                    probe_state,
                                    bitmap_builder,
                                    in_memory_probes: vec![],
                                    in_memory_bitmap_builders: vec![],
                                    probe_spill_buffers: vec![],
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
                "Spill: threshold={}B, partitions={}",
                sc.threshold_bytes,
                sc.partition_count()
            ));
        }
        display
    }

    fn needs_probe_finalization(&self) -> bool {
        // Need finalization if we track bitmaps (left/right/outer/anti/semi), OR if spill is
        // configured (we must drain probe_spill_buffers against spilled build partitions).
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
