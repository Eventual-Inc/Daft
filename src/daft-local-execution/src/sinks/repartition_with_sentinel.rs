use std::{collections::VecDeque, sync::Arc};

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::Series;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::partitioning::RepartitionSpec;
use daft_micropartition::MicroPartition;
use daft_recordbatch::{RecordBatch, record_batch_max_composite};
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkOutput, BlockingSinkSinkResult,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
};

pub(crate) struct RayRepartitionWithSentinelState {
    states: VecDeque<Vec<MicroPartition>>,
}

impl RayRepartitionWithSentinelState {
    fn new(num_partitions: usize) -> Self {
        Self {
            states: (0..num_partitions).map(|_| vec![]).collect(),
        }
    }

    fn push(&mut self, parts: Vec<MicroPartition>) {
        for (vec, part) in self.states.iter_mut().zip(parts) {
            vec.push(part);
        }
    }

    fn emit(&mut self) -> Option<Vec<MicroPartition>> {
        self.states.pop_front()
    }
}

pub struct RepartitionWithSentinelSink {
    repartition_spec: RepartitionSpec,
    num_partitions: usize,
    sentinel_sort_keys: Vec<BoundExpr>,
}

impl RepartitionWithSentinelSink {
    pub fn new_ray(
        repartition_spec: RepartitionSpec,
        num_partitions: usize,
        sentinel_sort_keys: Vec<BoundExpr>,
    ) -> Self {
        Self {
            repartition_spec,
            num_partitions,
            sentinel_sort_keys,
        }
    }
}

/// Compute the max composite-key row across all record batches in a partition.
/// Returns `None` if the partition is empty.
fn compute_partition_max(
    partition: &MicroPartition,
    composite_keys: &[BoundExpr],
) -> DaftResult<Option<RecordBatch>> {
    let batches = partition.record_batches();
    let candidates: Vec<RecordBatch> = batches
        .iter()
        .filter_map(|b| record_batch_max_composite(b, composite_keys).transpose())
        .collect::<DaftResult<_>>()?;
    if candidates.is_empty() {
        return Ok(None);
    }
    if candidates.len() == 1 {
        return Ok(Some(candidates.into_iter().next().unwrap()));
    }
    let combined = RecordBatch::concat(&candidates.iter().collect::<Vec<_>>())?;
    record_batch_max_composite(&combined, composite_keys)
}

impl BlockingSink for RepartitionWithSentinelSink {
    type State = RayRepartitionWithSentinelState;

    #[instrument(skip_all, name = "RepartitionWithSentinelSink::sink")]
    fn sink(
        &self,
        input: MicroPartition,
        mut state: Self::State,
        _runtime_stats: Arc<Self::Stats>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        let repartition_spec = self.repartition_spec.clone();

        spawner
            .spawn(
                async move {
                    let partitioned = if let RepartitionSpec::Range(config) = &repartition_spec {
                        input.partition_by_range(
                            &config.by,
                            &config.boundaries,
                            &config.descending,
                        )?
                    } else {
                        unreachable!("RepartitionWithSentinelSink only supports Range repartition")
                    };

                    state.push(partitioned);
                    Ok(state)
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "RepartitionWithSentinelSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let num_partitions = self.num_partitions;
        let sentinel_sort_keys = self.sentinel_sort_keys.clone();

        let mut states = states;

        spawner
            .spawn(
                async move {
                    let mut repart_states = states.iter_mut().collect::<Vec<_>>();

                    let mut partition_futures = Vec::new();

                    for _ in 0..num_partitions {
                        let data = repart_states
                            .iter_mut()
                            .flat_map(|state| state.emit().unwrap())
                            .collect::<Vec<_>>();
                        let sentinel_sort_keys = sentinel_sort_keys.clone();
                        let fut = tokio::spawn(async move {
                            let together = MicroPartition::concat(data)?;
                            let sentinel = compute_partition_max(&together, &sentinel_sort_keys)?;
                            let schema = together.schema();
                            let concated = together.concat_or_get()?;
                            let mp = MicroPartition::new_loaded(
                                schema,
                                Arc::new(if let Some(t) = concated {
                                    vec![t]
                                } else {
                                    vec![]
                                }),
                                None,
                            );
                            Ok::<_, common_error::DaftError>((mp, sentinel))
                        });
                        partition_futures.push(fut);
                    }

                    let results = futures::future::try_join_all(partition_futures)
                        .await
                        .unwrap()
                        .into_iter()
                        .collect::<DaftResult<Vec<_>>>()?;

                    let (mut partitions, sentinel_rows): (Vec<_>, Vec<Option<RecordBatch>>) =
                        results.into_iter().unzip();

                    // Build sentinel table with exactly N rows (one per bucket).
                    let schema = partitions[0].schema();
                    // Empty buckets get a null row so the coordinator can index by position.
                    let null_row = RecordBatch::from_nonempty_columns(
                        schema
                            .fields()
                            .iter()
                            .map(|field| Series::full_null(&field.name, &field.dtype, 1))
                            .collect(),
                    )?;
                    let sentinel_batches: Vec<RecordBatch> = sentinel_rows
                        .into_iter()
                        .map(|row| row.unwrap_or_else(|| null_row.clone()))
                        .collect();
                    let sentinel_mp =
                        MicroPartition::new_loaded(schema, Arc::new(sentinel_batches), None);

                    // N range buckets + 1 sentinel partition = N+1 total
                    partitions.push(sentinel_mp);

                    Ok(BlockingSinkOutput::Partitions(partitions))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "RepartitionWithSentinel(Ray)".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Repartition
    }

    fn multiline_display(&self) -> Vec<String> {
        if let RepartitionSpec::Range(_) = &self.repartition_spec {
            vec![format!(
                "RepartitionWithSentinel: Range into {} partitions (+1 sentinel partition)",
                self.num_partitions
            )]
        } else {
            unreachable!("RepartitionWithSentinelSink only supports Range repartition")
        }
    }

    fn make_state(&self, _input_id: InputId) -> DaftResult<Self::State> {
        Ok(RayRepartitionWithSentinelState::new(self.num_partitions))
    }
}
