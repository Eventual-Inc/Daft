use std::{cmp::Ordering, collections::VecDeque, sync::Arc};

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::{
    array::ops::build_multi_array_compare,
    prelude::{SchemaRef, UInt64Array},
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::partitioning::RepartitionSpec;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkOutput, BlockingSinkSinkResult,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
    shuffle_metadata::{ShuffleMetadata, ShufflePartitionMetadata},
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

pub(crate) enum RepartitionWithSentinelState {
    Ray(RayRepartitionWithSentinelState),
}

enum RepartitionWithSentinelBackend {
    Ray {
        repartition_spec: RepartitionSpec,
        schema: SchemaRef,
    },
}

/// Find the row with the maximum value of `sort_keys` in `batch` using a linear scan.
/// Returns `None` if `batch` is empty.
fn record_batch_max(
    batch: &RecordBatch,
    sort_keys: &[BoundExpr],
) -> DaftResult<Option<RecordBatch>> {
    let len = batch.len();
    if len == 0 {
        return Ok(None);
    }
    let evaluated: Vec<_> = sort_keys
        .iter()
        .map(|k| batch.eval_expression(k))
        .collect::<DaftResult<_>>()?;
    let descending = vec![true; sort_keys.len()];
    let nulls_first = vec![false; sort_keys.len()];
    let cmp = build_multi_array_compare(&evaluated, &descending, &nulls_first)?;
    let max_idx = (1..len).fold(0usize, |best, i| {
        if cmp(i, best) == Ordering::Greater {
            i
        } else {
            best
        }
    });
    let idx = UInt64Array::from_vec("idx", vec![max_idx as u64]);
    Ok(Some(batch.take(&idx)?))
}

/// Find the row with the maximum value of `sort_keys` across all record batches in `partition`.
/// Returns `None` if the partition is empty.
fn compute_partition_max(
    partition: &MicroPartition,
    sort_keys: &[BoundExpr],
) -> DaftResult<Option<RecordBatch>> {
    let batches = partition.record_batches();
    let candidates: Vec<RecordBatch> = batches
        .iter()
        .filter_map(|b| record_batch_max(b, sort_keys).transpose())
        .collect::<DaftResult<_>>()?;
    if candidates.is_empty() {
        return Ok(None);
    }
    if candidates.len() == 1 {
        return Ok(Some(candidates.into_iter().next().unwrap()));
    }
    // Find the max among per-batch candidates (at most `batches.len()` rows).
    let combined = RecordBatch::concat(&candidates.iter().collect::<Vec<_>>())?;
    record_batch_max(&combined, sort_keys)
}

pub struct RepartitionWithSentinelSink {
    backend: RepartitionWithSentinelBackend,
    num_partitions: usize,
    sentinel_sort_keys: Vec<BoundExpr>,
}

impl RepartitionWithSentinelSink {
    pub fn new_ray(
        repartition_spec: RepartitionSpec,
        num_partitions: usize,
        schema: SchemaRef,
        sentinel_sort_keys: Vec<BoundExpr>,
    ) -> Self {
        Self {
            backend: RepartitionWithSentinelBackend::Ray {
                repartition_spec,
                schema,
            },
            num_partitions,
            sentinel_sort_keys,
        }
    }
}

impl BlockingSink for RepartitionWithSentinelSink {
    type State = RepartitionWithSentinelState;

    #[instrument(skip_all, name = "RepartitionWithSentinelSink::sink")]
    fn sink(
        &self,
        input: MicroPartition,
        state: Self::State,
        _runtime_stats: Arc<Self::Stats>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        match (&self.backend, state) {
            (
                RepartitionWithSentinelBackend::Ray {
                    repartition_spec, ..
                },
                RepartitionWithSentinelState::Ray(mut state),
            ) => {
                let repartition_spec = repartition_spec.clone();

                spawner
                    .spawn(
                        async move {
                            let partitioned = if let RepartitionSpec::Range(config) =
                                &repartition_spec
                            {
                                input.partition_by_range(
                                    &config.by,
                                    &config.boundaries,
                                    &config.descending,
                                )?
                            } else {
                                unreachable!(
                                    "RepartitionWithSentinelSink only supports Range repartition"
                                )
                            };

                            state.push(partitioned);
                            Ok(RepartitionWithSentinelState::Ray(state))
                        },
                        Span::current(),
                    )
                    .into()
            }
        }
    }

    #[instrument(skip_all, name = "RepartitionWithSentinelSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        match &self.backend {
            RepartitionWithSentinelBackend::Ray { schema, .. } => {
                let num_partitions = self.num_partitions;
                let schema = schema.clone();
                let sentinel_sort_keys = self.sentinel_sort_keys.clone();

                let mut states = states
                    .into_iter()
                    .map(|state| match state {
                        RepartitionWithSentinelState::Ray(state) => state,
                    })
                    .collect::<Vec<_>>();

                spawner
                    .spawn(
                        async move {
                            let mut repart_states = states.iter_mut().collect::<Vec<_>>();
                            let mut outputs = Vec::new();
                            for _ in 0..num_partitions {
                                let data = repart_states
                                    .iter_mut()
                                    .flat_map(|state| state.emit().unwrap())
                                    .collect::<Vec<_>>();
                                let schema = schema.clone();
                                let sentinel_sort_keys = sentinel_sort_keys.clone();
                                let fut = tokio::spawn(async move {
                                    let together = MicroPartition::concat(data)?;
                                    let sentinel =
                                        compute_partition_max(&together, &sentinel_sort_keys)?;
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
                                outputs.push(fut);
                            }
                            let results = futures::future::try_join_all(outputs)
                                .await
                                .unwrap()
                                .into_iter()
                                .collect::<DaftResult<Vec<_>>>()?;
                            let (partitions, sentinels): (Vec<_>, Vec<_>) =
                                results.into_iter().unzip();

                            #[cfg(feature = "python")]
                            {
                                use pyo3::{Python, types::PyAnyMethods};

                                let mut metadata = Vec::with_capacity(partitions.len());
                                Python::attach(|py| -> DaftResult<()> {
                                    let ray = py.import("ray")?;
                                    for partition in partitions {
                                        let py_partition =
                                            daft_micropartition::python::PyMicroPartition::from(
                                                partition.clone(),
                                            );
                                        let object_ref =
                                            ray.call_method1("put", (py_partition,))?.unbind();
                                        metadata.push(ShufflePartitionMetadata::with_object_ref(
                                            object_ref,
                                            partition.len(),
                                            partition.size_bytes(),
                                        ));
                                    }
                                    Ok(())
                                })?;
                                Ok(BlockingSinkOutput::ShuffleMetadata(ShuffleMetadata {
                                    partitions: metadata,
                                    sentinels: Some(sentinels),
                                }))
                            }
                            #[cfg(not(feature = "python"))]
                            {
                                unreachable!("RepartitionWithSentinelSink requires python feature")
                            }
                        },
                        Span::current(),
                    )
                    .into()
            }
        }
    }

    fn name(&self) -> NodeName {
        "RepartitionWithSentinel".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Repartition
    }

    fn multiline_display(&self) -> Vec<String> {
        match &self.backend {
            RepartitionWithSentinelBackend::Ray {
                repartition_spec, ..
            } => {
                if let RepartitionSpec::Range(config) = repartition_spec {
                    let pairs = config
                        .by
                        .iter()
                        .zip(config.descending.iter())
                        .map(|(sb, d)| {
                            format!("({}, {})", sb, if *d { "descending" } else { "ascending" })
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    vec![
                        format!(
                            "RepartitionWithSentinel: Range into {} partitions",
                            self.num_partitions
                        ),
                        format!("By: {:?}", pairs),
                    ]
                } else {
                    unreachable!("RepartitionWithSentinelSink only supports Range repartition")
                }
            }
        }
    }

    fn make_state(&self, _input_id: InputId) -> DaftResult<Self::State> {
        match &self.backend {
            RepartitionWithSentinelBackend::Ray { .. } => Ok(RepartitionWithSentinelState::Ray(
                RayRepartitionWithSentinelState::new(self.num_partitions),
            )),
        }
    }
}
