use std::{collections::VecDeque, sync::Arc};

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use common_runtime::get_io_runtime;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::partitioning::RepartitionSpec;
use daft_micropartition::MicroPartition;
use daft_partition_refs::FlightPartitionRef;
use daft_shuffles::{
    multi_partition_cache::{repartition_agg, write_partitions_one_shot},
    server::flight_server::ShuffleFlightServer,
};
use itertools::Itertools;
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkOutput, BlockingSinkSinkResult,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
};

pub(crate) struct RayRepartitionState {
    states: VecDeque<Vec<MicroPartition>>,
}

impl RayRepartitionState {
    fn push(&mut self, parts: Vec<MicroPartition>) {
        for (vec, part) in self.states.iter_mut().zip(parts) {
            vec.push(part);
        }
    }

    fn emit(&mut self) -> Option<Vec<MicroPartition>> {
        self.states.pop_front()
    }
}

pub(crate) struct FlightRepartitionState {
    /// Map task identifier; embedded in the per-partition `partition_ref_id` returned
    /// at finalize so downstream readers can find these bytes.
    input_id: InputId,
    /// Accumulator: raw, un-partitioned inputs handed to this state. We defer all
    /// partition_by_hash work to finalize so the map task partitions exactly once over
    /// its full input instead of once per sink call (~90 sink calls per map task at
    /// SF1000). Trades extra in-memory residency (one map task's worth of inputs) for
    /// a single partition pass.
    inputs: Vec<MicroPartition>,
}

pub(crate) enum RepartitionState {
    Ray(RayRepartitionState),
    Flight(FlightRepartitionState),
}

// TODO: unify shuffle backends in all local operations
enum RepartitionBackend {
    Ray,
    Flight {
        shuffle_id: u64,
        shuffle_dirs: Vec<String>,
        local_server: Arc<ShuffleFlightServer>,
        shuffle_address: String,
        schema: SchemaRef,
    },
}

impl RepartitionBackend {
    fn name(&self) -> &'static str {
        match &self {
            Self::Ray { .. } => "Ray",
            Self::Flight { .. } => "Flight",
        }
    }
}

pub struct RepartitionSink {
    backend: RepartitionBackend,
    repartition_spec: RepartitionSpec,
    num_partitions: usize,
}

impl RepartitionSink {
    pub fn new_ray(repartition_spec: RepartitionSpec, num_partitions: usize) -> Self {
        Self {
            backend: RepartitionBackend::Ray,
            repartition_spec,
            num_partitions,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_new_flight(
        num_partitions: usize,
        schema: SchemaRef,
        shuffle_id: u64,
        repartition_spec: RepartitionSpec,
        shuffle_dirs: Vec<String>,
        _compression: Option<String>, // TODO: re-introduce when one-shot writer supports it
        local_server: Arc<ShuffleFlightServer>,
        shuffle_address: String,
    ) -> DaftResult<Self> {
        Ok(Self {
            backend: RepartitionBackend::Flight {
                shuffle_id,
                shuffle_dirs,
                local_server,
                shuffle_address,
                schema,
            },
            repartition_spec,
            num_partitions,
        })
    }
}

impl BlockingSink for RepartitionSink {
    type State = RepartitionState;

    #[instrument(skip_all, name = "RepartitionSink::sink")]
    fn sink(
        &self,
        input: MicroPartition,
        mut state: Self::State,
        _runtime_stats: Arc<Self::Stats>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        let repartition_spec = self.repartition_spec.clone();
        let num_partitions = self.num_partitions;

        spawner
            .spawn(
                async move {
                    use std::sync::atomic::Ordering;
                    use std::time::Instant;
                    let t_sink = Instant::now();
                    let input_rows = input.len() as u64;
                    let input_bytes = input.size_bytes() as u64;
                    repartition_agg::SINK_INPUT_ROWS.fetch_add(input_rows, Ordering::Relaxed);
                    repartition_agg::SINK_INPUT_BYTES.fetch_add(input_bytes, Ordering::Relaxed);

                    match &mut state {
                        RepartitionState::Ray(s) => {
                            // Ray backend: keep the partition-on-every-sink-call behaviour;
                            // the downstream Ray emit expects N pre-partitioned columns.
                            let t_partition = Instant::now();
                            let partitioned =
                                partition_input(&input, &repartition_spec, num_partitions)?;
                            repartition_agg::PARTITION_CALLS.fetch_add(1, Ordering::Relaxed);
                            repartition_agg::PARTITION_US.fetch_add(
                                t_partition.elapsed().as_micros() as u64,
                                Ordering::Relaxed,
                            );
                            let t_push = Instant::now();
                            s.push(partitioned);
                            repartition_agg::PUSH_US.fetch_add(
                                t_push.elapsed().as_micros() as u64,
                                Ordering::Relaxed,
                            );
                        }
                        RepartitionState::Flight(s) => {
                            // Flight backend: append-only. partition + write happens once at
                            // finalize. One partition pass per map task replaces ~90.
                            s.inputs.push(input);
                        }
                    }

                    repartition_agg::SINK_CALLS.fetch_add(1, Ordering::Relaxed);
                    repartition_agg::SINK_US.fetch_add(
                        t_sink.elapsed().as_micros() as u64,
                        Ordering::Relaxed,
                    );
                    Ok(state)
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "RepartitionSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        match &self.backend {
            RepartitionBackend::Ray => {
                let num_partitions = self.num_partitions;

                let mut states = states
                    .into_iter()
                    .map(|state| match state {
                        RepartitionState::Ray(state) => state,
                        RepartitionState::Flight(_) => {
                            panic!("RepartitionSink state/backend mismatch")
                        }
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
                                let fut = tokio::spawn(async move {
                                    let together = MicroPartition::concat(data)?;
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
                                    Ok(mp)
                                });
                                outputs.push(fut);
                            }
                            let partitions = futures::future::try_join_all(outputs)
                                .await
                                .unwrap()
                                .into_iter()
                                .collect::<DaftResult<Vec<_>>>()?;
                            Ok(BlockingSinkOutput::Partitions(partitions))
                        },
                        Span::current(),
                    )
                    .into()
            }
            RepartitionBackend::Flight {
                shuffle_id,
                shuffle_dirs,
                local_server,
                shuffle_address,
                schema,
            } => {
                let shuffle_id = *shuffle_id;
                let shuffle_dirs = shuffle_dirs.clone();
                let local_server = local_server.clone();
                let shuffle_address = shuffle_address.clone();
                let schema = schema.clone();
                let repartition_spec = self.repartition_spec.clone();
                let num_partitions = self.num_partitions;

                // max_concurrency == 1, so this is always exactly one state.
                let state = match states.into_iter().next() {
                    Some(RepartitionState::Flight(s)) => s,
                    Some(RepartitionState::Ray(_)) => {
                        panic!("RepartitionSink state/backend mismatch")
                    }
                    None => {
                        return spawner
                            .spawn(
                                async move {
                                    Err(common_error::DaftError::InternalError(
                                        "Flight repartition finalize requires one state"
                                            .to_string(),
                                    ))
                                },
                                Span::current(),
                            )
                            .into();
                    }
                };

                spawner
                    .spawn(
                        async move {
                            use std::sync::atomic::Ordering;
                            use std::time::Instant;

                            let FlightRepartitionState { input_id, inputs } = state;

                            // Concat + partition: cheap metadata Vec → one partition pass.
                            let t_partition = Instant::now();
                            let merged = MicroPartition::concat(inputs)?;
                            let partitioned =
                                partition_input(&merged, &repartition_spec, num_partitions)?;
                            repartition_agg::PARTITION_CALLS.fetch_add(1, Ordering::Relaxed);
                            repartition_agg::PARTITION_US.fetch_add(
                                t_partition.elapsed().as_micros() as u64,
                                Ordering::Relaxed,
                            );

                            // Synchronous one-shot write — no channel, no coalesce buffer,
                            // no writer task spawn. Wrapped in spawn_blocking so the IPC
                            // encode CPU work runs on the io_runtime's blocking pool.
                            let t_close = Instant::now();
                            let schema_for_task = schema.clone();
                            let finalized = get_io_runtime(true)
                                .spawn_blocking(move || {
                                    write_partitions_one_shot(
                                        input_id,
                                        schema_for_task,
                                        &shuffle_dirs,
                                        shuffle_id,
                                        partitioned,
                                    )
                                })
                                .await??;
                            repartition_agg::FINALIZE_CLOSE_US.fetch_add(
                                t_close.elapsed().as_micros() as u64,
                                Ordering::Relaxed,
                            );
                            repartition_agg::FINALIZE_CALLS.fetch_add(1, Ordering::Relaxed);

                            local_server
                                .register_shuffle_partitions(shuffle_id, finalized.clone())
                                .await?;
                            Ok(BlockingSinkOutput::FlightPartitionRefs(
                                finalized
                                    .into_iter()
                                    .map(|partition| FlightPartitionRef {
                                        shuffle_id,
                                        server_address: shuffle_address.clone(),
                                        partition_ref_id: partition.partition_ref_id,
                                        num_rows: partition.num_rows,
                                        size_bytes: partition.size_bytes,
                                    })
                                    .collect(),
                            ))
                        },
                        Span::current(),
                    )
                    .into()
            }
        }
    }

    fn name(&self) -> NodeName {
        format!("Repartition({})", self.backend.name()).into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Repartition
    }

    fn multiline_display(&self) -> Vec<String> {
        let backend_name = self.backend.name();
        match &self.repartition_spec {
            RepartitionSpec::Hash(config) => vec![format!(
                "Repartition({backend_name}): By {} into {} partitions",
                config.by.iter().map(|e| e.to_string()).join(", "),
                self.num_partitions
            )],
            RepartitionSpec::Random(_) => vec![format!(
                "Repartition({backend_name}): Random into {} partitions",
                self.num_partitions
            )],
            RepartitionSpec::Range(_) => vec![format!(
                "Repartition({backend_name}): Range into {} partitions",
                self.num_partitions
            )],
        }
    }

    fn make_state(&self, input_id: InputId) -> DaftResult<Self::State> {
        match &self.backend {
            RepartitionBackend::Ray => Ok(RepartitionState::Ray(RayRepartitionState {
                states: (0..self.num_partitions).map(|_| vec![]).collect(),
            })),
            RepartitionBackend::Flight { .. } => {
                Ok(RepartitionState::Flight(FlightRepartitionState {
                    input_id,
                    inputs: Vec::new(),
                }))
            }
        }
    }

    fn max_concurrency(&self) -> usize {
        // One state per input_id. Parallelism comes from concurrent input_ids,
        // not concurrent workers per input_id. Keeps the accumulate-then-finalize
        // model simple — no need to merge state across workers.
        1
    }
}

fn partition_input(
    input: &MicroPartition,
    repartition_spec: &RepartitionSpec,
    num_partitions: usize,
) -> DaftResult<Vec<MicroPartition>> {
    match repartition_spec {
        RepartitionSpec::Hash(config) => {
            let bound_exprs = BoundExpr::bind_all(&config.by, &input.schema())?;
            input.partition_by_hash(&bound_exprs, num_partitions)
        }
        RepartitionSpec::Random(config) => {
            input.partition_by_random(num_partitions, config.seed.unwrap_or(0))
        }
        RepartitionSpec::Range(config) => {
            input.partition_by_range(&config.by, &config.boundaries, &config.descending)
        }
    }
}
