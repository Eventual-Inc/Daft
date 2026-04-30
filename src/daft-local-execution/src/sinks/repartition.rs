use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

use daft_common::error::DaftResult;
use daft_common::metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::partitioning::RepartitionSpec;
use daft_micropartition::MicroPartition;
use daft_local_plan::partition_refs::FlightPartitionRef;
use crate::shuffles::{
    server::flight_server::ShuffleFlightServer,
    shuffle_cache::{InProgressShuffleCache, partition_ref_id},
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
    partitions: Arc<Vec<Arc<InProgressShuffleCache>>>,
}

impl FlightRepartitionState {
    async fn push(&self, parts: Vec<MicroPartition>) -> DaftResult<()> {
        let push_futures = self
            .partitions
            .iter()
            .zip(parts)
            .map(|(cache, partition)| cache.push_partition_data(partition));
        futures::future::try_join_all(push_futures).await?;
        Ok(())
    }
}

pub(crate) enum RepartitionState {
    Ray(RayRepartitionState),
    Flight(FlightRepartitionState),
}

impl RepartitionState {
    async fn push(&mut self, parts: Vec<MicroPartition>) -> DaftResult<()> {
        match self {
            Self::Ray(state) => {
                state.push(parts);
                Ok(())
            }
            Self::Flight(state) => state.push(parts).await,
        }
    }
}

// TODO: unify shuffle backends in all local operations
enum RepartitionBackend {
    Ray,
    Flight {
        shuffle_id: u64,
        shuffle_dirs: Vec<String>,
        compression: Option<String>,
        local_server: Arc<ShuffleFlightServer>,
        shuffle_address: String,
        target_in_memory_size_per_partition: usize,
        schema: SchemaRef,
        // Only accessed from the single-threaded event loop; Mutex is just for Sync.
        partitions: Mutex<HashMap<InputId, Arc<Vec<Arc<InProgressShuffleCache>>>>>,
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
        compression: Option<String>,
        local_server: Arc<ShuffleFlightServer>,
        shuffle_address: String,
    ) -> DaftResult<Self> {
        const TARGET_TOTAL_IN_MEMORY_SIZE_BYTES: usize = 1024 * 1024 * 2000;
        Ok(Self {
            backend: RepartitionBackend::Flight {
                shuffle_id,
                shuffle_dirs,
                compression,
                local_server,
                shuffle_address,
                target_in_memory_size_per_partition: (TARGET_TOTAL_IN_MEMORY_SIZE_BYTES
                    / num_partitions)
                    .clamp(1024 * 1024 * 8, 1024 * 1024 * 128),
                schema,
                partitions: Mutex::new(HashMap::new()),
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
                    let partitioned = match repartition_spec {
                        RepartitionSpec::Hash(config) => {
                            let bound_exprs = BoundExpr::bind_all(&config.by, &input.schema())?;
                            input.partition_by_hash(&bound_exprs, num_partitions)?
                        }
                        RepartitionSpec::Random(config) => {
                            input.partition_by_random(num_partitions, config.seed.unwrap_or(0))?
                        }
                        RepartitionSpec::Range(config) => input.partition_by_range(
                            &config.by,
                            &config.boundaries,
                            &config.descending,
                        )?,
                    };

                    state.push(partitioned).await?;
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
                local_server,
                shuffle_address,
                ..
            } => {
                let shuffle_id = *shuffle_id;
                let local_server = local_server.clone();
                let states = states
                    .into_iter()
                    .map(|state| match state {
                        RepartitionState::Flight(state) => state,
                        RepartitionState::Ray(_) => {
                            panic!("RepartitionSink state/backend mismatch")
                        }
                    })
                    .collect::<Vec<_>>();

                let shuffle_address = shuffle_address.clone();
                spawner
                    .spawn(
                        async move {
                            let partitions = states
                                .into_iter()
                                .next()
                                .expect("Flight repartition finalize requires at least one state")
                                .partitions;
                            let finalized = futures::future::try_join_all(
                                partitions.iter().map(|partition| partition.close()),
                            )
                            .await?;
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
            RepartitionBackend::Flight {
                shuffle_dirs,
                shuffle_id,
                target_in_memory_size_per_partition,
                compression,
                schema,
                partitions,
                ..
            } => {
                let mut partitions = partitions.lock().unwrap();
                let partition_set = match partitions.entry(input_id) {
                    std::collections::hash_map::Entry::Occupied(e) => e.get().clone(),
                    std::collections::hash_map::Entry::Vacant(e) => {
                        let partition_set = Arc::new(
                            (0..self.num_partitions)
                                .map(|partition_idx| {
                                    Ok(Arc::new(InProgressShuffleCache::try_new(
                                        partition_ref_id(input_id, partition_idx),
                                        schema.clone(),
                                        shuffle_dirs,
                                        *shuffle_id,
                                        *target_in_memory_size_per_partition,
                                        compression.as_deref(),
                                    )?))
                                })
                                .collect::<DaftResult<Vec<_>>>()?,
                        );
                        e.insert(partition_set.clone());
                        partition_set
                    }
                };
                Ok(RepartitionState::Flight(FlightRepartitionState {
                    partitions: partition_set,
                }))
            }
        }
    }
}
