use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::partitioning::RepartitionSpec;
use daft_micropartition::MicroPartition;
use daft_partition_refs::FlightPartitionRef;
use daft_shuffles::{
    multi_partition_cache::MultiPartitionShuffleCache, server::flight_server::ShuffleFlightServer,
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
    /// One combined-file cache per map task (input_id). Holds all N output partitions
    /// inside a single IPC file with an in-memory byte-range index.
    cache: Arc<MultiPartitionShuffleCache>,
}

impl FlightRepartitionState {
    async fn push(&self, parts: Vec<MicroPartition>) -> DaftResult<()> {
        let push_futures = parts
            .into_iter()
            .enumerate()
            .map(|(partition_idx, mp)| self.cache.push_partition_data(partition_idx, mp));
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
        local_server: Arc<ShuffleFlightServer>,
        shuffle_address: String,
        schema: SchemaRef,
        // Only accessed from the single-threaded event loop; Mutex is just for Sync.
        // One MultiPartitionShuffleCache per map task (keyed by InputId). All N output
        // partitions for that map task live in one IPC file with byte-range metadata.
        caches: Mutex<HashMap<InputId, Arc<MultiPartitionShuffleCache>>>,
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
        _compression: Option<String>, // TODO: re-introduce when MultiPartitionShuffleCache supports it
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
                caches: Mutex::new(HashMap::new()),
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
                            // De-dup caches across states: per InputId, all worker states share the
                            // same Arc<MultiPartitionShuffleCache> (memoized in make_state). Close
                            // each unique cache once; closing returns one PartitionCache per
                            // output partition, all referencing the same physical file with
                            // disjoint byte ranges.
                            let mut unique: HashMap<usize, Arc<MultiPartitionShuffleCache>> =
                                HashMap::new();
                            for state in states {
                                let ptr = Arc::as_ptr(&state.cache) as usize;
                                unique.entry(ptr).or_insert(state.cache);
                            }
                            if unique.is_empty() {
                                return Err(common_error::DaftError::InternalError(
                                    "Flight repartition finalize requires at least one state"
                                        .to_string(),
                                ));
                            }
                            let close_futures =
                                unique.into_values().map(|cache| async move { cache.close().await });
                            let per_cache: Vec<Vec<_>> =
                                futures::future::try_join_all(close_futures).await?;
                            let finalized: Vec<_> =
                                per_cache.into_iter().flatten().collect();
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
                schema,
                caches,
                ..
            } => {
                let mut caches = caches.lock().unwrap();
                let cache = match caches.entry(input_id) {
                    std::collections::hash_map::Entry::Occupied(e) => e.get().clone(),
                    std::collections::hash_map::Entry::Vacant(e) => {
                        let cache = Arc::new(MultiPartitionShuffleCache::try_new(
                            input_id,
                            self.num_partitions,
                            schema.clone(),
                            shuffle_dirs,
                            *shuffle_id,
                        )?);
                        e.insert(cache.clone());
                        cache
                    }
                };
                Ok(RepartitionState::Flight(FlightRepartitionState { cache }))
            }
        }
    }
}
