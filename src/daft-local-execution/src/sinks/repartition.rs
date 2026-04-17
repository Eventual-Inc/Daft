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
use daft_partition_refs::{PyFlightPartitionRef, RayPartitionRef};
use daft_shuffles::{
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
    run::ShufflePartitionRefs,
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

pub(crate) enum RepartitionState {
    Ray(RayRepartitionState),
    Flight(FlightRepartitionState),
}

enum RepartitionBackend {
    Ray {
        repartition_spec: RepartitionSpec,
        schema: SchemaRef,
    },
    Flight {
        num_partitions: usize,
        shuffle_id: u64,
        repartition_spec: RepartitionSpec,
        shuffle_dirs: Vec<String>,
        compression: Option<String>,
        local_server: Arc<ShuffleFlightServer>,
        shuffle_address: String,
        target_in_memory_size_per_partition: usize,
        // Only accessed from the single-threaded event loop; Mutex is just for Sync.
        partitions: Mutex<HashMap<InputId, Arc<Vec<Arc<InProgressShuffleCache>>>>>,
    },
}

pub struct RepartitionSink {
    backend: RepartitionBackend,
    num_partitions: usize,
}

impl RepartitionSink {
    pub fn new_ray(
        repartition_spec: RepartitionSpec,
        num_partitions: usize,
        schema: SchemaRef,
    ) -> Self {
        Self {
            backend: RepartitionBackend::Ray {
                repartition_spec,
                schema,
            },
            num_partitions,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_new_flight(
        num_partitions: usize,
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
                num_partitions,
                shuffle_id,
                repartition_spec,
                shuffle_dirs,
                compression,
                local_server,
                shuffle_address,
                target_in_memory_size_per_partition: (TARGET_TOTAL_IN_MEMORY_SIZE_BYTES
                    / num_partitions)
                    .clamp(1024 * 1024 * 8, 1024 * 1024 * 128),
                partitions: Mutex::new(HashMap::new()),
            },
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
        state: Self::State,
        _runtime_stats: Arc<Self::Stats>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        match (&self.backend, state) {
            (
                RepartitionBackend::Ray {
                    repartition_spec,
                    schema,
                },
                RepartitionState::Ray(mut state),
            ) => {
                let repartition_spec = repartition_spec.clone();
                let num_partitions = self.num_partitions;
                let schema = schema.clone();
                spawner
                    .spawn(
                        async move {
                            let partitioned = match repartition_spec {
                                RepartitionSpec::Hash(config) => {
                                    let bound_exprs = config
                                        .by
                                        .iter()
                                        .map(|e| BoundExpr::try_new(e.clone(), &schema))
                                        .collect::<DaftResult<Vec<_>>>()?;
                                    input.partition_by_hash(&bound_exprs, num_partitions)?
                                }
                                RepartitionSpec::Random(config) => input.partition_by_random(
                                    num_partitions,
                                    config.seed.unwrap_or(0),
                                )?,
                                RepartitionSpec::Range(config) => input.partition_by_range(
                                    &config.by,
                                    &config.boundaries,
                                    &config.descending,
                                )?,
                            };
                            state.push(partitioned);
                            Ok(RepartitionState::Ray(state))
                        },
                        Span::current(),
                    )
                    .into()
            }
            (
                RepartitionBackend::Flight {
                    repartition_spec,
                    num_partitions,
                    ..
                },
                RepartitionState::Flight(state),
            ) => {
                let num_partitions = *num_partitions;
                let partition_by = match repartition_spec {
                    RepartitionSpec::Hash(config) => Some(config.by.clone()),
                    RepartitionSpec::Random(_) => None,
                    RepartitionSpec::Range(_) => {
                        unreachable!("Range repartition is not supported for flight shuffle")
                    }
                };

                spawner
                    .spawn(
                        async move {
                            let partitioned = match &partition_by {
                                Some(partition_by) => {
                                    let partition_by =
                                        BoundExpr::bind_all(partition_by, &input.schema())?;
                                    input.partition_by_hash(&partition_by, num_partitions)?
                                }
                                None => input.partition_by_random(num_partitions, 0)?,
                            };
                            let mut push_futures = Vec::new();
                            for (cache, partition) in
                                state.partitions.iter().zip(partitioned.into_iter())
                            {
                                push_futures.push(cache.push_partition_data(partition));
                            }
                            futures::future::try_join_all(push_futures).await?;
                            Ok(RepartitionState::Flight(state))
                        },
                        Span::current(),
                    )
                    .into()
            }
            _ => panic!("RepartitionSink state/backend mismatch"),
        }
    }

    #[instrument(skip_all, name = "RepartitionSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        match &self.backend {
            RepartitionBackend::Ray { schema, .. } => {
                let num_partitions = self.num_partitions;
                let schema = schema.clone();

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
                                let schema = schema.clone();
                                let fut = tokio::spawn(async move {
                                    let together = MicroPartition::concat(data)?;
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
                            #[cfg(feature = "python")]
                            {
                                use pyo3::{Python, types::PyAnyMethods};

                                let mut partition_refs = Vec::with_capacity(partitions.len());
                                Python::attach(|py| -> DaftResult<()> {
                                    let ray = py.import("ray")?;
                                    for partition in partitions {
                                        let py_partition =
                                            daft_micropartition::python::PyMicroPartition::from(
                                                partition.clone(),
                                            );
                                        let object_ref =
                                            ray.call_method1("put", (py_partition,))?.unbind();
                                        partition_refs.push(RayPartitionRef::new(
                                            object_ref,
                                            partition.len(),
                                            partition.size_bytes(),
                                        ));
                                    }
                                    Ok(())
                                })?;
                                Ok(BlockingSinkOutput::ShuffleMetadata(
                                    ShufflePartitionRefs::Ray(partition_refs),
                                ))
                            }
                            #[cfg(not(feature = "python"))]
                            {
                                unreachable!("RepartitionSink requires python feature")
                            }
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
                            Ok(BlockingSinkOutput::ShuffleMetadata(
                                ShufflePartitionRefs::Flight(
                                    finalized
                                        .into_iter()
                                        .map(|partition| {
                                            PyFlightPartitionRef::new(
                                                shuffle_id,
                                                shuffle_address.clone(),
                                                partition.partition_ref_id,
                                                partition.num_rows,
                                                partition.size_bytes,
                                            )
                                        })
                                        .collect(),
                                ),
                            ))
                        },
                        Span::current(),
                    )
                    .into()
            }
        }
    }

    fn name(&self) -> NodeName {
        "Repartition".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Repartition
    }

    fn multiline_display(&self) -> Vec<String> {
        match &self.backend {
            RepartitionBackend::Ray {
                repartition_spec, ..
            } => match repartition_spec {
                RepartitionSpec::Hash(config) => vec![format!(
                    "Repartition: By {} into {} partitions",
                    config.by.iter().map(|e| e.to_string()).join(", "),
                    self.num_partitions
                )],
                RepartitionSpec::Random(_) => vec![format!(
                    "Repartition: Random into {} partitions",
                    self.num_partitions
                )],
                RepartitionSpec::Range(config) => {
                    let pairs = config
                        .by
                        .iter()
                        .zip(config.descending.iter())
                        .map(|(sb, d)| {
                            format!("({}, {})", sb, if *d { "descending" } else { "ascending" })
                        })
                        .join(", ");
                    vec![
                        format!("Repartition: Range into {} partitions", self.num_partitions),
                        format!("By: {:?}", pairs),
                    ]
                }
            },
            RepartitionBackend::Flight {
                repartition_spec, ..
            } => match repartition_spec {
                RepartitionSpec::Hash(config) => vec![format!(
                    "Repartition(Flight): By {} into {} partitions",
                    config.by.iter().map(|e| e.to_string()).join(", "),
                    self.num_partitions
                )],
                RepartitionSpec::Random(_) => vec![format!(
                    "Repartition(Flight): Random into {} partitions",
                    self.num_partitions
                )],
                RepartitionSpec::Range(_) => {
                    unreachable!("Range repartition is not supported for flight shuffle")
                }
            },
        }
    }

    fn make_state(&self, input_id: InputId) -> DaftResult<Self::State> {
        match &self.backend {
            RepartitionBackend::Ray { .. } => Ok(RepartitionState::Ray(RayRepartitionState {
                states: (0..self.num_partitions).map(|_| vec![]).collect(),
            })),
            RepartitionBackend::Flight {
                num_partitions,
                shuffle_dirs,
                shuffle_id,
                target_in_memory_size_per_partition,
                compression,
                partitions,
                ..
            } => {
                let mut partitions = partitions.lock().unwrap();
                let partition_set = match partitions.entry(input_id) {
                    std::collections::hash_map::Entry::Occupied(e) => e.get().clone(),
                    std::collections::hash_map::Entry::Vacant(e) => {
                        let partition_set = Arc::new(
                            (0..*num_partitions)
                                .map(|partition_idx| {
                                    Ok(Arc::new(InProgressShuffleCache::try_new(
                                        partition_ref_id(input_id, partition_idx),
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
