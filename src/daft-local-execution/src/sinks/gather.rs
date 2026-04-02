use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_micropartition::MicroPartition;
use daft_shuffles::{
    server::flight_server::ShuffleFlightServer, shuffle_cache::InProgressShuffleCache,
};
#[cfg(feature = "python")]
use pyo3::types::PyAnyMethods;
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkOutput, BlockingSinkSinkResult,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
    shuffle_metadata::{ShuffleMetadata, ShufflePartitionMetadata},
};

pub(crate) struct RayGatherState {
    partitions: Vec<MicroPartition>,
}

pub(crate) struct FlightGatherState {
    cache: Arc<InProgressShuffleCache>,
}

pub(crate) enum GatherState {
    Ray(RayGatherState),
    Flight(FlightGatherState),
}

enum GatherBackend {
    Ray {
        schema: SchemaRef,
    },
    Flight {
        shuffle_id: u64,
        local_server: Arc<ShuffleFlightServer>,
        shuffle_dirs: Vec<String>,
        compression: Option<String>,
        target_in_memory_size_per_partition: usize,
        caches: Mutex<HashMap<InputId, Arc<InProgressShuffleCache>>>,
    },
}

pub struct GatherSink {
    backend: GatherBackend,
}

impl GatherSink {
    pub fn new_ray(schema: SchemaRef) -> Self {
        Self {
            backend: GatherBackend::Ray { schema },
        }
    }

    pub fn try_new_flight(
        shuffle_id: u64,
        shuffle_dirs: Vec<String>,
        compression: Option<String>,
        local_server: Arc<ShuffleFlightServer>,
    ) -> DaftResult<Self> {
        const TARGET_TOTAL_IN_MEMORY_SIZE_BYTES: usize = 1024 * 1024 * 2000;
        Ok(Self {
            backend: GatherBackend::Flight {
                shuffle_id,
                local_server,
                shuffle_dirs,
                compression,
                target_in_memory_size_per_partition: TARGET_TOTAL_IN_MEMORY_SIZE_BYTES
                    .clamp(1024 * 1024 * 8, 1024 * 1024 * 128),
                caches: Mutex::new(HashMap::new()),
            },
        })
    }
}

impl BlockingSink for GatherSink {
    type State = GatherState;

    #[instrument(skip_all, name = "GatherSink::sink")]
    fn sink(
        &self,
        input: MicroPartition,
        state: Self::State,
        _runtime_stats: Arc<Self::Stats>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        match (&self.backend, state) {
            (GatherBackend::Ray { .. }, GatherState::Ray(mut state)) => spawner
                .spawn(
                    async move {
                        state.partitions.push(input);
                        Ok(GatherState::Ray(state))
                    },
                    Span::current(),
                )
                .into(),
            (GatherBackend::Flight { .. }, GatherState::Flight(state)) => spawner
                .spawn(
                    async move {
                        state.cache.push_partitioned_data(vec![input]).await?;
                        Ok(GatherState::Flight(state))
                    },
                    Span::current(),
                )
                .into(),
            _ => panic!("GatherSink state/backend mismatch"),
        }
    }

    #[instrument(skip_all, name = "GatherSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        match &self.backend {
            GatherBackend::Ray { schema } => {
                let schema = schema.clone();
                let states = states
                    .into_iter()
                    .map(|state| match state {
                        GatherState::Ray(state) => state,
                        GatherState::Flight(_) => panic!("GatherSink state/backend mismatch"),
                    })
                    .collect::<Vec<_>>();

                spawner
                    .spawn(
                        async move {
                            let parts = states
                                .into_iter()
                                .flat_map(|state| state.partitions)
                                .collect::<Vec<_>>();
                            let together = MicroPartition::concat(parts)?;
                            let concated = together.concat_or_get()?;
                            let partition = MicroPartition::new_loaded(
                                schema.clone(),
                                Arc::new(if let Some(t) = concated {
                                    vec![t]
                                } else {
                                    vec![]
                                }),
                                None,
                            );
                            #[cfg(feature = "python")]
                            {
                                use pyo3::Python;

                                let metadata = Python::attach(|py| -> DaftResult<_> {
                                    let ray = py.import("ray")?;
                                    let py_partition =
                                        daft_micropartition::python::PyMicroPartition::from(
                                            partition.clone(),
                                        );
                                    let object_ref =
                                        ray.call_method1("put", (py_partition,))?.unbind();
                                    Ok(vec![ShufflePartitionMetadata::with_object_ref(
                                        object_ref,
                                        partition.len(),
                                        partition.size_bytes(),
                                    )])
                                })?;
                                Ok(BlockingSinkOutput::ShuffleMetadata(ShuffleMetadata {
                                    partitions: metadata,
                                }))
                            }
                            #[cfg(not(feature = "python"))]
                            {
                                unreachable!("GatherSink requires python feature")
                            }
                        },
                        Span::current(),
                    )
                    .into()
            }
            GatherBackend::Flight {
                shuffle_id,
                local_server,
                ..
            } => {
                let shuffle_id = *shuffle_id;
                let local_server = local_server.clone();
                let states = states
                    .into_iter()
                    .map(|state| match state {
                        GatherState::Flight(state) => state,
                        GatherState::Ray(_) => panic!("GatherSink state/backend mismatch"),
                    })
                    .collect::<Vec<_>>();

                spawner
                    .spawn(
                        async move {
                            let cache = states.into_iter().next().unwrap().cache;
                            let finalized = cache.close().await?;
                            let num_rows = finalized
                                .rows_per_partition()
                                .into_iter()
                                .next()
                                .unwrap_or(0);
                            let size_bytes = finalized
                                .bytes_per_partition()
                                .into_iter()
                                .next()
                                .unwrap_or(0);
                            local_server
                                .register_shuffle_cache(shuffle_id, finalized.into())
                                .await?;
                            Ok(BlockingSinkOutput::ShuffleMetadata(ShuffleMetadata {
                                partitions: vec![ShufflePartitionMetadata::new(
                                    num_rows, size_bytes,
                                )],
                            }))
                        },
                        Span::current(),
                    )
                    .into()
            }
        }
    }

    fn name(&self) -> NodeName {
        "Gather".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Repartition
    }

    fn multiline_display(&self) -> Vec<String> {
        match &self.backend {
            GatherBackend::Ray { .. } => vec!["Gather".to_string()],
            GatherBackend::Flight { .. } => vec!["Gather(Flight)".to_string()],
        }
    }

    fn make_state(&self, input_id: InputId) -> DaftResult<Self::State> {
        match &self.backend {
            GatherBackend::Ray { .. } => Ok(GatherState::Ray(RayGatherState {
                partitions: Vec::new(),
            })),
            GatherBackend::Flight {
                shuffle_dirs,
                shuffle_id,
                target_in_memory_size_per_partition,
                compression,
                caches,
                ..
            } => {
                let mut caches = caches.lock().unwrap();
                let cache = match caches.entry(input_id) {
                    std::collections::hash_map::Entry::Occupied(e) => e.get().clone(),
                    std::collections::hash_map::Entry::Vacant(e) => {
                        let cache = Arc::new(InProgressShuffleCache::try_new(
                            1,
                            shuffle_dirs,
                            input_id.to_string(),
                            *shuffle_id,
                            *target_in_memory_size_per_partition,
                            compression.as_deref(),
                        )?);
                        e.insert(cache.clone());
                        cache
                    }
                };
                Ok(GatherState::Flight(FlightGatherState { cache }))
            }
        }
    }
}
