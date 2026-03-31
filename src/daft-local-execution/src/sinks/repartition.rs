use std::{collections::VecDeque, sync::Arc};

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::partitioning::RepartitionSpec;
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkOutput, BlockingSinkSinkResult,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
    shuffle_metadata::{ShuffleMetadata, ShufflePartitionMetadata},
};

pub(crate) struct RepartitionState {
    states: VecDeque<Vec<MicroPartition>>,
}

impl RepartitionState {
    fn push(&mut self, parts: Vec<MicroPartition>) {
        for (vec, part) in self.states.iter_mut().zip(parts) {
            vec.push(part);
        }
    }

    fn emit(&mut self) -> Option<Vec<MicroPartition>> {
        self.states.pop_front()
    }
}

pub struct RepartitionSink {
    repartition_spec: RepartitionSpec,
    num_partitions: usize,
    schema: SchemaRef,
}

impl RepartitionSink {
    pub fn new(
        repartition_spec: RepartitionSpec,
        num_partitions: usize,
        schema: SchemaRef,
    ) -> Self {
        Self {
            repartition_spec,
            num_partitions,
            schema,
        }
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
        let schema = self.schema.clone();
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
                        RepartitionSpec::Random(config) => {
                            // TODO: Should default seed be 0?
                            input.partition_by_random(num_partitions, config.seed.unwrap_or(0))?
                        }
                        RepartitionSpec::Range(config) => input.partition_by_range(
                            &config.by,
                            &config.boundaries,
                            &config.descending,
                        )?,
                    };
                    state.push(partitioned);
                    Ok(state)
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "RepartitionSink::finalize")]
    fn finalize(
        &self,
        mut states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let num_partitions = self.num_partitions;
        let schema = self.schema.clone();

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

                        let mut metadata = Vec::with_capacity(partitions.len());
                        Python::attach(|py| -> DaftResult<()> {
                            let ray = py.import("ray")?;
                            for partition in partitions {
                                let py_partition =
                                    daft_micropartition::python::PyMicroPartition::from(
                                        partition.clone(),
                                    );
                                let object_ref = ray.call_method1("put", (py_partition,))?.unbind();
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
                        }))
                    }
                    #[cfg(not(feature = "python"))]
                    {
                        unreachable!("RepartitionShuffleWriteSink requires python feature")
                    }
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "Repartition".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Repartition
    }

    fn multiline_display(&self) -> Vec<String> {
        match &self.repartition_spec {
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
        }
    }

    fn make_state(&self, _input_id: InputId) -> DaftResult<Self::State> {
        Ok(RepartitionState {
            states: (0..self.num_partitions).map(|_| vec![]).collect(),
        })
    }
}
