use std::{collections::VecDeque, sync::Arc};

use common_error::DaftResult;
use common_runtime::get_compute_pool_num_threads;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_io::IOStatsContext;
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{instrument, Span};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult, BlockingSinkState,
    BlockingSinkStatus,
};
use crate::{sinks::blocking_sink::BlockingSinkFinalizeOutput, ExecutionTaskSpawner};

struct RepartitionState {
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

impl BlockingSinkState for RepartitionState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct RepartitionSink {
    columns: Arc<Vec<BoundExpr>>,
    num_partitions: usize,
    schema: SchemaRef,
}

impl RepartitionSink {
    pub fn new(columns: Vec<BoundExpr>, num_partitions: usize, schema: SchemaRef) -> Self {
        Self {
            columns: Arc::new(columns),
            num_partitions,
            schema,
        }
    }
}

impl BlockingSink for RepartitionSink {
    #[instrument(skip_all, name = "RepartitionSink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn BlockingSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult {
        let columns = self.columns.clone();
        let num_partitions = self.num_partitions;
        spawner
            .spawn(
                async move {
                    let repartition_state = state
                        .as_any_mut()
                        .downcast_mut::<RepartitionState>()
                        .expect("RepartitionSink should have RepartitionState");
                    let partitioned = input.partition_by_hash(&columns, num_partitions)?;
                    repartition_state.push(partitioned);
                    Ok(BlockingSinkStatus::NeedMoreInput(state))
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "RepartitionSink::finalize")]
    fn finalize(
        &self,
        mut states: Vec<Box<dyn BlockingSinkState>>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let num_partitions = self.num_partitions;
        let schema = self.schema.clone();

        spawner
            .spawn(
                async move {
                    let mut repart_states = states
                        .iter_mut()
                        .map(|state| {
                            let repart_state = state
                                .as_any_mut()
                                .downcast_mut::<RepartitionState>()
                                .expect("RepartitionSink should have RepartitionState");

                            repart_state
                        })
                        .collect::<Vec<_>>();

                    let mut outputs = Vec::new();
                    for _ in 0..num_partitions {
                        let data = repart_states
                            .iter_mut()
                            .flat_map(|state| state.emit().unwrap())
                            .collect::<Vec<_>>();
                        let schema = schema.clone();
                        let fut = tokio::spawn(async move {
                            let together = MicroPartition::concat(&data)?;
                            let concated =
                                together.concat_or_get(IOStatsContext::new("get tables"))?;
                            let mp = MicroPartition::new_loaded(schema, concated, None);
                            Ok(Arc::new(mp))
                        });
                        outputs.push(fut);
                    }
                    let outputs = futures::future::try_join_all(outputs)
                        .await
                        .unwrap()
                        .into_iter()
                        .collect::<DaftResult<Vec<_>>>()?;
                    Ok(BlockingSinkFinalizeOutput::Finished(outputs))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> &'static str {
        "Repartition"
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![format!(
            "Repartition: By {} into {} partitions",
            self.columns.iter().map(|e| e.to_string()).join(", "),
            self.num_partitions
        )]
    }

    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(RepartitionState {
            states: (0..self.num_partitions).map(|_| vec![]).collect(),
        }))
    }
}
