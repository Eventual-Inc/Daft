use std::{sync::Arc, time::Instant};

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkOutput, BlockingSinkSinkResult,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
};

/// Parameters for the TopN that both the state and sinker need
struct TopNParams {
    // Sort By Parameters
    sort_by: Vec<BoundExpr>,
    descending: Vec<bool>,
    nulls_first: Vec<bool>,
    // Limit Parameters
    limit: usize,
    offset: Option<usize>,
    // Output Schema
    schema: SchemaRef,
}

/// Current status of the TopN operation
pub(crate) enum TopNState {
    /// Operator is still collecting input
    Building(Vec<MicroPartition>),
    /// Operator has finished collecting all input and ready to produce output
    /// by doing a final sort + limit
    Done,
}

impl TopNState {
    /// Process a new micro-partition and update the top N values
    fn append(&mut self, part: MicroPartition) {
        let Self::Building(top_values) = self else {
            panic!("TopNSink should be in Building state");
        };

        top_values.push(part);
    }

    /// Finalize the TopN operation and return the top N values
    fn finalize(&mut self) -> Vec<MicroPartition> {
        let Self::Building(top_values) = self else {
            panic!("TopNSink should be in Building state");
        };

        let top_values = std::mem::take(top_values);
        *self = Self::Done;
        top_values
    }
}

pub struct TopNSink {
    params: Arc<TopNParams>,
}

impl TopNSink {
    pub fn new(
        sort_by: Vec<BoundExpr>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
        limit: usize,
        offset: Option<usize>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            params: Arc::new(TopNParams {
                sort_by,
                descending,
                nulls_first,
                limit,
                offset,
                schema,
            }),
        }
    }
}

impl BlockingSink for TopNSink {
    type State = TopNState;

    #[instrument(skip_all, name = "TopNSink::sink")]
    fn sink(
        &self,
        input: MicroPartition,
        mut state: Self::State,
        _runtime_stats: Arc<Self::Stats>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        let params = self.params.clone();

        spawner
            .spawn(
                async move {
                    let start = Instant::now();
                    // Find the top N values in the input micro-partition
                    let limit = params.limit + params.offset.unwrap_or(0);
                    let top_input_rows = input.top_n(
                        &params.sort_by,
                        &params.descending,
                        &params.nulls_first,
                        limit,
                        Some(0),
                    )?;

                    // Append to the collection of existing top N values
                    state.append(top_input_rows);
                    let time = start.elapsed();
                    eprintln!("time: {:?}", time);
                    Ok(state)
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "TopNSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let params = self.params.clone();
        spawner
            .spawn(
                async move {
                    let start = Instant::now();
                    let mut joinset = tokio::task::JoinSet::new();
                    for mut state in states {
                        let params = params.clone();
                        joinset.spawn(async move {
                            let parts = state.finalize();
                            let concated =
                                MicroPartition::concat_or_empty(parts, params.schema.clone())?;
                            let final_output = concated.top_n(
                                &params.sort_by,
                                &params.descending,
                                &params.nulls_first,
                                params.limit,
                                Some(0),
                            )?;
                            Ok(final_output)
                        });
                    }

                    let parts = joinset
                        .join_all()
                        .await
                        .into_iter()
                        .collect::<DaftResult<Vec<_>>>()?;
                    let concated = MicroPartition::concat(parts)?;
                    let final_output = concated.top_n(
                        &params.sort_by,
                        &params.descending,
                        &params.nulls_first,
                        params.limit,
                        params.offset,
                    )?;
                    let final_time = start.elapsed();
                    eprintln!("final_time: {:?}", final_time);
                    Ok(BlockingSinkOutput::Partitions(vec![final_output]))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        format!("TopN {}", self.params.limit).into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::TopN
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut lines = vec![];
        assert!(!self.params.sort_by.is_empty());
        let pairs = self
            .params
            .sort_by
            .iter()
            .zip(self.params.descending.iter())
            .zip(self.params.nulls_first.iter())
            .map(|((sb, d), nf)| {
                format!(
                    "({}, {}, {})",
                    sb,
                    if *d { "descending" } else { "ascending" },
                    if *nf { "nulls first" } else { "nulls last" }
                )
            })
            .join(", ");

        let limit = self.params.limit;
        lines.push(match self.params.offset {
            Some(offset) => {
                format!(
                    "TopN: Sort by = {}, Num Rows = {}, Offset = {}",
                    pairs, limit, offset
                )
            }
            None => format!("TopN: Sort by = {}, Num Rows = {}", pairs, limit),
        });

        lines
    }

    fn make_state(&self, _input_id: InputId) -> DaftResult<Self::State> {
        Ok(TopNState::Building(vec![]))
    }
}
