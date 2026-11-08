use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_metrics::ops::NodeType;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{Span, instrument};

use super::blocking_sink::{BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
    sinks::blocking_sink::BlockingSinkOutput,
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
    retained_limit_bytes: Option<usize>,
}

/// Current status of the TopN operation
pub(crate) enum TopNState {
    /// The optional partition is the global `limit + offset` prefix seen so
    /// far, never one partition per PostgreSQL input batch.
    Building(Option<MicroPartition>),
    /// Operator has finished collecting all input and ready to produce output
    /// by doing a final sort + limit
    Done,
}

impl TopNState {
    fn take_retained(&mut self) -> Option<MicroPartition> {
        let Self::Building(top_values) = self else {
            panic!("TopNSink should be in Building state");
        };
        top_values.take()
    }

    fn replace_retained(
        &mut self,
        part: MicroPartition,
        retained_limit_bytes: Option<usize>,
    ) -> DaftResult<()> {
        let Self::Building(top_values) = self else {
            panic!("TopNSink should be in Building state");
        };
        let retained_bytes = part.size_bytes();
        if let Some(limit) = retained_limit_bytes
            && retained_bytes > limit
        {
            return Err(DaftError::ComputeError(format!(
                "PGDAFT_TOP_N_MEMORY_LIMIT: retained {retained_bytes} bytes exceeds {limit} bytes; spill is not enabled"
            )));
        }
        *top_values = Some(part);
        Ok(())
    }

    /// Finalize and take the one global retained prefix.
    fn finalize(&mut self) -> Option<MicroPartition> {
        let Self::Building(top_values) = self else {
            panic!("TopNSink should be in Building state");
        };
        let top_values = top_values.take();
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
        retained_limit_bytes: Option<usize>,
    ) -> Self {
        Self {
            params: Arc::new(TopNParams {
                sort_by,
                descending,
                nulls_first,
                limit,
                offset,
                retained_limit_bytes,
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
                    // Reduce each input batch, merge it with the one prefix
                    // retained so far, and reduce again. State is therefore
                    // O(limit + offset), not O(number of input batches).
                    let retained = params
                        .limit
                        .checked_add(params.offset.unwrap_or(0))
                        .ok_or_else(|| {
                            DaftError::ComputeError(
                                "PGDAFT_TOP_N_LIMIT_OVERFLOW: limit + offset exceeds usize"
                                    .to_string(),
                            )
                        })?;
                    let top_input_rows = input.top_n(
                        &params.sort_by,
                        &params.descending,
                        &params.nulls_first,
                        retained,
                        Some(0),
                    )?;
                    let next = match state.take_retained() {
                        None => top_input_rows,
                        Some(previous) => {
                            let merged = MicroPartition::concat(vec![previous, top_input_rows])?;
                            merged.top_n(
                                &params.sort_by,
                                &params.descending,
                                &params.nulls_first,
                                retained,
                                Some(0),
                            )?
                        }
                    };
                    state.replace_retained(next, params.retained_limit_bytes)?;
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
                    let parts: Vec<MicroPartition> = states
                        .into_iter()
                        .filter_map(|mut state| state.finalize())
                        .collect();
                    if parts.is_empty() {
                        return Ok(BlockingSinkOutput::Partitions(Vec::new()));
                    }
                    let concated = MicroPartition::concat(parts)?;
                    let final_output = concated.top_n(
                        &params.sort_by,
                        &params.descending,
                        &params.nulls_first,
                        params.limit,
                        params.offset,
                    )?;
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
        Ok(TopNState::Building(None))
    }

    fn max_concurrency(&self) -> usize {
        // One state makes the retained-prefix memory bound exact. The child
        // pipeline remains free to execute in parallel.
        1
    }
}
