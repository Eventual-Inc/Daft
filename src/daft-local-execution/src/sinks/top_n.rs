use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{instrument, Span};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeOutput, BlockingSinkFinalizeResult, BlockingSinkSinkResult,
    BlockingSinkState, BlockingSinkStatus,
};
use crate::ExecutionTaskSpawner;

/// Parameters for the TopN that both the state and sinker need
struct TopNParams {
    // Sort By Parameters
    sort_by: Vec<BoundExpr>,
    descending: Vec<bool>,
    nulls_first: Vec<bool>,
    // Limit Parameters
    limit: usize,
}

/// Current status of the TopN operation
enum TopNState {
    /// Operator is still collecting input
    Building(Vec<Arc<MicroPartition>>),
    /// Operator has finished collecting all input and ready to produce output
    /// by doing a final sort + limit
    Done,
}

impl TopNState {
    /// Process a new micro-partition and update the top N values
    fn append(&mut self, part: Arc<MicroPartition>) {
        let Self::Building(ref mut top_values) = self else {
            panic!("TopNSink should be in Building state");
        };

        top_values.push(part);
    }

    /// Finalize the TopN operation and return the top N values
    fn finalize(&mut self) -> Vec<Arc<MicroPartition>> {
        let Self::Building(top_values) = self else {
            panic!("TopNSink should be in Building state");
        };

        let top_values = std::mem::take(top_values);
        *self = Self::Done;
        top_values
    }
}

impl BlockingSinkState for TopNState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
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
    ) -> Self {
        Self {
            params: Arc::new(TopNParams {
                sort_by,
                descending,
                nulls_first,
                limit,
            }),
        }
    }
}

impl BlockingSink for TopNSink {
    #[instrument(skip_all, name = "TopNSink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn BlockingSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult {
        let params = self.params.clone();

        spawner
            .spawn(
                async move {
                    let top_n_state = state
                        .as_any_mut()
                        .downcast_mut::<TopNState>()
                        .expect("TopNSink should have top_n state");

                    // Find the top N values in the input micro-partition
                    let top_input_rows = input.top_n(
                        &params.sort_by,
                        &params.descending,
                        &params.nulls_first,
                        params.limit,
                    )?;

                    // Append to the collection of existing top N values
                    top_n_state.append(Arc::new(top_input_rows));
                    Ok(BlockingSinkStatus::NeedMoreInput(state))
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "TopNSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let params = self.params.clone();
        spawner
            .spawn(
                async move {
                    let parts = states.into_iter().flat_map(|mut state| {
                        let state = state
                            .as_any_mut()
                            .downcast_mut::<TopNState>()
                            .expect("State type mismatch");
                        state.finalize()
                    });
                    let concated = MicroPartition::concat(parts)?;
                    let final_output = Arc::new(concated.top_n(
                        &params.sort_by,
                        &params.descending,
                        &params.nulls_first,
                        params.limit,
                    )?);
                    Ok(BlockingSinkFinalizeOutput::Finished(vec![final_output]))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> &'static str {
        "TopN"
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
        lines.push(format!(
            "TopN: Sort by = {}, Num Rows = {}",
            pairs, self.params.limit
        ));
        lines
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(TopNState::Building(vec![])))
    }
}
