use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{instrument, Span};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult, BlockingSinkState,
    BlockingSinkStatus,
};
use crate::ExecutionTaskSpawner;

/// Parameters for the TopN that both the state and sinker need
struct TopNParams {
    // Sort By Parameters
    sort_by: Vec<ExprRef>,
    descending: Vec<bool>,
    nulls_first: Vec<bool>,
    // Limit Parameters
    limit: usize,
}

/// Current status of the TopN operation
enum TopNState {
    /// Operator is still collecting input and truncating to limit
    Building(Arc<MicroPartition>),
    /// Operator has finished collecting all input and ready to produce output
    Done,
}

impl TopNState {
    /// Process a new micro-partition and update the top N values
    fn push(&mut self, part: Arc<MicroPartition>, params: &TopNParams) {
        let Self::Building(ref mut top_values) = self else {
            panic!("TopNSink should be in Building state");
        };

        // First find the top N values in the input micro-partition
        let top_input_rows = part
            .sort(&params.sort_by, &params.descending, &params.nulls_first)
            .unwrap() // TODO: Where to catch errors
            .slice(0, params.limit)
            .unwrap();

        // Then combine with existing top N values to have 2*N values
        let concated = MicroPartition::concat([top_values, &top_input_rows]).unwrap();

        // Re-sort the combined partition to get the top N values
        // TODO: Use merge-sort merge algorithm to combine the two sorted partitions
        let sorted = Arc::new(
            concated
                .sort(&params.sort_by, &params.descending, &params.nulls_first)
                .unwrap(),
        );

        *top_values = sorted.slice(0, params.limit).unwrap().into();
    }

    /// Finalize the TopN operation and return the top N values
    fn finalize(&mut self) -> Arc<MicroPartition> {
        let Self::Building(top_values) = self else {
            panic!("TopNSink should be in Building state");
        };

        let top_values = top_values.clone();
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
    input_schema: SchemaRef,
    params: Arc<TopNParams>,
}

impl TopNSink {
    pub fn new(
        input_schema: &SchemaRef,
        sort_by: Vec<ExprRef>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
        limit: usize,
    ) -> Self {
        Self {
            input_schema: input_schema.clone(),
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
                    top_n_state.push(input, &params);
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
                    let parts = states.into_iter().map(|mut state| {
                        let state = state
                            .as_any_mut()
                            .downcast_mut::<TopNState>()
                            .expect("State type mismatch");
                        state.finalize()
                    });
                    let concated = MicroPartition::concat(parts)?;
                    let sorted = Arc::new(concated.sort(
                        &params.sort_by,
                        &params.descending,
                        &params.nulls_first,
                    )?);
                    Ok(Some(Arc::new(sorted.slice(0, params.limit)?)))
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
        Ok(Box::new(TopNState::Building(Arc::new(
            MicroPartition::empty(Some(self.input_schema.clone())),
        ))))
    }
}
