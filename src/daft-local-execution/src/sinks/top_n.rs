use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{instrument, Span};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult, BlockingSinkState,
    BlockingSinkStatus,
};
use crate::ExecutionTaskSpawner;

struct TopNParams {
    sort_by: Vec<ExprRef>,
    descending: Vec<bool>,
    nulls_first: Vec<bool>,
    limit: usize,
}

enum TopNStatus {
    Building,
    Done,
}

struct TopNState {
    top_values: Arc<MicroPartition>,
    status: TopNStatus,
}

impl TopNState {
    fn push(&mut self, part: Arc<MicroPartition>, params: &TopNParams) {
        if matches!(self.status, TopNStatus::Building) {
            let concatenated = MicroPartition::concat([self.top_values.clone(), part]).unwrap();
            let sorted = Arc::new(
                concatenated
                    .sort(&params.sort_by, &params.descending, &params.nulls_first)
                    .unwrap(),
            );
            self.top_values = sorted.slice(0, params.limit).unwrap().into();
        } else {
            panic!("TopNSink should be in Building state");
        }
    }

    fn finalize(&mut self) -> Arc<MicroPartition> {
        let res = if matches!(self.status, TopNStatus::Building) {
            self.top_values.clone()
        } else {
            panic!("TopNSink should be in Building state");
        };
        self.status = TopNStatus::Done;
        res
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
        sort_by: Vec<ExprRef>,
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
        _spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult {
        state
            .as_any_mut()
            .downcast_mut::<TopNState>()
            .expect("TopNSink should have top_n state")
            .push(input, &self.params);
        Ok(BlockingSinkStatus::NeedMoreInput(state)).into()
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
        Ok(Box::new(TopNState {
            top_values: Arc::new(MicroPartition::empty(None)),
            status: TopNStatus::Building,
        }))
    }
}
