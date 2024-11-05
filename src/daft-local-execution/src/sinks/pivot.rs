use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::{AggExpr, Expr, ExprRef};
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::blocking_sink::{BlockingSink, BlockingSinkState, BlockingSinkStatus};
use crate::{pipeline::PipelineResultType, NUM_CPUS};

enum PivotState {
    Accumulating(Vec<Arc<MicroPartition>>),
    Done,
}

impl PivotState {
    fn push(&mut self, part: Arc<MicroPartition>) {
        if let Self::Accumulating(ref mut parts) = self {
            parts.push(part);
        } else {
            panic!("PivotSink should be in Accumulating state");
        }
    }

    fn finalize(&mut self) -> Vec<Arc<MicroPartition>> {
        let res = if let Self::Accumulating(ref mut parts) = self {
            std::mem::take(parts)
        } else {
            panic!("PivotSink should be in Accumulating state");
        };
        *self = Self::Done;
        res
    }
}

impl BlockingSinkState for PivotState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct PivotSink {
    pub group_by: Vec<ExprRef>,
    pub pivot_column: ExprRef,
    pub value_column: ExprRef,
    pub aggregation: AggExpr,
    pub names: Vec<String>,
}

impl PivotSink {
    pub fn new(
        group_by: Vec<ExprRef>,
        pivot_column: ExprRef,
        value_column: ExprRef,
        aggregation: AggExpr,
        names: Vec<String>,
    ) -> Self {
        Self {
            group_by,
            pivot_column,
            value_column,
            aggregation,
            names,
        }
    }
}

impl BlockingSink for PivotSink {
    #[instrument(skip_all, name = "PivotSink::sink")]
    fn sink(
        &self,
        input: &Arc<MicroPartition>,
        mut state: Box<dyn BlockingSinkState>,
    ) -> DaftResult<BlockingSinkStatus> {
        state
            .as_any_mut()
            .downcast_mut::<PivotState>()
            .expect("PivotSink should have PivotState")
            .push(input.clone());
        Ok(BlockingSinkStatus::NeedMoreInput(state))
    }

    #[instrument(skip_all, name = "PivotSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
    ) -> DaftResult<Option<PipelineResultType>> {
        let all_parts = states.into_iter().flat_map(|mut state| {
            state
                .as_any_mut()
                .downcast_mut::<PivotState>()
                .expect("PivotSink should have PivotState")
                .finalize()
        });
        let concated = MicroPartition::concat(all_parts)?;
        let group_by_with_pivot = self
            .group_by
            .iter()
            .chain(std::iter::once(&self.pivot_column))
            .cloned()
            .collect::<Vec<_>>();
        let agged = concated.agg(
            &[Expr::Agg(self.aggregation.clone()).into()],
            &group_by_with_pivot,
        )?;
        let pivoted = Arc::new(agged.pivot(
            &self.group_by,
            self.pivot_column.clone(),
            self.value_column.clone(),
            self.names.clone(),
        )?);
        Ok(Some(pivoted.into()))
    }

    fn name(&self) -> &'static str {
        "PivotSink"
    }

    fn max_concurrency(&self) -> usize {
        *NUM_CPUS
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(PivotState::Accumulating(vec![])))
    }
}
