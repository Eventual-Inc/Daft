use std::sync::Arc;

use common_error::DaftResult;
use daft_core::schema::SchemaRef;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::{
    intermediate_op::{IntermediateOpSpec, IntermediateOperator, OperatorOutput},
    state::OperatorTaskState,
};

#[derive(Clone)]
pub struct AggregateSpec {
    pub agg_exprs: Vec<ExprRef>,
    pub group_by: Vec<ExprRef>,
    pub input_schema: SchemaRef,
}

impl AggregateSpec {
    pub fn new(agg_exprs: Vec<ExprRef>, group_by: Vec<ExprRef>, input_schema: SchemaRef) -> Self {
        Self {
            agg_exprs,
            group_by,
            input_schema,
        }
    }
}

impl IntermediateOpSpec for AggregateSpec {
    fn to_operator(&self) -> Box<dyn IntermediateOperator> {
        Box::new(AggregateOperator {
            spec: self.clone(),
            state: OperatorTaskState::new(),
        })
    }
}

pub struct AggregateOperator {
    spec: AggregateSpec,
    state: OperatorTaskState,
}

impl IntermediateOperator for AggregateOperator {
    #[instrument(skip_all, name = "AggregateOperator::execute")]
    fn execute(&mut self, input: &Arc<MicroPartition>) -> DaftResult<OperatorOutput> {
        let agged = input.agg(&self.spec.agg_exprs, &self.spec.group_by)?;
        self.state.add(Arc::new(agged));

        match self.state.try_clear() {
            Some(part) => Ok(OperatorOutput::Ready(part?)),
            None => Ok(OperatorOutput::NeedMoreInput),
        }
    }

    fn finalize(&mut self) -> DaftResult<Option<Arc<MicroPartition>>> {
        match self.state.clear() {
            Some(part) => part.map(Some),
            None => {
                let empty_mp = MicroPartition::empty(Some(self.spec.input_schema.clone()));
                let agged = empty_mp.agg(&self.spec.agg_exprs, &self.spec.group_by)?;
                Ok(Some(Arc::new(agged)))
            }
        }
    }

    fn name(&self) -> &'static str {
        "AggregateOperator"
    }
}
