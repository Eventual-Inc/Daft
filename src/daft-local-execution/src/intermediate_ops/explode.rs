use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_functions_list::explode;
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{instrument, Span};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOperator, IntermediateOperatorResult,
};
use crate::{ops::NodeType, pipeline::NodeName, ExecutionTaskSpawner};

pub struct ExplodeOperator {
    to_explode: Arc<Vec<BoundExpr>>,
}

impl ExplodeOperator {
    pub fn new(to_explode: Vec<BoundExpr>) -> Self {
        Self {
            to_explode: Arc::new(
                to_explode
                    .into_iter()
                    .map(|expr| BoundExpr::new_unchecked(explode(expr.inner().clone())))
                    .collect(),
            ),
        }
    }
}

impl IntermediateOperator for ExplodeOperator {
    type State = ();

    #[instrument(skip_all, name = "ExplodeOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Self::State,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult<Self> {
        let to_explode = self.to_explode.clone();
        task_spawner
            .spawn(
                async move {
                    let out = input.explode(&to_explode)?;
                    Ok((
                        state,
                        IntermediateOperatorResult::NeedMoreInput(Some(Arc::new(out))),
                    ))
                },
                Span::current(),
            )
            .into()
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![format!(
            "Explode: {}",
            self.to_explode.iter().map(|e| e.to_string()).join(", ")
        )]
    }

    fn name(&self) -> NodeName {
        "Explode".into()
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        Ok(())
    }

    fn op_type(&self) -> NodeType {
        NodeType::Explode
    }
}
