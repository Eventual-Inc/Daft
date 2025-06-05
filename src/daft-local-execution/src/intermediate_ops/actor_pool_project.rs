use std::{sync::Arc, vec};

use common_error::DaftResult;
use daft_dsl::{
    count_actor_pool_udfs,
    expr::bound_expr::BoundExpr,
    functions::python::{
        get_concurrency, get_resource_request, get_udf_names, try_get_batch_size_from_udf,
    },
};
use daft_local_plan::ActorHandle;
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{instrument, Span};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOpState, IntermediateOperator,
    IntermediateOperatorResult,
};
use crate::{ExecutionRuntimeContext, ExecutionTaskSpawner};

/// Each ActorPoolProjectState holds a handle to a single actor process.
/// The concurrency of the actor pool is thus tied to the concurrency of the operator
/// and the local executor handles task scheduling.
///
/// TODO: Implement a work-stealing dispatcher in the executor to improve pipelining.
struct ActorPoolProjectState {
    pub actor_handle: ActorHandle,
}

impl IntermediateOpState for ActorPoolProjectState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct ActorPoolProjectOperator {
    projection: Vec<BoundExpr>,
    concurrency: usize,
    batch_size: Option<usize>,
    memory_request: u64,
    existing_actor_handle: Option<ActorHandle>,
}

impl ActorPoolProjectOperator {
    pub fn try_new(
        projection: Vec<BoundExpr>,
        existing_actor_handle: Option<ActorHandle>,
    ) -> DaftResult<Self> {
        let projection_unbound = projection
            .iter()
            .map(|expr| expr.inner().clone())
            .collect::<Vec<_>>();

        let num_actor_pool_udfs: usize = count_actor_pool_udfs(&projection_unbound);

        assert_eq!(
            num_actor_pool_udfs, 1,
            "Expected only one actor pool udf in an actor pool project"
        );

        let concurrency = get_concurrency(&projection_unbound);
        let batch_size = try_get_batch_size_from_udf(&projection_unbound)?;

        let memory_request = get_resource_request(&projection)
            .and_then(|req| req.memory_bytes())
            .map(|m| m as u64)
            .unwrap_or(0);
        Ok(Self {
            projection,
            concurrency,
            batch_size,
            memory_request,
            existing_actor_handle,
        })
    }
}

impl IntermediateOperator for ActorPoolProjectOperator {
    #[instrument(skip_all, name = "ActorPoolProjectOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn IntermediateOpState>,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult {
        let memory_request = self.memory_request;
        let fut = task_spawner.spawn_with_memory_request(
            memory_request,
            async move {
                let actor_pool_project_state = state
                    .as_any_mut()
                    .downcast_mut::<ActorPoolProjectState>()
                    .expect("ActorPoolProjectState");
                let res = actor_pool_project_state
                    .actor_handle
                    .eval_input(input)
                    .map(|result| IntermediateOperatorResult::NeedMoreInput(Some(result)))?;
                Ok((state, res))
            },
            Span::current(),
        );
        fut.into()
    }

    fn name(&self) -> &'static str {
        "ActorPoolProject"
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("ActorPoolProject:".to_string());
        res.push(format!(
            "Projection = [{}]",
            self.projection.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "UDFs = [{}]",
            self.projection
                .iter()
                .flat_map(|expr| get_udf_names(expr.inner()))
                .join(", ")
        ));
        res.push(format!("Concurrency = {}", self.concurrency));
        if let Some(resource_request) = get_resource_request(&self.projection) {
            let multiline_display = resource_request.multiline_display();
            res.push(format!(
                "Resource request = {{ {} }}",
                multiline_display.join(", ")
            ));
        } else {
            res.push("Resource request = None".to_string());
        }
        res
    }

    fn make_state(&self) -> DaftResult<Box<dyn IntermediateOpState>> {
        // TODO: Pass relevant CUDA_VISIBLE_DEVICES to the actor
        Ok(Box::new(ActorPoolProjectState {
            actor_handle: self.existing_actor_handle.clone().unwrap_or_else(|| {
                // Swordfish only supports process actors right now, not Ray actors
                ActorHandle::try_new(&self.projection, false).unwrap()
            }),
        }))
    }

    fn max_concurrency(&self) -> DaftResult<usize> {
        Ok(self.concurrency)
    }

    fn morsel_size_range(&self, runtime_handle: &ExecutionRuntimeContext) -> (usize, usize) {
        if let Some(batch_size) = self.batch_size {
            (batch_size, batch_size)
        } else {
            (0, runtime_handle.default_morsel_size())
        }
    }
}
