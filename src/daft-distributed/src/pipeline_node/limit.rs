use std::{collections::HashSet, sync::Arc};

use common_error::DaftResult;
use common_metrics::ops::{NodeCategory, NodeType};
#[cfg(feature = "python")]
use common_py_serde::PyObjectWrapper;
use common_runtime::JoinSet;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;
use futures::StreamExt;
#[cfg(feature = "python")]
use pyo3::{Py, PyAny, Python, types::PyAnyMethods};
#[cfg(feature = "python")]
use tokio_util::sync::CancellationToken;

use super::{DistributedPipelineNode, PipelineNodeImpl, TaskBuilderStream};
use crate::{
    pipeline_node::{NodeID, PipelineNodeConfig, PipelineNodeContext},
    plan::{PlanConfig, PlanExecutionContext},
    scheduling::task::{SwordfishTaskBuilder, TaskID},
    utils::channel::{Sender, create_channel},
};

#[cfg(feature = "python")]
async fn start_limit_counter_actor(
    limit: u64,
    offset: u64,
    timeout: usize,
) -> DaftResult<PyObjectWrapper> {
    let actor: Py<PyAny> =
        common_runtime::python::execute_python_coroutine::<_, Py<PyAny>>(move |py| {
            let module = py.import(pyo3::intern!(py, "daft.execution.ray_distributed_limit"))?;
            let coroutine = module.call_method1(
                pyo3::intern!(py, "start_limit_counter_actor"),
                (limit as i64, offset as i64, timeout as i64),
            )?;
            Ok(coroutine)
        })
        .await?;
    Ok(PyObjectWrapper(Arc::new(actor)))
}

#[cfg(feature = "python")]
fn teardown_limit_counter_actor(actor: &PyObjectWrapper) {
    Python::attach(|py| {
        if let Err(e) = actor.0.call_method1(py, pyo3::intern!(py, "teardown"), ()) {
            tracing::warn!("Error tearing down limit counter actor: {:?}", e);
        }
    });
}

#[cfg(feature = "python")]
async fn await_limit_completion(actor: &PyObjectWrapper) -> DaftResult<Vec<String>> {
    let actor = actor.0.clone();
    common_runtime::python::execute_python_coroutine::<_, Vec<String>>(move |py| {
        let coroutine = actor.call_method1(py, pyo3::intern!(py, "await_limit_completion"), ())?;
        Ok(coroutine.into_bound(py))
    })
    .await
}

pub(crate) struct LimitNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    limit: u64,
    offset: Option<u64>,
    child: DistributedPipelineNode,
}

impl LimitNode {
    const NODE_NAME: &'static str = "Limit";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        limit: usize,
        offset: Option<usize>,
        schema: SchemaRef,
        child: DistributedPipelineNode,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Arc::from(Self::NODE_NAME),
            NodeType::Limit,
            NodeCategory::StreamingSink,
        );
        let config = PipelineNodeConfig::new(
            schema,
            plan_config.config.clone(),
            child.config().clustering_spec.clone(),
        );
        Self {
            config,
            context,
            limit: limit as u64,
            offset: offset.map(|o| o as u64),
            child,
        }
    }

    #[cfg(feature = "python")]
    async fn limit_execution_loop(
        self: Arc<Self>,
        mut input_task_stream: TaskBuilderStream,
        result_tx: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        let actor = start_limit_counter_actor(
            self.limit,
            self.offset.unwrap_or(0),
            self.config.execution_config.actor_udf_ready_timeout,
        )
        .await?;

        let parent_cancel = CancellationToken::new();
        let mut running_tasks = JoinSet::new();
        let mut completed_ids: HashSet<TaskID> = HashSet::new();
        let mut contributors: Option<HashSet<TaskID>> = None;
        let mut contributors_fut = Box::pin(await_limit_completion(&actor));
        let mut input_exhausted = false;
        loop {
            // Break once every contributing input_id has materialized; for
            // limit==0 or limits never hit, fall through to natural exhaustion.
            let done = match &contributors {
                Some(c) if !c.is_empty() => c.is_subset(&completed_ids),
                _ => input_exhausted && running_tasks.is_empty(),
            };
            if done {
                break;
            }
            tokio::select! {
                biased;
                res = &mut contributors_fut, if contributors.is_none() => {
                    contributors = Some(
                        res?
                            .into_iter()
                            .filter_map(|s| s.parse::<TaskID>().ok())
                            .collect(),
                    );
                }
                Some(join_result) = running_tasks.join_next(), if !running_tasks.is_empty() => {
                    if let Ok(Ok(task_id)) = join_result {
                        completed_ids.insert(task_id);
                    }
                }
                builder_opt = input_task_stream.next(), if !input_exhausted => {
                    let Some(builder) = builder_opt else {
                        input_exhausted = true;
                        continue;
                    };
                    let limit = self.limit;
                    let offset = self.offset;
                    let node_id = self.node_id();
                    let actor_for_plan = actor.clone();
                    let builder = builder
                        .map_plan(self.as_ref(), move |input| {
                            LocalPhysicalPlan::distributed_limit(
                                input,
                                actor_for_plan.clone(),
                                limit,
                                offset,
                                StatsState::NotMaterialized,
                                LocalNodeContext::new(Some(node_id as usize)),
                            )
                        })
                        .with_cancel_token(parent_cancel.child_token());
                    let (builder, notify_token) = builder.add_notify_token();
                    running_tasks.spawn(notify_token);
                    if result_tx.send(builder).await.is_err() {
                        input_exhausted = true;
                    }
                }
                else => break,
            }
        }
        drop(result_tx);
        parent_cancel.cancel();
        teardown_limit_counter_actor(&actor);
        Ok(())
    }
}

impl PipelineNodeImpl for LimitNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![self.child.clone()]
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        match &self.offset {
            Some(o) => vec![format!("Limit: Num Rows = {}, Offset = {}", self.limit, o)],
            None => vec![format!("Limit: {}", self.limit)],
        }
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let input_stream = self.child.clone().produce_tasks(plan_context);

        #[cfg(feature = "python")]
        {
            let (result_tx, result_rx) = create_channel(1);
            plan_context.spawn(self.limit_execution_loop(input_stream, result_tx));
            TaskBuilderStream::from(result_rx)
        }
        #[cfg(not(feature = "python"))]
        {
            let _ = input_stream;
            unimplemented!("Distributed Limit requires the python feature")
        }
    }
}
