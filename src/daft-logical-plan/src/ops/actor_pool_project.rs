use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_resource_request::ResourceRequest;
use daft_dsl::{
    count_actor_pool_udfs, exprs_to_schema,
    functions::python::{get_concurrency, get_resource_request, get_udf_names},
    ExprRef,
};
use daft_schema::schema::SchemaRef;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    logical_plan::{Error, Result},
    stats::StatsState,
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActorPoolProject {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub projection: Vec<ExprRef>,
    pub projected_schema: SchemaRef,
    pub stats_state: StatsState,
}

impl ActorPoolProject {
    /// Same as try_new, but with a DaftResult return type so this can be public.
    pub fn new(input: Arc<LogicalPlan>, projection: Vec<ExprRef>) -> DaftResult<Self> {
        Ok(Self::try_new(input, projection)?)
    }

    pub(crate) fn try_new(input: Arc<LogicalPlan>, projection: Vec<ExprRef>) -> Result<Self> {
        let num_actor_pool_udfs: usize = count_actor_pool_udfs(&projection);
        if !num_actor_pool_udfs == 1 {
            return Err(Error::CreationError { source: DaftError::InternalError(format!("Expected ActorPoolProject to have exactly 1 actor pool UDF expression but found: {num_actor_pool_udfs}")) });
        }

        let projected_schema = exprs_to_schema(&projection, input.schema())?;

        Ok(Self {
            plan_id: None,
            node_id: None,
            input,
            projection,
            projected_schema,
            stats_state: StatsState::NotMaterialized,
        })
    }

    pub fn with_plan_id(mut self, plan_id: usize) -> Self {
        self.plan_id = Some(plan_id);
        self
    }

    pub fn with_node_id(mut self, node_id: usize) -> Self {
        self.node_id = Some(node_id);
        self
    }

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        // TODO(desmond): We can do better estimations with the projection schema. For now, reuse the old logic.
        let input_stats = self.input.materialized_stats();
        self.stats_state = StatsState::Materialized(input_stats.clone().into());
        self
    }

    pub fn resource_request(&self) -> Option<ResourceRequest> {
        get_resource_request(self.projection.as_slice())
    }

    pub fn concurrency(&self) -> usize {
        get_concurrency(self.projection.as_slice())
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("ActorPoolProject:".to_string());
        res.push(format!(
            "Projection = [{}]",
            self.projection.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "UDFs = [{}]",
            self.projection.iter().flat_map(get_udf_names).join(", ")
        ));
        res.push(format!("Concurrency = {}", self.concurrency()));
        if let Some(resource_request) = self.resource_request() {
            let multiline_display = resource_request.multiline_display();
            res.push(format!(
                "Resource request = {{ {} }}",
                multiline_display.join(", ")
            ));
        }
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
