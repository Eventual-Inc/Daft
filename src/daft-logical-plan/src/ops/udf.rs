use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_resource_request::ResourceRequest;
use daft_core::prelude::Schema;
use daft_dsl::{
    expr::count_udfs,
    functions::python::{get_resource_request, get_udf_names, try_get_concurrency},
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
pub struct UDFProject {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    // Projected column info
    pub project: ExprRef,
    // Additional columns to pass through
    pub passthrough_columns: Vec<ExprRef>,

    pub projected_schema: SchemaRef,
    pub stats_state: StatsState,
}

impl UDFProject {
    /// Same as try_new, but with a DaftResult return type so this can be public.
    pub fn new(
        input: Arc<LogicalPlan>,
        project: ExprRef,
        passthrough_columns: Vec<ExprRef>,
    ) -> DaftResult<Self> {
        Ok(Self::try_new(input, project, passthrough_columns)?)
    }

    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        project: ExprRef,
        passthrough_columns: Vec<ExprRef>,
    ) -> Result<Self> {
        let num_udfs: usize = count_udfs(&[project.clone()]);
        if num_udfs != 1 {
            return Err(Error::CreationError {
                source: DaftError::InternalError(format!(
                    "Expected UDFProject to have exactly 1 UDF expression but found: {num_udfs}"
                )),
            });
        }

        let fields = passthrough_columns
            .iter()
            .chain(std::iter::once(&project))
            .map(|e| e.to_field(&input.schema()))
            .collect::<DaftResult<Vec<_>>>()?;
        let projected_schema = Arc::new(Schema::new(fields));

        Ok(Self {
            plan_id: None,
            node_id: None,
            input,
            project,
            passthrough_columns,
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
        get_resource_request(&[self.project.clone()])
    }

    pub fn concurrency(&self) -> Option<usize> {
        try_get_concurrency(&self.project)
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("UDFProject:".to_string());
        res.push(format!(
            "UDF {} = {}",
            get_udf_names(&self.project).join(", "),
            self.project
        ));
        res.push(format!(
            "Passthrough columns = {}",
            self.passthrough_columns
                .iter()
                .map(|c| c.to_string())
                .join(", ")
        ));
        res.push(format!("Concurrency = {:?}", self.concurrency()));
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
