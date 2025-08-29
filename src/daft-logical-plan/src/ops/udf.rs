use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_resource_request::ResourceRequest;
use daft_core::prelude::Schema;
use daft_dsl::{
    expr::count_udfs,
    functions::python::{get_udf_properties, UDFProperties},
    ExprRef,
};
use daft_schema::{dtype::DataType, schema::SchemaRef};
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
    // UDF properties
    pub udf_properties: UDFProperties,

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

        let udf_properties = get_udf_properties(&project);

        // Check if any inputs or outputs are Python-dtype columns
        // and that use_process != true
        #[cfg(feature = "python")]
        {
            if matches!(udf_properties.use_process, Some(true)) {
                if let Some(col) = input
                    .schema()
                    .fields()
                    .iter()
                    .find(|f| matches!(f.dtype, DataType::Python))
                {
                    return Err(Error::CreationError {
                        source: DaftError::InternalError(
                            format!("UDF `{}` can not set `use_process=True` because it has a Python-dtype input column `{}`. Please unset `use_process` or cast the input to a non-Python dtype if possible.", udf_properties.name, col.name)
                        ),
                    });
                }
                if project.to_field(&input.schema())?.dtype == DataType::Python {
                    return Err(Error::CreationError {
                        source: DaftError::InternalError(
                            format!("UDF `{}` can not set `use_process=True` because it returns a Python-dtype value. Please unset `use_process` or specify the `return_dtype` to another dtype if possible.", udf_properties.name)
                        ),
                    });
                }
            }
        }

        Ok(Self {
            plan_id: None,
            node_id: None,
            input,
            project,
            passthrough_columns,
            udf_properties,
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
        self.udf_properties.resource_request.clone()
    }

    pub fn concurrency(&self) -> Option<usize> {
        self.udf_properties.concurrency
    }

    pub fn is_actor_pool_udf(&self) -> bool {
        self.udf_properties.concurrency.is_some()
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("UDFProject:".to_string());
        res.push(format!(
            "UDF {} = {}",
            self.udf_properties.name, self.project
        ));
        res.push(format!(
            "Passthrough Columns = {}",
            if self.passthrough_columns.is_empty() {
                "None".to_string()
            } else {
                self.passthrough_columns
                    .iter()
                    .map(|c| c.to_string())
                    .join(", ")
            }
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
