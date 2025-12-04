use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::Schema;
use daft_dsl::{ExprRef, functions::python::UDFProperties};
use daft_schema::schema::SchemaRef;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{LogicalPlan, logical_plan::Result, stats::StatsState};

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct UDFProject {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    // UDF
    pub expr: ExprRef,
    pub udf_properties: UDFProperties,
    // Additional columns to pass through
    pub passthrough_columns: Vec<ExprRef>,

    pub projected_schema: SchemaRef,
    pub stats_state: StatsState,
}

impl UDFProject {
    /// Same as try_new, but with a DaftResult return type so this can be public.
    pub fn new(
        input: Arc<LogicalPlan>,
        udf_expr: ExprRef,
        passthrough_columns: Vec<ExprRef>,
    ) -> DaftResult<Self> {
        Ok(Self::try_new(input, udf_expr, passthrough_columns)?)
    }

    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        expr: ExprRef,
        passthrough_columns: Vec<ExprRef>,
    ) -> Result<Self> {
        let udf_properties = UDFProperties::from_expr(&expr)?;
        let output_field = expr.to_field(&input.schema())?;

        let fields = passthrough_columns
            .iter()
            .map(|e| e.to_field(&input.schema()))
            .chain(std::iter::once(Ok(output_field)))
            .collect::<DaftResult<Vec<_>>>()?;
        let projected_schema = Arc::new(Schema::new(fields));

        Ok(Self {
            plan_id: None,
            node_id: None,
            input,
            expr,
            udf_properties,
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

    pub fn is_actor_pool_udf(&self) -> bool {
        self.udf_properties.is_actor_pool_udf()
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![
            format!("UDF: {}", self.udf_properties.name),
            format!("Expr = {}", self.expr),
            format!(
                "Passthrough Columns = {}",
                if self.passthrough_columns.is_empty() {
                    "None".to_string()
                } else {
                    self.passthrough_columns
                        .iter()
                        .map(|c| c.to_string())
                        .join(", ")
                }
            ),
            format!(
                "Properties = {{ {} }}",
                self.udf_properties.multiline_display(false).join(", ")
            ),
        ];

        if let Some(resource_request) = &self.udf_properties.resource_request {
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
