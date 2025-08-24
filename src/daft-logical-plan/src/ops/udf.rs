use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_core::prelude::Schema;
use daft_dsl::{expr::count_udfs, functions::python::UDFImpl, ExprRef};
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
    // UDF
    pub udf_expr: UDFImpl,
    pub out_name: Arc<str>,
    // Additional columns to pass through
    pub passthrough_columns: Vec<ExprRef>,

    pub projected_schema: SchemaRef,
    pub stats_state: StatsState,
}

impl UDFProject {
    /// Same as try_new, but with a DaftResult return type so this can be public.
    pub fn new(
        input: Arc<LogicalPlan>,
        udf_expr: UDFImpl,
        out_name: Arc<str>,
        passthrough_columns: Vec<ExprRef>,
    ) -> DaftResult<Self> {
        Ok(Self::try_new(
            input,
            udf_expr,
            out_name,
            passthrough_columns,
        )?)
    }

    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        udf_expr: UDFImpl,
        out_name: Arc<str>,
        passthrough_columns: Vec<ExprRef>,
    ) -> Result<Self> {
        let num_udfs: usize = count_udfs(&udf_expr.to_expr());
        if num_udfs != 1 {
            return Err(Error::CreationError {
                source: DaftError::InternalError(format!(
                    "Expected UDFProject to have exactly 1 UDF expression but found: {num_udfs}"
                )),
            });
        }

        let fields = passthrough_columns
            .iter()
            .map(|e| e.to_field(&input.schema()))
            .chain(std::iter::once(
                udf_expr
                    .to_expr()
                    .alias(out_name.clone())
                    .to_field(&input.schema()),
            ))
            .collect::<DaftResult<Vec<_>>>()?;
        let projected_schema = Arc::new(Schema::new(fields));

        Ok(Self {
            plan_id: None,
            node_id: None,
            input,
            udf_expr,
            out_name,
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
        self.udf_expr.concurrency().is_some()
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![
            format!("UDF: {}", self.udf_expr.name()),
            format!(
                "Expr = {}",
                self.udf_expr.to_expr().alias(self.out_name.clone())
            ),
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
            format!("Concurrency = {:?}", self.udf_expr.concurrency()),
        ];
        if let Some(resource_request) = self.udf_expr.resource_request() {
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
