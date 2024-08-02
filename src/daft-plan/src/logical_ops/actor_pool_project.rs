use std::sync::Arc;

use daft_core::schema::{Schema, SchemaRef};
use daft_dsl::{resolve_exprs, ExprRef};
use itertools::Itertools;
use snafu::ResultExt;

use crate::{
    logical_plan::{CreationSnafu, Result},
    LogicalPlan, ResourceRequest,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ActorPoolProject {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub projection: Vec<ExprRef>,
    pub resource_request: ResourceRequest,
    pub projected_schema: SchemaRef,
    pub num_actors: usize,
}

impl ActorPoolProject {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        projection: Vec<ExprRef>,
        resource_request: ResourceRequest,
        num_actors: usize,
    ) -> Result<Self> {
        let (projection, fields) =
            resolve_exprs(projection, input.schema().as_ref()).context(CreationSnafu)?;
        let projected_schema = Schema::new(fields).context(CreationSnafu)?.into();
        Ok(ActorPoolProject {
            input,
            projection,
            resource_request,
            projected_schema,
            num_actors,
        })
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![
            format!(
                "ActorPoolProject: {}",
                self.projection.iter().map(|e| e.to_string()).join(", ")
            ),
            format!("Num Actors = {}", self.num_actors),
        ];
        res.extend(self.resource_request.multiline_display());
        res
    }
}
