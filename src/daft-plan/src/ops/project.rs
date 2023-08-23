use std::sync::Arc;

use daft_core::schema::{Schema, SchemaRef};
use daft_dsl::Expr;
use snafu::ResultExt;

use crate::logical_plan::{CreationSnafu, Result};
use crate::{LogicalPlan, ResourceRequest};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Project {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub projection: Vec<Expr>,
    pub resource_request: ResourceRequest,
    pub projected_schema: SchemaRef,
}

impl Project {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        projection: Vec<Expr>,
        resource_request: ResourceRequest,
    ) -> Result<Self> {
        let upstream_schema = input.schema();
        let projected_schema = {
            let fields = projection
                .iter()
                .map(|e| e.to_field(&upstream_schema))
                .collect::<common_error::DaftResult<Vec<_>>>()
                .context(CreationSnafu)?;
            Schema::new(fields).context(CreationSnafu)?.into()
        };
        Ok(Self {
            input,
            projection,
            resource_request,
            projected_schema,
        })
    }
}
