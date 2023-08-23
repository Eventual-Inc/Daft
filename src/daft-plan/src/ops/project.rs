use std::sync::Arc;

use common_error::DaftResult;
use daft_core::schema::{Schema, SchemaRef};
use daft_dsl::Expr;
use snafu::ResultExt;

use crate::logical_plan::{CreationSnafu, Result};
use crate::{LogicalPlan, ResourceRequest};

#[derive(Clone, Debug)]
pub struct Project {
    pub projection: Vec<Expr>,
    pub projected_schema: SchemaRef,
    pub resource_request: ResourceRequest,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
}

impl Project {
    pub(crate) fn new(
        projection: Vec<Expr>,
        resource_request: ResourceRequest,
        input: Arc<LogicalPlan>,
    ) -> Result<Self> {
        let upstream_schema = input.schema();
        let projected_schema = {
            let fields = projection
                .iter()
                .map(|e| e.to_field(&upstream_schema))
                .collect::<DaftResult<Vec<_>>>()
                .context(CreationSnafu)?;
            Schema::new(fields).unwrap().into()
        };
        Ok(Self {
            projection,
            projected_schema,
            resource_request,
            input,
        })
    }
}
