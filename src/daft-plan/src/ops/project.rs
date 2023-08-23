use std::sync::Arc;

use daft_core::schema::{Schema, SchemaRef};
use daft_dsl::Expr;

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
    ) -> Self {
        let upstream_schema = input.schema();
        let projected_schema = {
            let fields = projection
                .iter()
                .map(|e| e.to_field(&upstream_schema).unwrap())
                .collect::<Vec<_>>();
            Schema::new(fields).unwrap().into()
        };
        Self {
            projection,
            projected_schema,
            resource_request,
            input,
        }
    }
}
