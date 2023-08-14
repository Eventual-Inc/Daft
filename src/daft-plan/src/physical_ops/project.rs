use std::sync::Arc;

use daft_dsl::Expr;

use crate::{physical_plan::PhysicalPlan, ResourceRequest};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Project {
    pub projection: Vec<Expr>,
    pub resource_request: ResourceRequest,
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
}

impl Project {
    pub(crate) fn new(
        projection: Vec<Expr>,
        resource_request: ResourceRequest,
        input: Arc<PhysicalPlan>,
    ) -> Self {
        Self {
            projection,
            resource_request,
            input,
        }
    }
}
