use std::sync::Arc;

use daft_dsl::Expr;

use crate::{physical_plan::PhysicalPlan, ResourceRequest};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Project {
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
    pub projection: Vec<Expr>,
    pub resource_request: ResourceRequest,
}

impl Project {
    pub(crate) fn new(
        input: Arc<PhysicalPlan>,
        projection: Vec<Expr>,
        resource_request: ResourceRequest,
    ) -> Self {
        Self {
            input,
            projection,
            resource_request,
        }
    }
}
