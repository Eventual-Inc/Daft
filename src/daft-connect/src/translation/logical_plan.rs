use daft_logical_plan::LogicalPlanBuilder;
use eyre::{bail, Context};
use spark_connect::{relation::RelType, Relation};
use tracing::warn;

use crate::translation::logical_plan::{aggregate::aggregate, project::project, range::range};

mod aggregate;
mod project;
mod range;

pub fn to_logical_plan(relation: Relation) -> eyre::Result<LogicalPlanBuilder> {
    if let Some(common) = relation.common {
        warn!("Ignoring common metadata for relation: {common:?}; not yet implemented");
    };

    let Some(rel_type) = relation.rel_type else {
        bail!("Relation type is required");
    };

    match rel_type {
        RelType::Range(r) => range(r).wrap_err("Failed to apply range to logical plan"),
        RelType::Project(p) => project(*p).wrap_err("Failed to apply project to logical plan"),
        RelType::Aggregate(a) => {
            aggregate(*a).wrap_err("Failed to apply aggregate to logical plan")
        }
        plan => bail!("Unsupported relation type: {plan:?}"),
    }
}
