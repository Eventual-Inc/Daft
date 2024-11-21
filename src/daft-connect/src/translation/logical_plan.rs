use daft_logical_plan::LogicalPlanBuilder;
use eyre::{bail, Context};
use spark_connect::{relation::RelType, Limit, Relation};
use tracing::warn;

use crate::translation::logical_plan::{aggregate::aggregate, project::project, range::range, to_df::to_df};

mod aggregate;
mod project;
mod range;
mod to_df;
mod local_relation;

pub fn to_logical_plan(relation: Relation) -> eyre::Result<LogicalPlanBuilder> {
    if let Some(common) = relation.common {
        warn!("Ignoring common metadata for relation: {common:?}; not yet implemented");
    };

    let Some(rel_type) = relation.rel_type else {
        bail!("Relation type is required");
    };

    match rel_type {
        RelType::Limit(l) => limit(*l).wrap_err("Failed to apply limit to logical plan"),
        RelType::Range(r) => range(r).wrap_err("Failed to apply range to logical plan"),
        RelType::Project(p) => project(*p).wrap_err("Failed to apply project to logical plan"),
        RelType::Aggregate(a) => {
            aggregate(*a).wrap_err("Failed to apply aggregate to logical plan")
        }
        RelType::ToDf(t) => to_df(*t).wrap_err("Failed to apply to_df to logical plan"),
        RelType::LocalRelation(l) => local_relation(*l).wrap_err("Failed to apply local_relation to logical plan"),
        plan => bail!("Unsupported relation type: {plan:?}"),
    }
}

fn limit(limit: Limit) -> eyre::Result<LogicalPlanBuilder> {
    let Limit { input, limit } = limit;

    let Some(input) = input else {
        bail!("input must be set");
    };

    let plan = to_logical_plan(*input)?.limit(i64::from(limit), false)?; // todo: eager or no

    Ok(plan)
}
