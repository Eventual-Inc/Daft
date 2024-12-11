use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::partitioning::InMemoryPartitionSetCache;
use eyre::{bail, Context};
use spark_connect::{relation::RelType, Limit, Relation};
use tracing::warn;

use crate::translation::logical_plan::{
    aggregate::aggregate, local_relation::local_relation, project::project, range::range,
    to_df::to_df, with_columns::with_columns,
};

mod aggregate;
mod local_relation;
mod project;
mod range;
mod to_df;
mod with_columns;

pub fn to_logical_plan(
    relation: Relation,
    pset_cache: &InMemoryPartitionSetCache,
) -> eyre::Result<LogicalPlanBuilder> {
    if let Some(common) = relation.common {
        if common.origin.is_some() {
            warn!("Ignoring common metadata for relation: {common:?}; not yet implemented");
        }
    };

    let Some(rel_type) = relation.rel_type else {
        bail!("Relation type is required");
    };

    match rel_type {
        RelType::Limit(l) => {
            limit(*l, pset_cache).wrap_err("Failed to apply limit to logical plan")
        }
        RelType::Range(r) => range(r).wrap_err("Failed to apply range to logical plan"),
        RelType::Project(p) => {
            project(*p, pset_cache).wrap_err("Failed to apply project to logical plan")
        }
        RelType::Aggregate(a) => {
            aggregate(*a, pset_cache).wrap_err("Failed to apply aggregate to logical plan")
        }
        RelType::WithColumns(w) => {
            with_columns(*w, pset_cache).wrap_err("Failed to apply with_columns to logical plan")
        }
        RelType::ToDf(t) => to_df(*t, pset_cache).wrap_err("Failed to apply to_df to logical plan"),
        RelType::LocalRelation(l) => {
            local_relation(l, pset_cache).wrap_err("Failed to apply local_relation to logical plan")
        }
        plan => bail!("Unsupported relation type: {plan:?}"),
    }
}

fn limit(limit: Limit, pset_cache: &InMemoryPartitionSetCache) -> eyre::Result<LogicalPlanBuilder> {
    let Limit { input, limit } = limit;

    let Some(input) = input else {
        bail!("input must be set");
    };

    to_logical_plan(*input, pset_cache)?
        .limit(i64::from(limit), false)
        .wrap_err("Failed to apply limit to logical plan")
    // todo: eager or no
}
