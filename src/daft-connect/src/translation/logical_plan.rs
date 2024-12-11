use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::partitioning::InMemoryPartitionSet;
use eyre::{bail, Context};
use spark_connect::{relation::RelType, Limit, Relation};
use tracing::warn;

use crate::translation::logical_plan::{
    aggregate::aggregate, drop::drop, filter::filter, local_relation::local_relation,
    project::project, range::range, read::read, to_df::to_df, with_columns::with_columns,
};

mod aggregate;
mod drop;
mod filter;
mod local_relation;
mod project;
mod range;
mod read;
mod to_df;
mod with_columns;

pub struct Plan {
    pub builder: LogicalPlanBuilder,
    pub psets: InMemoryPartitionSet,
}

impl Plan {
    pub fn new(builder: LogicalPlanBuilder) -> Self {
        Self {
            builder,
            psets: InMemoryPartitionSet::default(),
        }
    }
}

impl From<LogicalPlanBuilder> for Plan {
    fn from(builder: LogicalPlanBuilder) -> Self {
        Self {
            builder,
            psets: InMemoryPartitionSet::default(),
        }
    }
}

pub async fn to_logical_plan(relation: Relation) -> eyre::Result<Plan> {
    if let Some(common) = relation.common {
        if common.origin.is_some() {
            warn!("Ignoring common metadata for relation: {common:?}; not yet implemented");
        }
    };

    let Some(rel_type) = relation.rel_type else {
        bail!("Relation type is required");
    };

    match rel_type {
        RelType::Limit(l) => limit(*l)
            .await
            .wrap_err("Failed to apply limit to logical plan"),
        RelType::Range(r) => range(r).wrap_err("Failed to apply range to logical plan"),
        RelType::Project(p) => project(*p)
            .await
            .wrap_err("Failed to apply project to logical plan"),
        RelType::Filter(f) => filter(*f)
            .await
            .wrap_err("Failed to apply filter to logical plan"),
        RelType::Aggregate(a) => aggregate(*a)
            .await
            .wrap_err("Failed to apply aggregate to logical plan"),
        RelType::WithColumns(w) => with_columns(*w)
            .await
            .wrap_err("Failed to apply with_columns to logical plan"),
        RelType::ToDf(t) => to_df(*t)
            .await
            .wrap_err("Failed to apply to_df to logical plan"),
        RelType::LocalRelation(l) => {
            local_relation(l).wrap_err("Failed to apply local_relation to logical plan")
        }
        RelType::Read(r) => read(r)
            .await
            .wrap_err("Failed to apply read to logical plan"),
        RelType::Drop(d) => drop(*d)
            .await
            .wrap_err("Failed to apply drop to logical plan"),
        plan => bail!("Unsupported relation type: {plan:?}"),
    }
}

async fn limit(limit: Limit) -> eyre::Result<Plan> {
    let Limit { input, limit } = limit;

    let Some(input) = input else {
        bail!("input must be set");
    };

    let mut plan = Box::pin(to_logical_plan(*input)).await?;
    plan.builder = plan.builder.limit(i64::from(limit), false)?; // todo: eager or no

    Ok(plan)
}
