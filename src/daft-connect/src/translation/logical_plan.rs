use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::{partitioning::InMemoryPartitionSetCache, MicroPartition};
use eyre::{bail, Context};
use spark_connect::{relation::RelType, Limit, Relation};
use tracing::warn;

mod aggregate;
mod local_relation;
mod project;
mod range;
mod read;
mod to_df;
mod with_columns;

pub struct Translator<'a> {
    pub pset_cache: &'a InMemoryPartitionSetCache<MicroPartition>,
}
impl Translator<'_> {
    pub fn new(pset_cache: &InMemoryPartitionSetCache<MicroPartition>) -> Translator {
        Translator { pset_cache }
    }

    pub async fn to_logical_plan(&self, relation: Relation) -> eyre::Result<LogicalPlanBuilder> {
        if let Some(common) = relation.common {
            if common.origin.is_some() {
                warn!("Ignoring common metadata for relation: {common:?}; not yet implemented");
            }
        };

        let Some(rel_type) = relation.rel_type else {
            bail!("Relation type is required");
        };

        match rel_type {
            RelType::Limit(l) => self
                .limit(*l)
                .await
                .wrap_err("Failed to apply limit to logical plan"),
            RelType::Range(r) => self
                .range(r)
                .wrap_err("Failed to apply range to logical plan"),
            RelType::Project(p) => self
                .project(*p)
                .await
                .wrap_err("Failed to apply project to logical plan"),
            RelType::Aggregate(a) => self
                .aggregate(*a)
                .await
                .wrap_err("Failed to apply aggregate to logical plan"),
            RelType::WithColumns(w) => self
                .with_columns(*w)
                .await
                .wrap_err("Failed to apply with_columns to logical plan"),
            RelType::ToDf(t) => self
                .to_df(*t)
                .await
                .wrap_err("Failed to apply to_df to logical plan"),
            RelType::LocalRelation(l) => self
                .local_relation(l)
                .wrap_err("Failed to apply local_relation to logical plan"),
            RelType::Read(r) => read::read(r)
                .await
                .wrap_err("Failed to apply read to logical plan"),
            plan => bail!("Unsupported relation type: {plan:?}"),
        }
    }
}

impl Translator<'_> {
    async fn limit(&self, limit: Limit) -> eyre::Result<LogicalPlanBuilder> {
        let Limit { input, limit } = limit;

        let Some(input) = input else {
            bail!("input must be set");
        };

        let plan = Box::pin(self.to_logical_plan(*input)).await?;

        plan.limit(i64::from(limit), false)
            .wrap_err("Failed to apply limit to logical plan")
    }
}
