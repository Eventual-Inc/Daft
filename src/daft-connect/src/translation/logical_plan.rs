use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use daft_core::prelude::Schema;
use daft_dsl::LiteralValue;
use daft_local_execution::NativeExecutor;
use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::{
    partitioning::{
        InMemoryPartitionSetCache, MicroPartitionSet, PartitionCacheEntry, PartitionMetadata,
        PartitionSet, PartitionSetCache,
    },
    MicroPartition,
};
use daft_table::Table;
use eyre::{bail, Context};
use futures::TryStreamExt;
use spark_connect::{relation::RelType, Limit, Relation, ShowString};
use tracing::warn;

mod aggregate;
mod drop;
mod filter;
mod local_relation;
mod project;
mod range;
mod read;
mod to_df;
mod with_columns;

pub struct SparkAnalyzer<'a> {
    pub psets: &'a InMemoryPartitionSetCache,
}

impl SparkAnalyzer<'_> {
    pub fn new(pset: &InMemoryPartitionSetCache) -> SparkAnalyzer {
        SparkAnalyzer { psets: pset }
    }
    pub fn create_in_memory_scan(
        &self,
        plan_id: usize,
        schema: Arc<Schema>,
        tables: Vec<Table>,
    ) -> eyre::Result<LogicalPlanBuilder> {
        let partition_key = uuid::Uuid::new_v4().to_string();

        let pset = Arc::new(MicroPartitionSet::from_tables(plan_id, tables)?);

        let PartitionMetadata {
            num_rows,
            size_bytes,
        } = pset.metadata();
        let num_partitions = pset.num_partitions();

        self.psets.put_partition_set(&partition_key, &pset);

        let cache_entry = PartitionCacheEntry::new_rust(partition_key.clone(), pset);

        Ok(LogicalPlanBuilder::in_memory_scan(
            &partition_key,
            cache_entry,
            schema,
            num_partitions,
            size_bytes,
            num_rows,
        )?)
    }

    pub async fn to_logical_plan(&self, relation: Relation) -> eyre::Result<LogicalPlanBuilder> {
        let Some(common) = relation.common else {
            bail!("Common metadata is required");
        };

        if common.origin.is_some() {
            warn!("Ignoring common metadata for relation: {common:?}; not yet implemented");
        }

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
            RelType::LocalRelation(l) => {
                let Some(plan_id) = common.plan_id else {
                    bail!("Plan ID is required for LocalRelation");
                };
                self.local_relation(plan_id, l)
                    .wrap_err("Failed to apply local_relation to logical plan")
            }
            RelType::Read(r) => read::read(r)
                .await
                .wrap_err("Failed to apply read to logical plan"),
            RelType::Drop(d) => self
                .drop(*d)
                .await
                .wrap_err("Failed to apply drop to logical plan"),
            RelType::Filter(f) => self
                .filter(*f)
                .await
                .wrap_err("Failed to apply filter to logical plan"),
            RelType::ShowString(ss) => {
                let Some(plan_id) = common.plan_id else {
                    bail!("Plan ID is required for LocalRelation");
                };
                self.show_string(plan_id, *ss)
                    .await
                    .wrap_err("Failed to show string")
            }
            plan => bail!("Unsupported relation type: {plan:?}"),
        }
    }

    async fn limit(&self, limit: Limit) -> eyre::Result<LogicalPlanBuilder> {
        let Limit { input, limit } = limit;

        let Some(input) = input else {
            bail!("input must be set");
        };

        let plan = Box::pin(self.to_logical_plan(*input)).await?;

        plan.limit(i64::from(limit), false)
            .wrap_err("Failed to apply limit to logical plan")
    }

    /// right now this just naively applies a limit to the logical plan
    /// In the future, we want this to more closely match our daft implementation
    async fn show_string(
        &self,
        plan_id: i64,
        show_string: ShowString,
    ) -> eyre::Result<LogicalPlanBuilder> {
        let ShowString {
            input,
            num_rows,
            truncate: _,
            vertical,
        } = show_string;

        if vertical {
            bail!("Vertical show string is not supported");
        }

        let Some(input) = input else {
            bail!("input must be set");
        };

        let plan = Box::pin(self.to_logical_plan(*input)).await?;
        let plan = plan.limit(num_rows as i64, true)?;

        let optimized_plan = tokio::task::spawn_blocking(move || plan.optimize())
            .await
            .unwrap()?;

        let cfg = Arc::new(DaftExecutionConfig::default());
        let native_executor = NativeExecutor::from_logical_plan_builder(&optimized_plan)?;
        let result_stream = native_executor.run(self.psets, cfg, None)?.into_stream();
        let batch = result_stream.try_collect::<Vec<_>>().await?;
        let single_batch = MicroPartition::concat(batch)?;
        let tbls = single_batch.get_tables()?;
        let tbl = Table::concat(&tbls)?;
        let output = tbl.to_comfy_table(None).to_string();

        let s = LiteralValue::Utf8(output)
            .into_single_value_series()?
            .rename("show_string");

        let tbl = Table::from_nonempty_columns(vec![s])?;
        let schema = tbl.schema.clone();

        self.create_in_memory_scan(plan_id as _, schema, vec![tbl])
    }
}
