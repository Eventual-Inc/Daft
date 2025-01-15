use std::sync::Arc;

use daft_core::prelude::Schema;
use daft_dsl::{col, LiteralValue};
use daft_logical_plan::{LogicalPlanBuilder, PyLogicalPlanBuilder};
use daft_micropartition::{
    partitioning::{
        MicroPartitionSet, PartitionCacheEntry, PartitionMetadata, PartitionSet, PartitionSetCache,
    },
    python::PyMicroPartition,
    MicroPartition,
};
use daft_table::Table;
use eyre::{bail, Context};
use futures::TryStreamExt;
use spark_connect::{
    expression::{
        sort_order::{NullOrdering, SortDirection},
        SortOrder,
    },
    relation::RelType,
    Deduplicate, Limit, Relation, ShowString, Sort,
};
use tracing::debug;

use crate::{not_yet_implemented, session::Session, util::FromOptionalField, Runner};

mod aggregate;
mod drop;
mod filter;
mod local_relation;
mod project;
mod range;
mod read;
mod to_df;
mod with_columns;
mod with_columns_renamed;

use pyo3::{intern, prelude::*};

use super::to_daft_expr;

#[derive(Clone)]
pub struct SparkAnalyzer<'a> {
    pub session: &'a Session,
}

impl SparkAnalyzer<'_> {
    pub fn new(session: &Session) -> SparkAnalyzer<'_> {
        SparkAnalyzer { session }
    }

    pub fn create_in_memory_scan(
        &self,
        plan_id: usize,
        schema: Arc<Schema>,
        tables: Vec<Table>,
    ) -> eyre::Result<LogicalPlanBuilder> {
        let runner = self.session.get_runner()?;

        match runner {
            Runner::Ray => {
                let mp =
                    MicroPartition::new_loaded(tables[0].schema.clone(), Arc::new(tables), None);
                Python::with_gil(|py| {
                    // Convert MicroPartition to a logical plan using Python interop.
                    let py_micropartition = py
                        .import(intern!(py, "daft.table"))?
                        .getattr(intern!(py, "MicroPartition"))?
                        .getattr(intern!(py, "_from_pymicropartition"))?
                        .call1((PyMicroPartition::from(mp),))?;

                    // ERROR:   2: AttributeError: 'daft.daft.PySchema' object has no attribute '_schema'
                    let py_plan_builder = py
                        .import(intern!(py, "daft.dataframe.dataframe"))?
                        .getattr(intern!(py, "to_logical_plan_builder"))?
                        .call1((py_micropartition,))?;
                    let py_plan_builder = py_plan_builder.getattr(intern!(py, "_builder"))?;
                    let plan: PyLogicalPlanBuilder = py_plan_builder.extract()?;

                    Ok::<_, eyre::Error>(dbg!(plan.builder))
                })
            }
            Runner::Native => {
                let partition_key = uuid::Uuid::new_v4().to_string();

                let pset = Arc::new(MicroPartitionSet::from_tables(plan_id, tables)?);

                let PartitionMetadata {
                    num_rows,
                    size_bytes,
                } = pset.metadata();
                let num_partitions = pset.num_partitions();

                self.session.psets.put_partition_set(&partition_key, &pset);

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
        }
    }

    pub async fn to_logical_plan(&self, relation: Relation) -> eyre::Result<LogicalPlanBuilder> {
        let Some(common) = relation.common else {
            bail!("Common metadata is required");
        };

        if common.origin.is_some() {
            debug!("Ignoring common metadata for relation: {common:?}; not yet implemented");
        }

        let Some(rel_type) = relation.rel_type else {
            bail!("Relation type is required");
        };

        match rel_type {
            RelType::Limit(l) => self.limit(*l).await,
            RelType::Range(r) => self.range(r),
            RelType::Project(p) => self.project(*p).await,
            RelType::Aggregate(a) => self.aggregate(*a).await,
            RelType::WithColumns(w) => self.with_columns(*w).await,
            RelType::ToDf(t) => self.to_df(*t).await,
            RelType::LocalRelation(l) => {
                let Some(plan_id) = common.plan_id else {
                    bail!("Plan ID is required for LocalRelation");
                };
                self.local_relation(plan_id, l)
            }
            RelType::WithColumnsRenamed(w) => self.with_columns_renamed(*w).await,
            RelType::Read(r) => read::read(r).await,
            RelType::Drop(d) => self.drop(*d).await,
            RelType::Filter(f) => self.filter(*f).await,
            RelType::ShowString(ss) => {
                let Some(plan_id) = common.plan_id else {
                    bail!("Plan ID is required for LocalRelation");
                };
                self.show_string(plan_id, *ss).await
            }
            RelType::Deduplicate(rel) => self.deduplicate(*rel).await,
            RelType::Sort(rel) => self.sort(*rel).await,
            plan => not_yet_implemented!(r#"relation type: "{}""#, rel_name(&plan))?,
        }
    }

    async fn limit(&self, limit: Limit) -> eyre::Result<LogicalPlanBuilder> {
        let Limit { input, limit } = limit;

        let Some(input) = input else {
            bail!("input must be set");
        };

        let plan = Box::pin(self.to_logical_plan(*input)).await?;

        plan.limit(i64::from(limit), false).map_err(Into::into)
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

        let results = self.session.run_query(plan).await?;
        let results = results.try_collect::<Vec<_>>().await?;
        let single_batch = results
            .into_iter()
            .next()
            .ok_or_else(|| eyre::eyre!("No results"))?;

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

    async fn deduplicate(&self, deduplicate: Deduplicate) -> eyre::Result<LogicalPlanBuilder> {
        let Deduplicate {
            input,
            column_names,
            ..
        } = deduplicate;

        if !column_names.is_empty() {
            not_yet_implemented!("Deduplicate with column names")?;
        }

        let input = input.required("input")?;

        let plan = Box::pin(self.to_logical_plan(*input)).await?;

        plan.distinct().map_err(Into::into)
    }

    async fn sort(&self, sort: Sort) -> eyre::Result<LogicalPlanBuilder> {
        let Sort {
            input,
            order,
            is_global,
        } = sort;

        let input = input.required("input")?;

        if is_global == Some(false) {
            not_yet_implemented!("Non Global sort")?;
        }

        let plan = Box::pin(self.to_logical_plan(*input)).await?;
        if order.is_empty() {
            return plan
                .sort(vec![col("*")], vec![false], vec![false])
                .map_err(Into::into);
        }
        let mut sort_by = Vec::with_capacity(order.len());
        let mut descending = Vec::with_capacity(order.len());
        let mut nulls_first = Vec::with_capacity(order.len());

        for SortOrder {
            child,
            direction,
            null_ordering,
        } in order
        {
            let expr = child.required("child")?;
            let expr = to_daft_expr(&expr)?;

            let sort_direction = SortDirection::try_from(direction)
                .wrap_err_with(|| format!("Invalid sort direction: {direction}"))?;

            let desc = match sort_direction {
                SortDirection::Ascending => false,
                SortDirection::Descending | SortDirection::Unspecified => true,
            };

            let null_ordering = NullOrdering::try_from(null_ordering)
                .wrap_err_with(|| format!("Invalid sort nulls: {null_ordering}"))?;

            let nf = match null_ordering {
                NullOrdering::SortNullsUnspecified => desc,
                NullOrdering::SortNullsFirst => true,
                NullOrdering::SortNullsLast => false,
            };

            sort_by.push(expr);
            descending.push(desc);
            nulls_first.push(nf);
        }

        plan.sort(sort_by, descending, nulls_first)
            .map_err(Into::into)
    }
}

fn rel_name(rel: &RelType) -> &str {
    match rel {
        RelType::Read(_) => "Read",
        RelType::Project(_) => "Project",
        RelType::Filter(_) => "Filter",
        RelType::Join(_) => "Join",
        RelType::SetOp(_) => "SetOp",
        RelType::Sort(_) => "Sort",
        RelType::Limit(_) => "Limit",
        RelType::Aggregate(_) => "Aggregate",
        RelType::Sql(_) => "Sql",
        RelType::LocalRelation(_) => "LocalRelation",
        RelType::Sample(_) => "Sample",
        RelType::Offset(_) => "Offset",
        RelType::Deduplicate(_) => "Deduplicate",
        RelType::Range(_) => "Range",
        RelType::SubqueryAlias(_) => "SubqueryAlias",
        RelType::Repartition(_) => "Repartition",
        RelType::ToDf(_) => "ToDf",
        RelType::WithColumnsRenamed(_) => "WithColumnsRenamed",
        RelType::ShowString(_) => "ShowString",
        RelType::Drop(_) => "Drop",
        RelType::Tail(_) => "Tail",
        RelType::WithColumns(_) => "WithColumns",
        RelType::Hint(_) => "Hint",
        RelType::Unpivot(_) => "Unpivot",
        RelType::ToSchema(_) => "ToSchema",
        RelType::RepartitionByExpression(_) => "RepartitionByExpression",
        RelType::MapPartitions(_) => "MapPartitions",
        RelType::CollectMetrics(_) => "CollectMetrics",
        RelType::Parse(_) => "Parse",
        RelType::GroupMap(_) => "GroupMap",
        RelType::CoGroupMap(_) => "CoGroupMap",
        RelType::WithWatermark(_) => "WithWatermark",
        RelType::ApplyInPandasWithState(_) => "ApplyInPandasWithState",
        RelType::HtmlString(_) => "HtmlString",
        RelType::CachedLocalRelation(_) => "CachedLocalRelation",
        RelType::CachedRemoteRelation(_) => "CachedRemoteRelation",
        RelType::CommonInlineUserDefinedTableFunction(_) => "CommonInlineUserDefinedTableFunction",
        RelType::AsOfJoin(_) => "AsOfJoin",
        RelType::CommonInlineUserDefinedDataSource(_) => "CommonInlineUserDefinedDataSource",
        RelType::WithRelations(_) => "WithRelations",
        RelType::Transpose(_) => "Transpose",
        RelType::FillNa(_) => "FillNa",
        RelType::DropNa(_) => "DropNa",
        RelType::Replace(_) => "Replace",
        RelType::Summary(_) => "Summary",
        RelType::Crosstab(_) => "Crosstab",
        RelType::Describe(_) => "Describe",
        RelType::Cov(_) => "Cov",
        RelType::Corr(_) => "Corr",
        RelType::ApproxQuantile(_) => "ApproxQuantile",
        RelType::FreqItems(_) => "FreqItems",
        RelType::SampleBy(_) => "SampleBy",
        RelType::Catalog(_) => "Catalog",
        RelType::Extension(_) => "Extension",
        RelType::Unknown(_) => "Unknown",
    }
}
