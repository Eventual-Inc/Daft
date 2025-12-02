use std::{
    any::Any,
    hash::{Hash, Hasher},
    num::NonZeroUsize,
    sync::Arc,
};

use common_daft_config::DaftExecutionConfig;
use common_display::ascii::AsciiTreeDisplay;
use common_error::{DaftError, DaftResult};
use common_treenode::{TreeNode, TreeNodeRecursion};
use daft_core::join::JoinSide;
use daft_dsl::{
    Column, Expr, ResolvedColumn, Subquery, SubqueryPlan, optimization::get_required_columns,
};
use daft_schema::{field::Field, schema::SchemaRef};
use indexmap::IndexSet;
use serde::{Deserialize, Serialize};
use snafu::Snafu;

pub use crate::ops::*;
use crate::stats::{PlanStats, StatsState};

/// Logical plan for a Daft query.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub enum LogicalPlan {
    Source(Source),
    Shard(Shard),
    Project(Project),
    UDFProject(UDFProject),
    Filter(Filter),
    IntoBatches(IntoBatches),
    Limit(Limit),
    Offset(Offset),
    Explode(Explode),
    Unpivot(Unpivot),
    Sort(Sort),
    Repartition(Repartition),
    Distinct(Distinct),
    Aggregate(Aggregate),
    Pivot(Pivot),
    Concat(Concat),
    Intersect(Intersect),
    Union(Union),
    Join(Join),
    Sink(Sink),
    Sample(Sample),
    MonotonicallyIncreasingId(MonotonicallyIncreasingId),
    SubqueryAlias(SubqueryAlias),
    Window(Window),
    TopN(TopN),
    VLLMProject(VLLMProject),
}

#[cfg(not(debug_assertions))]
impl std::fmt::Debug for LogicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogicalPlan")
            .field("plan_id", &self.plan_id())
            .field("node_id", &self.node_id())
            .finish()
    }
}

pub type LogicalPlanRef = Arc<LogicalPlan>;

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct SubqueryAlias {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    pub input: LogicalPlanRef,
    pub name: Arc<str>,
}

impl SubqueryAlias {
    pub fn new(input: LogicalPlanRef, name: impl Into<Arc<str>>) -> Self {
        Self {
            plan_id: None,
            node_id: None,
            input,
            name: name.into(),
        }
    }

    pub fn with_plan_id(mut self, plan_id: usize) -> Self {
        self.plan_id = Some(plan_id);
        self
    }

    pub fn with_node_id(mut self, node_id: usize) -> Self {
        self.node_id = Some(node_id);
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec![format!("Alias"), format!("name = {}", self.name)]
    }
}

/// Return value of [LogicalPlan::required_columns]
pub(crate) struct RequiredCols {
    /// Base required columns for an operator. For operators with 2 sides
    /// (e.g. join, concat), this is the required columns for the left side.
    pub(crate) required_cols: IndexSet<String>,
    /// Optional right side columns for an operator. For operators with 2 sides
    /// (e.g. join, concat), this is the optional columns for the right side.
    /// For other operators, this is None.
    pub(crate) opt_right_cols: Option<IndexSet<String>>,
}

impl RequiredCols {
    pub fn new(required_cols: IndexSet<String>, opt_right_cols: Option<IndexSet<String>>) -> Self {
        Self {
            required_cols,
            opt_right_cols,
        }
    }

    /// Unwrap and return the required (left) side columns. Intended for 1-side ops.
    pub fn single(self) -> IndexSet<String> {
        self.required_cols
    }

    /// Unwrap and return the required (left) and optional (right) side columns. Intended for 2-side ops.
    pub fn double(self) -> (IndexSet<String>, IndexSet<String>) {
        if let Some(opt_right_cols) = self.opt_right_cols {
            (self.required_cols, opt_right_cols)
        } else {
            panic!("Expected 2-side plan node (e.g. join, concat), but got 1-side plan node")
        }
    }
}

impl LogicalPlan {
    pub fn arced(self) -> Arc<Self> {
        Arc::new(self)
    }

    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Source(Source { output_schema, .. }) => output_schema.clone(),
            Self::Shard(Shard { input, .. }) => input.schema(),
            Self::Project(Project {
                projected_schema, ..
            }) => projected_schema.clone(),
            Self::UDFProject(UDFProject {
                projected_schema, ..
            }) => projected_schema.clone(),
            Self::Filter(Filter { input, .. }) => input.schema(),
            Self::IntoBatches(IntoBatches { input, .. }) => input.schema(),
            Self::Limit(Limit { input, .. }) => input.schema(),
            Self::Offset(Offset { input, .. }) => input.schema(),
            Self::Explode(Explode {
                exploded_schema, ..
            }) => exploded_schema.clone(),
            Self::Unpivot(Unpivot { output_schema, .. }) => output_schema.clone(),
            Self::Sort(Sort { input, .. }) => input.schema(),
            Self::Repartition(Repartition { input, .. }) => input.schema(),
            Self::Distinct(Distinct { input, .. }) => input.schema(),
            Self::Aggregate(Aggregate { output_schema, .. }) => output_schema.clone(),
            Self::Pivot(Pivot { output_schema, .. }) => output_schema.clone(),
            Self::Concat(Concat { input, .. }) => input.schema(),
            Self::Intersect(Intersect { lhs, .. }) => lhs.schema(),
            Self::Union(Union { lhs, .. }) => lhs.schema(),
            Self::Join(Join { output_schema, .. }) => output_schema.clone(),
            Self::Sink(Sink { schema, .. }) => schema.clone(),
            Self::Sample(Sample { input, .. }) => input.schema(),
            Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId { schema, .. }) => {
                schema.clone()
            }
            Self::SubqueryAlias(SubqueryAlias { input, .. }) => input.schema(),
            Self::Window(Window { schema, .. }) => schema.clone(),
            Self::TopN(TopN { input, .. }) => input.schema(),
            Self::VLLMProject(VLLMProject { output_schema, .. }) => output_schema.clone(),
        }
    }

    pub(crate) fn required_columns(&self) -> RequiredCols {
        // TODO: https://github.com/Eventual-Inc/Daft/pull/1288#discussion_r1307820697
        match self {
            Self::Shard(..)
            | Self::Limit(..)
            | Self::IntoBatches(..)
            | Self::Offset(..)
            | Self::Sample(..)
            | Self::MonotonicallyIncreasingId(..) => RequiredCols::new(IndexSet::new(), None),
            Self::Concat(..) => RequiredCols::new(IndexSet::new(), Some(IndexSet::new())),
            Self::Project(projection) => {
                let res = projection
                    .projection
                    .iter()
                    .flat_map(get_required_columns)
                    .collect();
                RequiredCols::new(res, None)
            }
            Self::UDFProject(UDFProject {
                expr,
                passthrough_columns,
                ..
            }) => {
                let mut res = passthrough_columns
                    .iter()
                    .flat_map(get_required_columns)
                    .collect::<IndexSet<_>>();

                res.extend(get_required_columns(expr).into_iter());
                RequiredCols::new(res, None)
            }
            Self::Filter(filter) => RequiredCols::new(
                get_required_columns(&filter.predicate)
                    .iter()
                    .cloned()
                    .collect(),
                None,
            ),
            Self::Sort(sort) => {
                let res = sort.sort_by.iter().flat_map(get_required_columns).collect();
                RequiredCols::new(res, None)
            }
            Self::Repartition(repartition) => {
                let res = repartition
                    .repartition_spec
                    .repartition_by()
                    .iter()
                    .flat_map(get_required_columns)
                    .collect();
                RequiredCols::new(res, None)
            }
            Self::Explode(explode) => {
                let res = explode
                    .to_explode
                    .iter()
                    .flat_map(get_required_columns)
                    .collect();
                RequiredCols::new(res, None)
            }
            Self::Unpivot(unpivot) => {
                let res = unpivot
                    .ids
                    .iter()
                    .chain(unpivot.values.iter())
                    .flat_map(get_required_columns)
                    .collect();
                RequiredCols::new(res, None)
            }
            Self::Distinct(distinct) => {
                if let Some(on) = &distinct.columns {
                    let res = on.iter().flat_map(get_required_columns).collect();
                    RequiredCols::new(res, None)
                } else {
                    let res = distinct
                        .input
                        .schema()
                        .field_names()
                        .map(ToString::to_string)
                        .collect();
                    RequiredCols::new(res, None)
                }
            }
            Self::Aggregate(aggregate) => {
                let res = aggregate
                    .aggregations
                    .iter()
                    .flat_map(|agg| agg.children())
                    .flat_map(|e| get_required_columns(&e))
                    .chain(aggregate.groupby.iter().flat_map(get_required_columns))
                    .collect();
                RequiredCols::new(res, None)
            }
            Self::Pivot(pivot) => {
                let res = pivot
                    .group_by
                    .iter()
                    .chain(std::iter::once(&pivot.pivot_column))
                    .chain(std::iter::once(&pivot.value_column))
                    .flat_map(get_required_columns)
                    .collect();
                RequiredCols::new(res, None)
            }
            Self::Join(join) => {
                let mut left = IndexSet::new();
                let mut right = IndexSet::new();

                if let Some(pred) = join.on.inner() {
                    pred.apply(|e| {
                        match e.as_ref() {
                            Expr::Column(Column::Resolved(ResolvedColumn::JoinSide(
                                Field { name, .. },
                                JoinSide::Left,
                            ))) => {
                                left.insert(name.clone());
                            }
                            Expr::Column(Column::Resolved(ResolvedColumn::JoinSide(
                                Field { name, .. },
                                JoinSide::Right,
                            ))) => {
                                right.insert(name.clone());
                            }
                            _ => {}
                        }

                        Ok(TreeNodeRecursion::Continue)
                    })
                    .unwrap();
                }
                RequiredCols::new(left, Some(right))
            }
            Self::Intersect(_) => RequiredCols::new(IndexSet::new(), Some(IndexSet::new())),
            Self::Union(_) => RequiredCols::new(IndexSet::new(), Some(IndexSet::new())),
            Self::Source(_) => todo!(),
            Self::Sink(_) => todo!(),
            Self::SubqueryAlias(SubqueryAlias { input, .. }) => input.required_columns(),
            Self::Window(window) => {
                let res = window
                    .window_spec
                    .partition_by
                    .iter()
                    .chain(window.window_spec.order_by.iter())
                    .flat_map(get_required_columns)
                    .collect();
                RequiredCols::new(res, None)
            }
            Self::TopN(top_n) => {
                let res = top_n
                    .sort_by
                    .iter()
                    .flat_map(get_required_columns)
                    .collect();
                RequiredCols::new(res, None)
            }
            Self::VLLMProject(VLLMProject { expr, .. }) => RequiredCols::new(
                get_required_columns(&expr.input).into_iter().collect(),
                None,
            ),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Source(..) => "Source",
            Self::Shard(..) => "Shard",
            Self::Project(..) => "Project",
            Self::UDFProject(..) => "UDFProject",
            Self::Filter(..) => "Filter",
            Self::IntoBatches(..) => "IntoBatches",
            Self::Limit(..) => "Limit",
            Self::Offset(..) => "Offset",
            Self::Explode(..) => "Explode",
            Self::Unpivot(..) => "Unpivot",
            Self::Sort(..) => "Sort",
            Self::Repartition(..) => "Repartition",
            Self::Distinct(..) => "Distinct",
            Self::Aggregate(..) => "Aggregate",
            Self::Pivot(..) => "Pivot",
            Self::Concat(..) => "Concat",
            Self::Join(..) => "Join",
            Self::Intersect(..) => "Intersect",
            Self::Union(..) => "Union",
            Self::Sink(..) => "Sink",
            Self::Sample(..) => "Sample",
            Self::MonotonicallyIncreasingId(..) => "MonotonicallyIncreasingId",
            Self::SubqueryAlias(..) => "Alias",
            Self::Window(..) => "Window",
            Self::TopN(..) => "TopN",
            Self::VLLMProject(..) => "VLLMProject",
        }
    }

    pub fn stats_state(&self) -> &StatsState {
        match self {
            Self::Source(Source { stats_state, .. })
            | Self::Shard(Shard { stats_state, .. })
            | Self::Project(Project { stats_state, .. })
            | Self::UDFProject(UDFProject { stats_state, .. })
            | Self::Filter(Filter { stats_state, .. })
            | Self::IntoBatches(IntoBatches { stats_state, .. })
            | Self::Limit(Limit { stats_state, .. })
            | Self::Offset(Offset { stats_state, .. })
            | Self::Explode(Explode { stats_state, .. })
            | Self::Unpivot(Unpivot { stats_state, .. })
            | Self::Sort(Sort { stats_state, .. })
            | Self::Repartition(Repartition { stats_state, .. })
            | Self::Distinct(Distinct { stats_state, .. })
            | Self::Aggregate(Aggregate { stats_state, .. })
            | Self::Pivot(Pivot { stats_state, .. })
            | Self::Concat(Concat { stats_state, .. })
            | Self::Join(Join { stats_state, .. })
            | Self::Sink(Sink { stats_state, .. })
            | Self::Sample(Sample { stats_state, .. })
            | Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId { stats_state, .. })
            | Self::Window(Window { stats_state, .. })
            | Self::TopN(TopN { stats_state, .. })
            | Self::VLLMProject(VLLMProject { stats_state, .. }) => stats_state,
            Self::Intersect(_) | Self::Union(_) | Self::SubqueryAlias(_) => {
                panic!(
                    "{} nodes should be optimized away before stats are materialized",
                    self.name()
                )
            }
        }
    }

    pub fn materialized_stats(&self) -> &PlanStats {
        self.stats_state().materialized_stats()
    }

    // Materializes stats over logical plans. If stats are already materialized, this function recomputes stats, which might be
    // useful if stats become stale during query planning.
    pub fn with_materialized_stats(self, cfg: &DaftExecutionConfig) -> Self {
        match self {
            Self::Source(plan) => Self::Source(plan.with_materialized_stats(cfg)),
            Self::Shard(plan) => Self::Shard(plan.with_materialized_stats()),
            Self::Project(plan) => Self::Project(plan.with_materialized_stats()),
            Self::UDFProject(plan) => Self::UDFProject(plan.with_materialized_stats()),
            Self::Filter(plan) => Self::Filter(plan.with_materialized_stats()),
            Self::IntoBatches(plan) => Self::IntoBatches(plan.with_materialized_stats()),
            Self::Limit(plan) => Self::Limit(plan.with_materialized_stats()),
            Self::Offset(plan) => Self::Offset(plan.with_materialized_stats()),
            Self::Explode(plan) => Self::Explode(plan.with_materialized_stats()),
            Self::Unpivot(plan) => Self::Unpivot(plan.with_materialized_stats()),
            Self::Sort(plan) => Self::Sort(plan.with_materialized_stats()),
            Self::Repartition(plan) => Self::Repartition(plan.with_materialized_stats()),
            Self::Distinct(plan) => Self::Distinct(plan.with_materialized_stats()),
            Self::Aggregate(plan) => Self::Aggregate(plan.with_materialized_stats()),
            Self::Pivot(plan) => Self::Pivot(plan.with_materialized_stats()),
            Self::Concat(plan) => Self::Concat(plan.with_materialized_stats()),
            Self::Join(plan) => Self::Join(plan.with_materialized_stats()),
            Self::Sink(plan) => Self::Sink(plan.with_materialized_stats()),
            Self::Sample(plan) => Self::Sample(plan.with_materialized_stats()),
            Self::MonotonicallyIncreasingId(plan) => {
                Self::MonotonicallyIncreasingId(plan.with_materialized_stats())
            }
            Self::Window(plan) => Self::Window(plan.with_materialized_stats()),
            Self::TopN(plan) => Self::TopN(plan.with_materialized_stats()),
            Self::VLLMProject(plan) => Self::VLLMProject(plan.with_materialized_stats()),
            Self::Intersect(_) | Self::Union(_) | Self::SubqueryAlias(_) => {
                panic!(
                    "{} should be optimized away before stats are derived",
                    self.name()
                )
            }
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        match self {
            Self::Source(source) => source.multiline_display(),
            Self::Shard(shard) => shard.multiline_display(),
            Self::Project(projection) => projection.multiline_display(),
            Self::UDFProject(projection) => projection.multiline_display(),
            Self::Filter(filter) => filter.multiline_display(),
            Self::IntoBatches(into_batches) => into_batches.multiline_display(),
            Self::Limit(limit) => limit.multiline_display(),
            Self::Offset(offset) => offset.multiline_display(),
            Self::Explode(explode) => explode.multiline_display(),
            Self::Unpivot(unpivot) => unpivot.multiline_display(),
            Self::Sort(sort) => sort.multiline_display(),
            Self::Repartition(repartition) => repartition.multiline_display(),
            Self::Distinct(distinct) => distinct.multiline_display(),
            Self::Aggregate(aggregate) => aggregate.multiline_display(),
            Self::Pivot(pivot) => pivot.multiline_display(),
            Self::Concat(concat) => concat.multiline_display(),
            Self::Intersect(inner) => inner.multiline_display(),
            Self::Union(inner) => inner.multiline_display(),
            Self::Join(join) => join.multiline_display(),
            Self::Sink(sink) => sink.multiline_display(),
            Self::Sample(sample) => sample.multiline_display(),
            Self::MonotonicallyIncreasingId(monotonically_increasing_id) => {
                monotonically_increasing_id.multiline_display()
            }
            Self::SubqueryAlias(alias) => alias.multiline_display(),
            Self::Window(window) => window.multiline_display(),
            Self::TopN(top_n) => top_n.multiline_display(),
            Self::VLLMProject(vllm_project) => vllm_project.multiline_display(),
        }
    }

    pub fn children(&self) -> Vec<&Self> {
        match self {
            Self::Source(..) => vec![],
            Self::Shard(Shard { input, .. }) => vec![input],
            Self::Project(Project { input, .. }) => vec![input],
            Self::UDFProject(UDFProject { input, .. }) => vec![input],
            Self::Filter(Filter { input, .. }) => vec![input],
            Self::IntoBatches(IntoBatches { input, .. }) => vec![input],
            Self::Limit(Limit { input, .. }) => vec![input],
            Self::Offset(Offset { input, .. }) => vec![input],
            Self::Explode(Explode { input, .. }) => vec![input],
            Self::Unpivot(Unpivot { input, .. }) => vec![input],
            Self::Sort(Sort { input, .. }) => vec![input],
            Self::Repartition(Repartition { input, .. }) => vec![input],
            Self::Distinct(Distinct { input, .. }) => vec![input],
            Self::Aggregate(Aggregate { input, .. }) => vec![input],
            Self::Pivot(Pivot { input, .. }) => vec![input],
            Self::Concat(Concat { input, other, .. }) => vec![input, other],
            Self::Join(Join { left, right, .. }) => vec![left, right],
            Self::Sink(Sink { input, .. }) => vec![input],
            Self::Intersect(Intersect { lhs, rhs, .. }) => vec![lhs, rhs],
            Self::Union(Union { lhs, rhs, .. }) => vec![lhs, rhs],
            Self::Sample(Sample { input, .. }) => vec![input],
            Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId { input, .. }) => {
                vec![input]
            }
            Self::SubqueryAlias(SubqueryAlias { input, .. }) => vec![input],
            Self::Window(Window { input, .. }) => vec![input],
            Self::TopN(TopN { input, .. }) => vec![input],
            Self::VLLMProject(VLLMProject { input, .. }) => vec![input],
        }
    }

    pub fn with_new_children(&self, children: &[Arc<Self>]) -> Self {
        match children {
            [input] => match self {
                Self::Source(_) => panic!(
                    "Source nodes don't have children, with_new_children() should never be called for Source ops"
                ),
                Self::Shard(Shard { sharder, .. }) => {
                    Self::Shard(Shard::new(input.clone(), sharder.clone()))
                }
                Self::Project(Project { projection, .. }) => {
                    Self::Project(Project::try_new(input.clone(), projection.clone()).unwrap())
                }
                Self::UDFProject(UDFProject {
                    expr,
                    passthrough_columns,
                    ..
                }) => Self::UDFProject(
                    UDFProject::try_new(input.clone(), expr.clone(), passthrough_columns.clone())
                        .unwrap(),
                ),
                Self::Filter(Filter { predicate, .. }) => {
                    Self::Filter(Filter::try_new(input.clone(), predicate.clone()).unwrap())
                }
                Self::IntoBatches(IntoBatches { batch_size, .. }) => {
                    Self::IntoBatches(IntoBatches::new(input.clone(), *batch_size))
                }
                Self::Limit(Limit {
                    limit,
                    offset,
                    eager,
                    ..
                }) => Self::Limit(Limit::new(input.clone(), *limit, *offset, *eager)),
                Self::Offset(Offset { offset, .. }) => {
                    Self::Offset(Offset::new(input.clone(), *offset))
                }
                Self::Explode(Explode { to_explode, .. }) => {
                    Self::Explode(Explode::try_new(input.clone(), to_explode.clone()).unwrap())
                }
                Self::Sort(Sort {
                    sort_by,
                    descending,
                    nulls_first,
                    ..
                }) => Self::Sort(
                    Sort::try_new(
                        input.clone(),
                        sort_by.clone(),
                        descending.clone(),
                        nulls_first.clone(),
                    )
                    .unwrap(),
                ),
                Self::Repartition(Repartition {
                    repartition_spec: scheme_config,
                    ..
                }) => Self::Repartition(Repartition::new(input.clone(), scheme_config.clone())),
                Self::Distinct(distinct) => {
                    Self::Distinct(Distinct::new(input.clone(), distinct.columns.clone()))
                }
                Self::Aggregate(Aggregate {
                    aggregations,
                    groupby,
                    ..
                }) => Self::Aggregate(
                    Aggregate::try_new(input.clone(), aggregations.clone(), groupby.clone())
                        .unwrap(),
                ),
                Self::Pivot(Pivot {
                    group_by,
                    pivot_column,
                    value_column,
                    aggregation,
                    names,
                    ..
                }) => Self::Pivot(
                    Pivot::try_new(
                        input.clone(),
                        group_by.clone(),
                        pivot_column.clone(),
                        value_column.clone(),
                        aggregation.into(),
                        names.clone(),
                    )
                    .unwrap(),
                ),
                Self::Sink(Sink { sink_info, .. }) => {
                    Self::Sink(Sink::try_new(input.clone(), sink_info.clone()).unwrap())
                }
                Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId {
                    column_name,
                    starting_offset,
                    ..
                }) => Self::MonotonicallyIncreasingId(
                    MonotonicallyIncreasingId::try_new(
                        input.clone(),
                        Some(column_name),
                        *starting_offset,
                    )
                    .unwrap(),
                ),
                Self::Unpivot(Unpivot {
                    ids,
                    values,
                    variable_name,
                    value_name,
                    output_schema,
                    ..
                }) => Self::Unpivot(Unpivot::new(
                    input.clone(),
                    ids.clone(),
                    values.clone(),
                    variable_name.clone(),
                    value_name.clone(),
                    output_schema.clone(),
                )),

                Self::Sample(Sample {
                    fraction,
                    size,
                    with_replacement,
                    seed,
                    ..
                }) => Self::Sample(Sample::new(
                    input.clone(),
                    *fraction,
                    *size,
                    *with_replacement,
                    *seed,
                )),
                Self::SubqueryAlias(SubqueryAlias { name: id, .. }) => {
                    Self::SubqueryAlias(SubqueryAlias::new(input.clone(), id.clone()))
                }
                Self::Window(Window {
                    window_functions,
                    aliases,
                    window_spec,
                    ..
                }) => Self::Window(
                    Window::try_new(
                        input.clone(),
                        window_functions.clone(),
                        aliases.clone(),
                        window_spec.clone(),
                    )
                    .unwrap(),
                ),
                Self::TopN(TopN {
                    sort_by,
                    descending,
                    nulls_first,
                    limit,
                    offset,
                    ..
                }) => Self::TopN(
                    TopN::try_new(
                        input.clone(),
                        sort_by.clone(),
                        descending.clone(),
                        nulls_first.clone(),
                        *limit,
                        *offset,
                    )
                    .unwrap(),
                ),
                Self::VLLMProject(VLLMProject {
                    expr,
                    output_column_name,
                    ..
                }) => Self::VLLMProject(VLLMProject::new(
                    input.clone(),
                    expr.clone(),
                    output_column_name.clone(),
                )),
                Self::Concat(_) | Self::Intersect(_) | Self::Union(_) | Self::Join(_) => panic!(
                    "{} ops should never have only one input, but got one",
                    input.name()
                ),
            },
            [input1, input2] => match self {
                Self::Source(_) => panic!(
                    "Source nodes don't have children, with_new_children() should never be called for Source ops"
                ),
                Self::Concat(_) => {
                    Self::Concat(Concat::try_new(input1.clone(), input2.clone()).unwrap())
                }
                Self::Intersect(inner) => Self::Intersect(
                    Intersect::try_new(input1.clone(), input2.clone(), inner.is_all).unwrap(),
                ),
                Self::Union(inner) => Self::Union(
                    Union::try_new(
                        input1.clone(),
                        input2.clone(),
                        inner.quantifier,
                        inner.strategy,
                    )
                    .unwrap(),
                ),
                Self::Join(Join {
                    on,
                    join_type,
                    join_strategy,
                    ..
                }) => Self::Join(
                    Join::try_new(
                        input1.clone(),
                        input2.clone(),
                        on.clone(),
                        *join_type,
                        *join_strategy,
                    )
                    .unwrap(),
                ),
                _ => panic!("Logical op {} has one input, but got two", self),
            },
            _ => panic!(
                "Logical ops should never have more than 2 inputs, but got: {}",
                children.len()
            ),
        }
    }

    /// Get the number of nodes in the logical plan tree.
    pub fn node_count(&self) -> NonZeroUsize {
        match self.children().as_slice() {
            [] => 1usize.try_into().unwrap(),
            [input] => input.node_count().checked_add(1usize).unwrap(),
            [input1, input2] => input1
                .node_count()
                .checked_add(input2.node_count().checked_add(1usize).unwrap().into())
                .unwrap(),
            children => panic!(
                "Logical ops should never have more than 2 inputs, but got: {}",
                children.len()
            ),
        }
    }

    pub fn repr_ascii(&self, simple: bool) -> String {
        let mut s = String::new();
        self.fmt_tree(&mut s, simple).unwrap();
        s
    }

    pub fn repr_indent(&self) -> String {
        let mut s = String::new();
        self.fmt_tree_indent_style(0, &mut s).unwrap();
        s
    }

    pub fn get_aliases(self: Arc<Self>) -> Vec<Arc<str>> {
        use common_treenode::TreeNode;

        let mut names = Vec::new();

        self.apply(|node| {
            if let Self::SubqueryAlias(SubqueryAlias { name, .. }) = node.as_ref() {
                names.push(name.clone());
                Ok(TreeNodeRecursion::Jump)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })
        .unwrap();

        names
    }

    pub fn get_schema_for_alias(self: &Arc<Self>, alias: &str) -> DaftResult<Option<SchemaRef>> {
        use common_treenode::TreeNode;

        let mut schema = None;

        self.apply(|node| {
            if let Self::SubqueryAlias(SubqueryAlias { name, .. }) = node.as_ref() {
                if name.as_ref() == alias {
                    if schema.is_some() {
                        return Err(DaftError::ValueError(format!(
                            "Plan must not have duplicate aliases in the same scope, found: {alias}"
                        )));
                    }

                    schema = Some(node.schema());
                }

                Ok(TreeNodeRecursion::Jump)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })?;

        Ok(schema)
    }

    pub fn get_schema_for_id(self: &Arc<Self>, id: usize) -> DaftResult<Option<SchemaRef>> {
        use common_treenode::TreeNode;

        let mut schema = None;

        self.apply(|node| {
            if let Some(plan_id) = node.plan_id() {
                if plan_id == &id {
                    if schema.is_some() {
                        return Err(DaftError::ValueError(format!(
                            "Plan must not have duplicate plan ids in the same scope, found: {id}"
                        )));
                    }

                    schema = Some(node.schema());

                    Ok(TreeNodeRecursion::Jump)
                } else {
                    Ok(TreeNodeRecursion::Continue)
                }
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })?;

        Ok(schema)
    }

    pub fn plan_id(&self) -> &Option<usize> {
        match self {
            Self::Source(Source { plan_id, .. })
            | Self::Shard(Shard { plan_id, .. })
            | Self::Project(Project { plan_id, .. })
            | Self::UDFProject(UDFProject { plan_id, .. })
            | Self::Filter(Filter { plan_id, .. })
            | Self::IntoBatches(IntoBatches { plan_id, .. })
            | Self::Limit(Limit { plan_id, .. })
            | Self::Offset(Offset { plan_id, .. })
            | Self::Explode(Explode { plan_id, .. })
            | Self::Unpivot(Unpivot { plan_id, .. })
            | Self::Sort(Sort { plan_id, .. })
            | Self::Repartition(Repartition { plan_id, .. })
            | Self::Distinct(Distinct { plan_id, .. })
            | Self::Aggregate(Aggregate { plan_id, .. })
            | Self::Pivot(Pivot { plan_id, .. })
            | Self::Concat(Concat { plan_id, .. })
            | Self::Intersect(Intersect { plan_id, .. })
            | Self::Union(Union { plan_id, .. })
            | Self::Join(Join { plan_id, .. })
            | Self::Sink(Sink { plan_id, .. })
            | Self::Sample(Sample { plan_id, .. })
            | Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId { plan_id, .. })
            | Self::SubqueryAlias(SubqueryAlias { plan_id, .. })
            | Self::Window(Window { plan_id, .. })
            | Self::TopN(TopN { plan_id, .. })
            | Self::VLLMProject(VLLMProject { plan_id, .. }) => plan_id,
        }
    }

    pub fn node_id(&self) -> &Option<usize> {
        match self {
            Self::Source(Source { node_id, .. })
            | Self::Shard(Shard { node_id, .. })
            | Self::Project(Project { node_id, .. })
            | Self::UDFProject(UDFProject { node_id, .. })
            | Self::Filter(Filter { node_id, .. })
            | Self::IntoBatches(IntoBatches { node_id, .. })
            | Self::Limit(Limit { node_id, .. })
            | Self::Offset(Offset { node_id, .. })
            | Self::Explode(Explode { node_id, .. })
            | Self::Unpivot(Unpivot { node_id, .. })
            | Self::Sort(Sort { node_id, .. })
            | Self::Repartition(Repartition { node_id, .. })
            | Self::Distinct(Distinct { node_id, .. })
            | Self::Aggregate(Aggregate { node_id, .. })
            | Self::Pivot(Pivot { node_id, .. })
            | Self::Concat(Concat { node_id, .. })
            | Self::Intersect(Intersect { node_id, .. })
            | Self::Union(Union { node_id, .. })
            | Self::Join(Join { node_id, .. })
            | Self::Sink(Sink { node_id, .. })
            | Self::Sample(Sample { node_id, .. })
            | Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId { node_id, .. })
            | Self::SubqueryAlias(SubqueryAlias { node_id, .. })
            | Self::Window(Window { node_id, .. })
            | Self::TopN(TopN { node_id, .. })
            | Self::VLLMProject(VLLMProject { node_id, .. }) => node_id,
        }
    }

    pub fn with_plan_id(self: Arc<Self>, plan_id: usize) -> Self {
        let this = Arc::unwrap_or_clone(self);
        match this {
            Self::Source(source) => Self::Source(source.with_plan_id(plan_id)),
            Self::Shard(shard) => Self::Shard(shard.with_plan_id(plan_id)),
            Self::Project(project) => Self::Project(project.with_plan_id(plan_id)),
            Self::UDFProject(project) => Self::UDFProject(project.with_plan_id(plan_id)),
            Self::Filter(filter) => Self::Filter(filter.with_plan_id(plan_id)),
            Self::IntoBatches(into_batches) => {
                Self::IntoBatches(into_batches.with_plan_id(plan_id))
            }
            Self::Limit(limit) => Self::Limit(limit.with_plan_id(plan_id)),
            Self::Offset(offset) => Self::Offset(offset.with_plan_id(plan_id)),
            Self::Explode(explode) => Self::Explode(explode.with_plan_id(plan_id)),
            Self::Unpivot(unpivot) => Self::Unpivot(unpivot.with_plan_id(plan_id)),
            Self::Sort(sort) => Self::Sort(sort.with_plan_id(plan_id)),
            Self::Repartition(repartition) => Self::Repartition(repartition.with_plan_id(plan_id)),
            Self::Distinct(distinct) => Self::Distinct(distinct.with_plan_id(plan_id)),
            Self::Aggregate(aggregate) => Self::Aggregate(aggregate.with_plan_id(plan_id)),
            Self::Pivot(pivot) => Self::Pivot(pivot.with_plan_id(plan_id)),
            Self::Concat(concat) => Self::Concat(concat.with_plan_id(plan_id)),
            Self::Intersect(intersect) => Self::Intersect(intersect.with_plan_id(plan_id)),
            Self::Union(union) => Self::Union(union.with_plan_id(plan_id)),
            Self::Join(join) => Self::Join(join.with_plan_id(plan_id)),
            Self::Sink(sink) => Self::Sink(sink.with_plan_id(plan_id)),
            Self::Sample(sample) => Self::Sample(sample.with_plan_id(plan_id)),
            Self::MonotonicallyIncreasingId(monotonically_increasing_id) => {
                Self::MonotonicallyIncreasingId(monotonically_increasing_id.with_plan_id(plan_id))
            }
            Self::SubqueryAlias(alias) => Self::SubqueryAlias(alias.with_plan_id(plan_id)),
            Self::Window(window) => Self::Window(window.with_plan_id(plan_id)),
            Self::TopN(top_n) => Self::TopN(top_n.with_plan_id(plan_id)),
            Self::VLLMProject(vllm_project) => {
                Self::VLLMProject(vllm_project.with_plan_id(plan_id))
            }
        }
    }

    pub fn with_node_id(self: Arc<Self>, node_id: usize) -> Self {
        let this = Arc::unwrap_or_clone(self);
        match this {
            Self::Source(source) => Self::Source(source.with_node_id(node_id)),
            Self::Shard(shard) => Self::Shard(shard.with_node_id(node_id)),
            Self::Project(project) => Self::Project(project.with_node_id(node_id)),
            Self::UDFProject(project) => Self::UDFProject(project.with_node_id(node_id)),
            Self::Filter(filter) => Self::Filter(filter.with_node_id(node_id)),
            Self::IntoBatches(into_batches) => {
                Self::IntoBatches(into_batches.with_node_id(node_id))
            }
            Self::Limit(limit) => Self::Limit(limit.with_node_id(node_id)),
            Self::Offset(offset) => Self::Offset(offset.with_node_id(node_id)),
            Self::Explode(explode) => Self::Explode(explode.with_node_id(node_id)),
            Self::Unpivot(unpivot) => Self::Unpivot(unpivot.with_node_id(node_id)),
            Self::Sort(sort) => Self::Sort(sort.with_node_id(node_id)),
            Self::Repartition(repartition) => Self::Repartition(repartition.with_node_id(node_id)),
            Self::Distinct(distinct) => Self::Distinct(distinct.with_node_id(node_id)),
            Self::Aggregate(aggregate) => Self::Aggregate(aggregate.with_node_id(node_id)),
            Self::Pivot(pivot) => Self::Pivot(pivot.with_node_id(node_id)),
            Self::Concat(concat) => Self::Concat(concat.with_node_id(node_id)),
            Self::Intersect(intersect) => Self::Intersect(intersect.with_node_id(node_id)),
            Self::Union(union) => Self::Union(union.with_node_id(node_id)),
            Self::Join(join) => Self::Join(join.with_node_id(node_id)),
            Self::Sink(sink) => Self::Sink(sink.with_node_id(node_id)),
            Self::Sample(sample) => Self::Sample(sample.with_node_id(node_id)),
            Self::MonotonicallyIncreasingId(monotonically_increasing_id) => {
                Self::MonotonicallyIncreasingId(monotonically_increasing_id.with_node_id(node_id))
            }
            Self::SubqueryAlias(alias) => Self::SubqueryAlias(alias.with_node_id(node_id)),
            Self::Window(window) => Self::Window(window.with_node_id(node_id)),
            Self::TopN(top_n) => Self::TopN(top_n.with_node_id(node_id)),
            Self::VLLMProject(vllm_project) => {
                Self::VLLMProject(vllm_project.with_node_id(node_id))
            }
        }
    }
}

impl SubqueryPlan for LogicalPlan {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }

    fn name(&self) -> &'static str {
        Self::name(self)
    }

    fn schema(&self) -> SchemaRef {
        Self::schema(self)
    }

    fn dyn_eq(&self, other: &dyn SubqueryPlan) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .is_some_and(|other| self == other)
    }

    fn dyn_hash(&self, mut state: &mut dyn Hasher) {
        self.hash(&mut state);
    }
}

pub(crate) fn downcast_subquery(subquery: &Subquery) -> LogicalPlanRef {
    subquery
        .plan
        .clone()
        .as_any_arc()
        .downcast::<LogicalPlan>()
        .expect("subquery plan should be a LogicalPlan")
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub(crate) enum Error {
    #[snafu(display("Unable to create logical plan node.\nDue to: {}", source))]
    CreationError { source: DaftError },
}
pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for DaftError {
    fn from(err: Error) -> Self {
        Self::External(err.into())
    }
}
impl From<DaftError> for Error {
    fn from(err: DaftError) -> Self {
        Self::CreationError { source: err }
    }
}

#[cfg(feature = "python")]
impl std::convert::From<Error> for pyo3::PyErr {
    fn from(value: Error) -> Self {
        let daft_error: DaftError = value.into();
        daft_error.into()
    }
}

macro_rules! impl_from_data_struct_for_logical_plan {
    ($name:ident) => {
        impl From<$name> for LogicalPlan {
            fn from(data: $name) -> Self {
                Self::$name(data)
            }
        }

        impl From<$name> for Arc<LogicalPlan> {
            fn from(data: $name) -> Self {
                Self::new(LogicalPlan::$name(data))
            }
        }
    };
}

impl_from_data_struct_for_logical_plan!(Source);
impl_from_data_struct_for_logical_plan!(Shard);
impl_from_data_struct_for_logical_plan!(Project);
impl_from_data_struct_for_logical_plan!(Filter);
impl_from_data_struct_for_logical_plan!(IntoBatches);
impl_from_data_struct_for_logical_plan!(Limit);
impl_from_data_struct_for_logical_plan!(Offset);
impl_from_data_struct_for_logical_plan!(Explode);
impl_from_data_struct_for_logical_plan!(Unpivot);
impl_from_data_struct_for_logical_plan!(Sort);
impl_from_data_struct_for_logical_plan!(Repartition);
impl_from_data_struct_for_logical_plan!(Distinct);
impl_from_data_struct_for_logical_plan!(Aggregate);
impl_from_data_struct_for_logical_plan!(Pivot);
impl_from_data_struct_for_logical_plan!(Concat);
impl_from_data_struct_for_logical_plan!(Intersect);
impl_from_data_struct_for_logical_plan!(Union);
impl_from_data_struct_for_logical_plan!(Join);
impl_from_data_struct_for_logical_plan!(Sink);
impl_from_data_struct_for_logical_plan!(Sample);
impl_from_data_struct_for_logical_plan!(MonotonicallyIncreasingId);
impl_from_data_struct_for_logical_plan!(Window);
impl_from_data_struct_for_logical_plan!(TopN);
