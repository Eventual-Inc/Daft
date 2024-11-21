use std::{num::NonZeroUsize, sync::Arc};

use common_display::ascii::AsciiTreeDisplay;
use common_error::DaftError;
use daft_dsl::{optimization::get_required_columns, SubqueryPlan};
use daft_schema::schema::SchemaRef;
use indexmap::IndexSet;
use snafu::Snafu;

pub use crate::ops::*;

/// Logical plan for a Daft query.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogicalPlan {
    Source(Source),
    Project(Project),
    ActorPoolProject(ActorPoolProject),
    Filter(Filter),
    Limit(Limit),
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
}

pub type LogicalPlanRef = Arc<LogicalPlan>;

impl LogicalPlan {
    pub fn arced(self) -> Arc<Self> {
        Arc::new(self)
    }
    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Source(Source { output_schema, .. }) => output_schema.clone(),
            Self::Project(Project {
                projected_schema, ..
            }) => projected_schema.clone(),
            Self::ActorPoolProject(ActorPoolProject {
                projected_schema, ..
            }) => projected_schema.clone(),
            Self::Filter(Filter { input, .. }) => input.schema(),
            Self::Limit(Limit { input, .. }) => input.schema(),
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
        }
    }

    pub fn required_columns(&self) -> Vec<IndexSet<String>> {
        // TODO: https://github.com/Eventual-Inc/Daft/pull/1288#discussion_r1307820697
        match self {
            Self::Limit(..) => vec![IndexSet::new()],
            Self::Sample(..) => vec![IndexSet::new()],
            Self::MonotonicallyIncreasingId(..) => vec![IndexSet::new()],
            Self::Concat(..) => vec![IndexSet::new(), IndexSet::new()],
            Self::Project(projection) => {
                let res = projection
                    .projection
                    .iter()
                    .flat_map(get_required_columns)
                    .collect();
                vec![res]
            }
            Self::ActorPoolProject(ActorPoolProject { projection, .. }) => {
                let res = projection.iter().flat_map(get_required_columns).collect();
                vec![res]
            }
            Self::Filter(filter) => {
                vec![get_required_columns(&filter.predicate)
                    .iter()
                    .cloned()
                    .collect()]
            }
            Self::Sort(sort) => {
                let res = sort.sort_by.iter().flat_map(get_required_columns).collect();
                vec![res]
            }
            Self::Repartition(repartition) => {
                let res = repartition
                    .repartition_spec
                    .repartition_by()
                    .iter()
                    .flat_map(get_required_columns)
                    .collect();
                vec![res]
            }
            Self::Explode(explode) => {
                let res = explode
                    .to_explode
                    .iter()
                    .flat_map(get_required_columns)
                    .collect();
                vec![res]
            }
            Self::Unpivot(unpivot) => {
                let res = unpivot
                    .ids
                    .iter()
                    .chain(unpivot.values.iter())
                    .flat_map(get_required_columns)
                    .collect();
                vec![res]
            }
            Self::Distinct(distinct) => {
                let res = distinct
                    .input
                    .schema()
                    .fields
                    .iter()
                    .map(|(name, _)| name)
                    .cloned()
                    .collect();
                vec![res]
            }
            Self::Aggregate(aggregate) => {
                let res = aggregate
                    .aggregations
                    .iter()
                    .flat_map(|agg| agg.children())
                    .flat_map(|e| get_required_columns(&e))
                    .chain(aggregate.groupby.iter().flat_map(get_required_columns))
                    .collect();
                vec![res]
            }
            Self::Pivot(pivot) => {
                let res = pivot
                    .group_by
                    .iter()
                    .chain(std::iter::once(&pivot.pivot_column))
                    .chain(std::iter::once(&pivot.value_column))
                    .flat_map(get_required_columns)
                    .collect();
                vec![res]
            }
            Self::Join(join) => {
                let left = join.left_on.iter().flat_map(get_required_columns).collect();
                let right = join
                    .right_on
                    .iter()
                    .flat_map(get_required_columns)
                    .collect();
                vec![left, right]
            }
            Self::Intersect(_) => vec![IndexSet::new(), IndexSet::new()],
            Self::Union(_) => vec![IndexSet::new(), IndexSet::new()],
            Self::Source(_) => todo!(),
            Self::Sink(_) => todo!(),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Source(..) => "Source",
            Self::Project(..) => "Project",
            Self::ActorPoolProject(..) => "ActorPoolProject",
            Self::Filter(..) => "Filter",
            Self::Limit(..) => "Limit",
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
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        match self {
            Self::Source(source) => source.multiline_display(),
            Self::Project(projection) => projection.multiline_display(),
            Self::ActorPoolProject(projection) => projection.multiline_display(),
            Self::Filter(Filter { predicate, .. }) => vec![format!("Filter: {predicate}")],
            Self::Limit(Limit { limit, .. }) => vec![format!("Limit: {limit}")],
            Self::Explode(explode) => explode.multiline_display(),
            Self::Unpivot(unpivot) => unpivot.multiline_display(),
            Self::Sort(sort) => sort.multiline_display(),
            Self::Repartition(repartition) => repartition.multiline_display(),
            Self::Distinct(_) => vec!["Distinct".to_string()],
            Self::Aggregate(aggregate) => aggregate.multiline_display(),
            Self::Pivot(pivot) => pivot.multiline_display(),
            Self::Concat(_) => vec!["Concat".to_string()],
            Self::Intersect(inner) => inner.multiline_display(),
            Self::Union(inner) => inner.multiline_display(),
            Self::Join(join) => join.multiline_display(),
            Self::Sink(sink) => sink.multiline_display(),
            Self::Sample(sample) => {
                vec![format!("Sample: {fraction}", fraction = sample.fraction)]
            }
            Self::MonotonicallyIncreasingId(_) => vec!["MonotonicallyIncreasingId".to_string()],
        }
    }

    pub fn children(&self) -> Vec<&Self> {
        match self {
            Self::Source(..) => vec![],
            Self::Project(Project { input, .. }) => vec![input],
            Self::ActorPoolProject(ActorPoolProject { input, .. }) => vec![input],
            Self::Filter(Filter { input, .. }) => vec![input],
            Self::Limit(Limit { input, .. }) => vec![input],
            Self::Explode(Explode { input, .. }) => vec![input],
            Self::Unpivot(Unpivot { input, .. }) => vec![input],
            Self::Sort(Sort { input, .. }) => vec![input],
            Self::Repartition(Repartition { input, .. }) => vec![input],
            Self::Distinct(Distinct { input, .. }) => vec![input],
            Self::Aggregate(Aggregate { input, .. }) => vec![input],
            Self::Pivot(Pivot { input, .. }) => vec![input],
            Self::Concat(Concat { input, other }) => vec![input, other],
            Self::Join(Join { left, right, .. }) => vec![left, right],
            Self::Sink(Sink { input, .. }) => vec![input],
            Self::Intersect(Intersect { lhs, rhs, .. }) => vec![lhs, rhs],
            Self::Union(Union { lhs, rhs, .. }) => vec![lhs, rhs],
            Self::Sample(Sample { input, .. }) => vec![input],
            Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId { input, .. }) => {
                vec![input]
            }
        }
    }

    pub fn with_new_children(&self, children: &[Arc<Self>]) -> Self {
        match children {
            [input] => match self {
                Self::Source(_) => panic!("Source nodes don't have children, with_new_children() should never be called for Source ops"),
                Self::Project(Project { projection, .. }) => Self::Project(Project::try_new(
                    input.clone(), projection.clone(),
                ).unwrap()),
                Self::ActorPoolProject(ActorPoolProject {projection, ..}) => Self::ActorPoolProject(ActorPoolProject::try_new(input.clone(), projection.clone()).unwrap()),
                Self::Filter(Filter { predicate, .. }) => Self::Filter(Filter::try_new(input.clone(), predicate.clone()).unwrap()),
                Self::Limit(Limit { limit, eager, .. }) => Self::Limit(Limit::new(input.clone(), *limit, *eager)),
                Self::Explode(Explode { to_explode, .. }) => Self::Explode(Explode::try_new(input.clone(), to_explode.clone()).unwrap()),
                Self::Sort(Sort { sort_by, descending, nulls_first, .. }) => Self::Sort(Sort::try_new(input.clone(), sort_by.clone(), descending.clone(), nulls_first.clone()).unwrap()),
                Self::Repartition(Repartition {  repartition_spec: scheme_config, .. }) => Self::Repartition(Repartition::try_new(input.clone(), scheme_config.clone()).unwrap()),
                Self::Distinct(_) => Self::Distinct(Distinct::new(input.clone())),
                Self::Aggregate(Aggregate { aggregations, groupby, ..}) => Self::Aggregate(Aggregate::try_new(input.clone(), aggregations.clone(), groupby.clone()).unwrap()),
                Self::Pivot(Pivot { group_by, pivot_column, value_column, aggregation, names, ..}) => Self::Pivot(Pivot::try_new(input.clone(), group_by.clone(), pivot_column.clone(), value_column.clone(), aggregation.into(), names.clone()).unwrap()),
                Self::Sink(Sink { sink_info, .. }) => Self::Sink(Sink::try_new(input.clone(), sink_info.clone()).unwrap()),
                Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId {column_name, .. }) => Self::MonotonicallyIncreasingId(MonotonicallyIncreasingId::new(input.clone(), Some(column_name))),
                Self::Unpivot(Unpivot {ids, values, variable_name, value_name, output_schema, ..}) => Self::Unpivot(Unpivot { input: input.clone(), ids: ids.clone(), values: values.clone(), variable_name: variable_name.clone(), value_name: value_name.clone(), output_schema: output_schema.clone() }),
                Self::Sample(Sample {fraction, with_replacement, seed, ..}) => Self::Sample(Sample::new(input.clone(), *fraction, *with_replacement, *seed)),
                Self::Concat(_) => panic!("Concat ops should never have only one input, but got one"),
                Self::Intersect(_) => panic!("Intersect ops should never have only one input, but got one"),
                Self::Union(_) => panic!("Union ops should never have only one input, but got one"),
                Self::Join(_) => panic!("Join ops should never have only one input, but got one"),
            },
            [input1, input2] => match self {
                Self::Source(_) => panic!("Source nodes don't have children, with_new_children() should never be called for Source ops"),
                Self::Concat(_) => Self::Concat(Concat::try_new(input1.clone(), input2.clone()).unwrap()),
                Self::Intersect(inner) => Self::Intersect(Intersect::try_new(input1.clone(), input2.clone(), inner.is_all).unwrap()),
                Self::Union(inner) => Self::Union(Union::try_new(input1.clone(), input2.clone(), inner.is_all).unwrap()),
                Self::Join(Join { left_on, right_on, null_equals_nulls, join_type, join_strategy, .. }) => Self::Join(Join::try_new(
                    input1.clone(),
                    input2.clone(),
                    left_on.clone(),
                    right_on.clone(),
                    null_equals_nulls.clone(),
                    *join_type,
                    *join_strategy,
                    None,  // The suffix is already eagerly computed in the constructor
                    None,  // the prefix is already eagerly computed in the constructor
                    false // this is already eagerly computed in the constructor
                ).unwrap()),
                _ => panic!("Logical op {} has one input, but got two", self),
            },
            _ => panic!("Logical ops should never have more than 2 inputs, but got: {}", children.len())
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
}

impl SubqueryPlan for LogicalPlan {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        Self::name(self)
    }

    fn schema(&self) -> SchemaRef {
        Self::schema(self)
    }
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
                Arc::new(LogicalPlan::$name(data))
            }
        }
    };
}

impl_from_data_struct_for_logical_plan!(Source);
impl_from_data_struct_for_logical_plan!(Project);
impl_from_data_struct_for_logical_plan!(Filter);
impl_from_data_struct_for_logical_plan!(Limit);
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
