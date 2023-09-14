use std::{cmp::max, num::NonZeroUsize, sync::Arc};

use common_error::DaftError;
use daft_core::schema::SchemaRef;
use daft_dsl::{optimization::get_required_columns, Expr};
use indexmap::IndexSet;
use snafu::Snafu;

use crate::{display::TreeDisplay, logical_ops::*, PartitionScheme, PartitionSpec};

/// Logical plan for a Daft query.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogicalPlan {
    Source(Source),
    Project(Project),
    Filter(Filter),
    Limit(Limit),
    Explode(Explode),
    Sort(Sort),
    Repartition(Repartition),
    Coalesce(Coalesce),
    Distinct(Distinct),
    Aggregate(Aggregate),
    Concat(Concat),
    Join(Join),
    Sink(Sink),
}

impl LogicalPlan {
    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Source(Source { output_schema, .. }) => output_schema.clone(),
            Self::Project(Project {
                projected_schema, ..
            }) => projected_schema.clone(),
            Self::Filter(Filter { input, .. }) => input.schema(),
            Self::Limit(Limit { input, .. }) => input.schema(),
            Self::Explode(Explode {
                exploded_schema, ..
            }) => exploded_schema.clone(),
            Self::Sort(Sort { input, .. }) => input.schema(),
            Self::Repartition(Repartition { input, .. }) => input.schema(),
            Self::Coalesce(Coalesce { input, .. }) => input.schema(),
            Self::Distinct(Distinct { input, .. }) => input.schema(),
            Self::Aggregate(aggregate) => aggregate.schema(),
            Self::Concat(Concat { input, .. }) => input.schema(),
            Self::Join(Join { output_schema, .. }) => output_schema.clone(),
            Self::Sink(Sink { schema, .. }) => schema.clone(),
        }
    }

    pub fn required_columns(&self) -> Vec<IndexSet<String>> {
        // TODO: https://github.com/Eventual-Inc/Daft/pull/1288#discussion_r1307820697
        match self {
            Self::Limit(..) | Self::Coalesce(..) => vec![IndexSet::new()],
            Self::Concat(..) => vec![IndexSet::new(), IndexSet::new()],
            Self::Project(projection) => {
                let res = projection
                    .projection
                    .iter()
                    .flat_map(get_required_columns)
                    .collect();
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
                    .partition_by
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
                    .map(|agg| get_required_columns(&Expr::Agg(agg.clone())))
                    .chain(aggregate.groupby.iter().map(get_required_columns))
                    .flatten()
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
            Self::Source(_) => todo!(),
            Self::Sink(_) => todo!(),
        }
    }

    pub fn partition_spec(&self) -> Arc<PartitionSpec> {
        match self {
            Self::Source(Source { partition_spec, .. }) => partition_spec.clone(),
            Self::Project(Project { partition_spec, .. }) => partition_spec.clone(),
            Self::Filter(Filter { input, .. }) => input.partition_spec(),
            Self::Limit(Limit { input, .. }) => input.partition_spec(),
            Self::Explode(Explode { input, .. }) => input.partition_spec(),
            Self::Sort(Sort { input, sort_by, .. }) => PartitionSpec::new_internal(
                PartitionScheme::Range,
                input.partition_spec().num_partitions,
                Some(sort_by.clone()),
            )
            .into(),
            Self::Repartition(Repartition {
                num_partitions,
                partition_by,
                scheme,
                ..
            }) => PartitionSpec::new_internal(
                scheme.clone(),
                *num_partitions,
                Some(partition_by.clone()),
            )
            .into(),
            Self::Coalesce(Coalesce { num_to, .. }) => {
                PartitionSpec::new_internal(PartitionScheme::Unknown, *num_to, None).into()
            }
            Self::Distinct(Distinct { input, .. }) => input.partition_spec(),
            Self::Aggregate(Aggregate { input, groupby, .. }) => {
                let input_partition_spec = input.partition_spec();
                if input_partition_spec.num_partitions == 1 {
                    input_partition_spec.clone()
                } else if groupby.is_empty() {
                    PartitionSpec::new_internal(PartitionScheme::Unknown, 1, None).into()
                } else {
                    PartitionSpec::new_internal(
                        PartitionScheme::Hash,
                        input.partition_spec().num_partitions,
                        Some(groupby.clone()),
                    )
                    .into()
                }
            }
            Self::Concat(Concat { input, other }) => PartitionSpec::new_internal(
                PartitionScheme::Unknown,
                input.partition_spec().num_partitions + other.partition_spec().num_partitions,
                None,
            )
            .into(),
            Self::Join(Join {
                left,
                right,
                left_on,
                ..
            }) => {
                let input_partition_spec = left.partition_spec();
                match max(
                    input_partition_spec.num_partitions,
                    right.partition_spec().num_partitions,
                ) {
                    // NOTE: This duplicates the repartitioning logic in the planner, where we
                    // conditionally repartition the left and right tables.
                    // TODO(Clark): Consolidate this logic with the planner logic when we push the partition spec
                    // to be an entirely planner-side concept.
                    1 => input_partition_spec,
                    num_partitions => PartitionSpec::new_internal(
                        PartitionScheme::Hash,
                        num_partitions,
                        Some(left_on.clone()),
                    )
                    .into(),
                }
            }
            Self::Sink(Sink { input, .. }) => input.partition_spec(),
        }
    }

    pub fn children(&self) -> Vec<&Arc<Self>> {
        match self {
            Self::Source(..) => vec![],
            Self::Project(Project { input, .. }) => vec![input],
            Self::Filter(Filter { input, .. }) => vec![input],
            Self::Limit(Limit { input, .. }) => vec![input],
            Self::Explode(Explode { input, .. }) => vec![input],
            Self::Sort(Sort { input, .. }) => vec![input],
            Self::Repartition(Repartition { input, .. }) => vec![input],
            Self::Coalesce(Coalesce { input, .. }) => vec![input],
            Self::Distinct(Distinct { input, .. }) => vec![input],
            Self::Aggregate(Aggregate { input, .. }) => vec![input],
            Self::Concat(Concat { input, other }) => vec![input, other],
            Self::Join(Join { left, right, .. }) => vec![left, right],
            Self::Sink(Sink { input, .. }) => vec![input],
        }
    }

    pub fn with_new_children(&self, children: &[Arc<LogicalPlan>]) -> Arc<LogicalPlan> {
        let new_plan = match children {
            [input] => match self {
                Self::Source(_) => panic!("Source nodes don't have children, with_new_children() should never be called for Source ops"),
                Self::Project(Project { projection, resource_request, .. }) => Self::Project(Project::try_new(
                    input.clone(), projection.clone(), resource_request.clone(),
                ).unwrap()),
                Self::Filter(Filter { predicate, .. }) => Self::Filter(Filter::try_new(input.clone(), predicate.clone()).unwrap()),
                Self::Limit(Limit { limit, .. }) => Self::Limit(Limit::new(input.clone(), *limit)),
                Self::Explode(Explode { to_explode, .. }) => Self::Explode(Explode::try_new(input.clone(), to_explode.clone()).unwrap()),
                Self::Sort(Sort { sort_by, descending, .. }) => Self::Sort(Sort::try_new(input.clone(), sort_by.clone(), descending.clone()).unwrap()),
                Self::Repartition(Repartition { num_partitions, partition_by, scheme, .. }) => Self::Repartition(Repartition::new(input.clone(), *num_partitions, partition_by.clone(), scheme.clone())),
                Self::Coalesce(Coalesce { num_to, .. }) => Self::Coalesce(Coalesce::new(input.clone(), *num_to)),
                Self::Distinct(_) => Self::Distinct(Distinct::new(input.clone())),
                Self::Aggregate(Aggregate { aggregations, groupby, ..}) => Self::Aggregate(Aggregate::try_new(input.clone(), aggregations.clone(), groupby.clone()).unwrap()),
                Self::Sink(Sink { schema, sink_info, .. }) => Self::Sink(Sink::new(input.clone(), schema.clone(), sink_info.clone())),
                _ => panic!("Logical op {} has two inputs, but got one", self),
            },
            [input1, input2] => match self {
                Self::Source(_) => panic!("Source nodes don't have children, with_new_children() should never be called for Source ops"),
                Self::Concat(_) => Self::Concat(Concat::try_new(input1.clone(), input2.clone()).unwrap()),
                Self::Join(Join { left_on, right_on, join_type, .. }) => Self::Join(Join::try_new(input1.clone(), input2.clone(), left_on.clone(), right_on.clone(), *join_type).unwrap()),
                _ => panic!("Logical op {} has one input, but got two", self),
            },
            _ => panic!("Logical ops should never have more than 2 inputs, but got: {}", children.len())
        };
        new_plan.into()
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
    pub fn name(&self) -> String {
        let name = match self {
            Self::Source(..) => "Source",
            Self::Project(..) => "Project",
            Self::Filter(..) => "Filter",
            Self::Limit(..) => "Limit",
            Self::Explode(..) => "Explode",
            Self::Sort(..) => "Sort",
            Self::Repartition(..) => "Repartition",
            Self::Coalesce(..) => "Coalesce",
            Self::Distinct(..) => "Distinct",
            Self::Aggregate(..) => "Aggregate",
            Self::Concat(..) => "Concat",
            Self::Join(..) => "Join",
            Self::Sink(..) => "Sink",
        };
        name.to_string()
    }

    pub fn multiline_display(&self) -> Vec<String> {
        match self {
            Self::Source(source) => source.multiline_display(),
            Self::Project(projection) => projection.multiline_display(),
            Self::Filter(Filter { predicate, .. }) => vec![format!("Filter: {predicate}")],
            Self::Limit(Limit { limit, .. }) => vec![format!("Limit: {limit}")],
            Self::Explode(Explode { to_explode, .. }) => {
                vec![format!(
                    "Explode: {}",
                    to_explode
                        .iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )]
            }
            Self::Sort(sort) => sort.multiline_display(),
            Self::Repartition(repartition) => repartition.multiline_display(),
            Self::Coalesce(Coalesce { num_to, .. }) => vec![format!("Coalesce: To = {num_to}")],
            Self::Distinct(_) => vec!["Distinct".to_string()],
            Self::Aggregate(aggregate) => aggregate.multiline_display(),
            Self::Concat(_) => vec!["Concat".to_string()],
            Self::Join(join) => join.multiline_display(),
            Self::Sink(sink) => sink.multiline_display(),
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

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub(crate) enum Error {
    #[snafu(display("Unable to create logical plan node.\nDue to: {}", source))]
    CreationError { source: DaftError },
}
pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for DaftError {
    fn from(err: Error) -> DaftError {
        DaftError::External(err.into())
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
    };
}

impl_from_data_struct_for_logical_plan!(Source);
impl_from_data_struct_for_logical_plan!(Project);
impl_from_data_struct_for_logical_plan!(Filter);
impl_from_data_struct_for_logical_plan!(Limit);
impl_from_data_struct_for_logical_plan!(Explode);
impl_from_data_struct_for_logical_plan!(Sort);
impl_from_data_struct_for_logical_plan!(Repartition);
impl_from_data_struct_for_logical_plan!(Coalesce);
impl_from_data_struct_for_logical_plan!(Distinct);
impl_from_data_struct_for_logical_plan!(Aggregate);
impl_from_data_struct_for_logical_plan!(Concat);
impl_from_data_struct_for_logical_plan!(Join);
impl_from_data_struct_for_logical_plan!(Sink);
