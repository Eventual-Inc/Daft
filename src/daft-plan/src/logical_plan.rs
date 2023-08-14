use std::{cmp::max, sync::Arc};

use daft_core::schema::SchemaRef;

use crate::{ops::*, PartitionScheme, PartitionSpec};

#[derive(Clone, Debug)]
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
            Self::Source(Source { schema, .. }) => schema.clone(),
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

    pub fn partition_spec(&self) -> Arc<PartitionSpec> {
        match self {
            Self::Source(Source { partition_spec, .. }) => partition_spec.clone(),
            Self::Project(Project { input, .. }) => input.partition_spec(),
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
            Self::Aggregate(Aggregate { input, .. }) => input.partition_spec(), // TODO
            Self::Concat(Concat { input, other }) => PartitionSpec::new_internal(
                PartitionScheme::Unknown,
                input.partition_spec().num_partitions + other.partition_spec().num_partitions,
                None,
            )
            .into(),
            Self::Join(Join {
                input,
                right,
                left_on,
                ..
            }) => {
                let input_partition_spec = input.partition_spec();
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

    pub fn children(&self) -> Vec<&Self> {
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
            Self::Join(Join { input, right, .. }) => vec![input, right],
            Self::Sink(Sink { input, .. }) => vec![input],
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        match self {
            Self::Source(source) => source.multiline_display(),
            Self::Project(Project { projection, .. }) => vec![format!("Project: {projection:?}")],
            Self::Filter(Filter { predicate, .. }) => vec![format!("Filter: {predicate}")],
            Self::Limit(Limit { limit, .. }) => vec![format!("Limit: {limit}")],
            Self::Explode(Explode { explode_exprs, .. }) => {
                vec![format!("Explode: {explode_exprs:?}")]
            }
            Self::Sort(sort) => sort.multiline_display(),
            Self::Repartition(repartition) => repartition.multiline_display(),
            Self::Coalesce(Coalesce { num_to, .. }) => vec![format!("Coalesce: {num_to}")],
            Self::Distinct(_) => vec!["Distinct".to_string()],
            Self::Aggregate(aggregate) => aggregate.multiline_display(),
            Self::Concat(_) => vec!["Concat".to_string()],
            Self::Join(join) => join.multiline_display(),
            Self::Sink(sink) => sink.multiline_display(),
        }
    }

    pub fn repr_ascii(&self) -> String {
        let mut s = String::new();
        crate::display::TreeDisplay::fmt_tree(self, &mut s).unwrap();
        s
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
