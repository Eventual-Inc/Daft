use std::sync::Arc;

use daft_core::schema::SchemaRef;

use crate::{ops::*, PartitionScheme, PartitionSpec};

#[derive(Clone, Debug)]
pub enum LogicalPlan {
    Source(Source),
    Filter(Filter),
    Limit(Limit),
    Sort(Sort),
    Repartition(Repartition),
    Aggregate(Aggregate),
}

impl LogicalPlan {
    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Source(Source { schema, .. }) => schema.clone(),
            Self::Filter(Filter { input, .. }) => input.schema(),
            Self::Limit(Limit { input, .. }) => input.schema(),
            Self::Sort(Sort { input, .. }) => input.schema(),
            Self::Repartition(Repartition { input, .. }) => input.schema(),
            Self::Aggregate(Aggregate { schema, .. }) => schema.clone(),
        }
    }

    pub fn partition_spec(&self) -> Arc<PartitionSpec> {
        match self {
            Self::Source(Source { partition_spec, .. }) => partition_spec.clone(),
            Self::Filter(Filter { input, .. }) => input.partition_spec(),
            Self::Limit(Limit { input, .. }) => input.partition_spec(),
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
            Self::Aggregate(Aggregate { input, .. }) => input.partition_spec(), // TODO
        }
    }

    pub fn children(&self) -> Vec<&Self> {
        match self {
            Self::Source(..) => vec![],
            Self::Filter(Filter { input, .. }) => vec![input],
            Self::Limit(Limit { input, .. }) => vec![input],
            Self::Sort(Sort { input, .. }) => vec![input],
            Self::Repartition(Repartition { input, .. }) => vec![input],
            Self::Aggregate(Aggregate { input, .. }) => vec![input],
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        match self {
            Self::Source(source) => source.multiline_display(),
            Self::Filter(Filter { predicate, .. }) => vec![format!("Filter: {predicate}")],
            Self::Limit(Limit { limit, .. }) => vec![format!("Limit: {limit}")],
            Self::Sort(sort) => sort.multiline_display(),
            Self::Repartition(repartition) => repartition.multiline_display(),
            Self::Aggregate(aggregate) => aggregate.multiline_display(),
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
impl_from_data_struct_for_logical_plan!(Filter);
impl_from_data_struct_for_logical_plan!(Limit);
impl_from_data_struct_for_logical_plan!(Sort);
impl_from_data_struct_for_logical_plan!(Repartition);
impl_from_data_struct_for_logical_plan!(Aggregate);
