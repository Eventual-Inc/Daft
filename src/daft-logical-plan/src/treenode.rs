use std::sync::Arc;

use common_error::DaftResult;
use common_scan_info::{PhysicalScanInfo, Pushdowns, ScanState};
use common_treenode::{DynTreeNode, Transformed, TreeNodeIterator};
use daft_core::prelude::SchemaRef;
use daft_dsl::ExprRef;

use crate::{
    ops::Source,
    partitioning::{HashRepartitionConfig, RepartitionSpec},
    LogicalPlan, SourceInfo,
};

impl DynTreeNode for LogicalPlan {
    fn arc_children(&self) -> Vec<Arc<Self>> {
        self.children()
            .into_iter()
            .map(|c| Arc::new(c.clone()))
            .collect()
    }

    fn with_new_arc_children(self: Arc<Self>, children: Vec<Arc<Self>>) -> DaftResult<Arc<Self>> {
        let old_children = self.arc_children();
        if children.len() != old_children.len() {
            panic!("LogicalPlan::with_new_arc_children: Wrong number of children")
        } else if children.is_empty()
            || children
                .iter()
                .zip(old_children.iter())
                .any(|(c1, c2)| !Arc::ptr_eq(c1, c2))
        {
            Ok(self.with_new_children(&children).arced())
        } else {
            Ok(self)
        }
    }
}

impl LogicalPlan {
    pub fn map_expressions<F: FnMut(ExprRef, &SchemaRef) -> DaftResult<Transformed<ExprRef>>>(
        self: Arc<Self>,
        mut f: F,
    ) -> DaftResult<Transformed<Arc<Self>>> {
        use crate::ops::{ActorPoolProject, Explode, Filter, Join, Project, Repartition, Sort};

        Ok(match self.as_ref() {
            Self::Project(Project {
                input,
                projection,
                projected_schema,
                stats_state,
            }) => projection
                .iter()
                .cloned()
                .map_and_collect(|expr| f(expr, &input.schema()))?
                .update_data(|new_projection| {
                    Self::Project(Project {
                        input: input.clone(),
                        projection: new_projection,
                        projected_schema: projected_schema.clone(),
                        stats_state: stats_state.clone(),
                    })
                    .into()
                }),
            Self::Filter(Filter {
                input,
                predicate,
                stats_state,
            }) => f(predicate.clone(), &input.schema())?.update_data(|expr| {
                Self::Filter(Filter {
                    input: input.clone(),
                    predicate: expr,
                    stats_state: stats_state.clone(),
                })
                .into()
            }),
            Self::Repartition(Repartition {
                input,
                repartition_spec,
                stats_state,
            }) => match repartition_spec {
                RepartitionSpec::Hash(HashRepartitionConfig { num_partitions, by }) => by
                    .iter()
                    .cloned()
                    .map_and_collect(|expr| f(expr, &input.schema()))?
                    .update_data(|expr| {
                        Self::Repartition(Repartition {
                            input: input.clone(),
                            repartition_spec: RepartitionSpec::Hash(HashRepartitionConfig {
                                num_partitions: *num_partitions,
                                by: expr,
                            }),
                            stats_state: stats_state.clone(),
                        })
                        .into()
                    }),
                _ => Transformed::no(self.clone()),
            },
            Self::ActorPoolProject(ActorPoolProject {
                input,
                projection,
                projected_schema,
                stats_state,
            }) => projection
                .iter()
                .cloned()
                .map_and_collect(|expr| f(expr, &input.schema()))?
                .update_data(|new_projection| {
                    Self::ActorPoolProject(ActorPoolProject {
                        input: input.clone(),
                        projection: new_projection,
                        projected_schema: projected_schema.clone(),
                        stats_state: stats_state.clone(),
                    })
                    .into()
                }),
            Self::Sort(Sort {
                input,
                sort_by,
                descending,
                nulls_first,
                stats_state,
            }) => sort_by
                .iter()
                .cloned()
                .map_and_collect(|expr| f(expr, &input.schema()))?
                .update_data(|new_sort_by| {
                    Self::Sort(Sort {
                        input: input.clone(),
                        sort_by: new_sort_by,
                        descending: descending.clone(),
                        nulls_first: nulls_first.clone(),
                        stats_state: stats_state.clone(),
                    })
                    .into()
                }),
            Self::Explode(Explode {
                input,
                to_explode,
                exploded_schema,
                stats_state,
            }) => to_explode
                .iter()
                .cloned()
                .map_and_collect(|expr| f(expr, &input.schema()))?
                .update_data(|new_to_explode| {
                    Self::Explode(Explode {
                        input: input.clone(),
                        to_explode: new_to_explode,
                        exploded_schema: exploded_schema.clone(),
                        stats_state: stats_state.clone(),
                    })
                    .into()
                }),
            Self::Join(Join {
                left,
                right,
                left_on,
                right_on,
                null_equals_nulls,
                join_type,
                join_strategy,
                output_schema,
                stats_state,
            }) => {
                let new_left_on = left_on
                    .iter()
                    .cloned()
                    .map_and_collect(|expr| f(expr, &left.schema()))?;
                let new_right_on = right_on
                    .iter()
                    .cloned()
                    .map_and_collect(|expr| f(expr, &right.schema()))?;

                if new_left_on.transformed && new_right_on.transformed {
                    Transformed::yes(
                        Self::Join(Join {
                            left: left.clone(),
                            right: right.clone(),
                            left_on: new_left_on.data,
                            right_on: new_right_on.data,
                            null_equals_nulls: null_equals_nulls.clone(),
                            join_type: *join_type,
                            join_strategy: *join_strategy,
                            output_schema: output_schema.clone(),
                            stats_state: stats_state.clone(),
                        })
                        .into(),
                    )
                } else {
                    Transformed::no(self)
                }
            }
            Self::Source(Source {
                output_schema,
                source_info,
                stats_state,
            }) => match source_info.as_ref() {
                SourceInfo::Physical(
                    physical_scan_info @ PhysicalScanInfo {
                        pushdowns:
                            pushdowns @ Pushdowns {
                                filters: Some(filter),
                                ..
                            },
                        scan_state: ScanState::Operator(scan_operator),
                        source_schema,
                        ..
                    },
                ) => {
                    let schema = if let Some(fields) = scan_operator.0.generated_fields() {
                        &Arc::new(source_schema.non_distinct_union(&fields))
                    } else {
                        source_schema
                    };

                    f(filter.clone(), schema)?.update_data(|new_filter| {
                        Self::Source(Source {
                            output_schema: output_schema.clone(),
                            source_info: Arc::new(SourceInfo::Physical(
                                physical_scan_info
                                    .with_pushdowns(pushdowns.with_filters(Some(new_filter))),
                            )),
                            stats_state: stats_state.clone(),
                        })
                        .into()
                    })
                }
                _ => Transformed::no(self),
            },
            _ => Transformed::no(self),
        })
    }
}
