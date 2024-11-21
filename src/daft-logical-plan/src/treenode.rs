use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{
    map_until_stop_and_collect, DynTreeNode, Transformed, TreeNodeIterator, TreeNodeRecursion,
};
use daft_dsl::ExprRef;

use crate::{
    partitioning::{HashRepartitionConfig, RepartitionSpec},
    LogicalPlan,
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
    pub fn map_expressions<F: FnMut(ExprRef) -> DaftResult<Transformed<ExprRef>>>(
        self,
        mut f: F,
    ) -> DaftResult<Transformed<Self>> {
        use crate::ops::{ActorPoolProject, Explode, Filter, Join, Project, Repartition, Sort};

        Ok(match self {
            Self::Project(Project {
                input,
                projection,
                projected_schema,
            }) => projection
                .into_iter()
                .map_until_stop_and_collect(f)?
                .update_data(|expr| {
                    Self::Project(Project {
                        input,
                        projection: expr,
                        projected_schema,
                    })
                }),
            Self::Filter(Filter { input, predicate }) => f(predicate)?.update_data(|expr| {
                Self::Filter(Filter {
                    input,
                    predicate: expr,
                })
            }),
            Self::Repartition(Repartition {
                input,
                repartition_spec,
            }) => match repartition_spec {
                RepartitionSpec::Hash(HashRepartitionConfig { num_partitions, by }) => by
                    .into_iter()
                    .map_until_stop_and_collect(f)?
                    .update_data(|expr| {
                        RepartitionSpec::Hash(HashRepartitionConfig {
                            num_partitions,
                            by: expr,
                        })
                    }),
                repartition_spec => Transformed::no(repartition_spec),
            }
            .update_data(|repartition_spec| {
                Self::Repartition(Repartition {
                    input,
                    repartition_spec,
                })
            }),
            Self::ActorPoolProject(ActorPoolProject {
                input,
                projection,
                projected_schema,
            }) => projection
                .into_iter()
                .map_until_stop_and_collect(f)?
                .update_data(|expr| {
                    Self::ActorPoolProject(ActorPoolProject {
                        input,
                        projection: expr,
                        projected_schema,
                    })
                }),
            Self::Sort(Sort {
                input,
                sort_by,
                descending,
                nulls_first,
            }) => sort_by
                .into_iter()
                .map_until_stop_and_collect(f)?
                .update_data(|expr| {
                    Self::Sort(Sort {
                        input,
                        sort_by: expr,
                        descending,
                        nulls_first,
                    })
                }),
            Self::Explode(Explode {
                input,
                to_explode,
                exploded_schema,
            }) => to_explode
                .into_iter()
                .map_until_stop_and_collect(f)?
                .update_data(|expr| {
                    Self::Explode(Explode {
                        input,
                        to_explode: expr,
                        exploded_schema,
                    })
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
            }) => {
                let o = left_on
                    .into_iter()
                    .zip(right_on)
                    .map_until_stop_and_collect(|(l, r)| {
                        map_until_stop_and_collect!(f(l), r, f(r))
                    })?;
                let (left_on, right_on) = o.data.into_iter().unzip();

                if o.transformed {
                    Transformed::yes(Self::Join(Join {
                        left,
                        right,
                        left_on,
                        right_on,
                        null_equals_nulls,
                        join_type,
                        join_strategy,
                        output_schema,
                    }))
                } else {
                    Transformed::no(Self::Join(Join {
                        left,
                        right,
                        left_on,
                        right_on,
                        null_equals_nulls,
                        join_type,
                        join_strategy,
                        output_schema,
                    }))
                }
            }
            lp => Transformed::no(lp),
        })
    }
}
