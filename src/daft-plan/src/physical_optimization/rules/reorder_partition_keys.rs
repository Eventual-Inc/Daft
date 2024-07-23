use common_error::DaftResult;
use common_treenode::{ConcreteTreeNode, Transformed, TreeNode};
use daft_dsl::{is_partition_compatible, ExprRef};

use crate::{
    partitioning::HashClusteringConfig,
    physical_ops::{Aggregate, Explode, FanoutByHash, HashJoin, Project, Unpivot},
    physical_optimization::{optimizer::PhysicalOptimizerRule, plan_context::PlanContext},
    ClusteringSpec, PhysicalPlan, PhysicalPlanRef,
};

pub struct ReorderPartitionKeys {}

type PartitionContext = PlanContext<Vec<ExprRef>>;

impl PhysicalOptimizerRule for ReorderPartitionKeys {
    fn rewrite(&self, plan: PhysicalPlanRef) -> DaftResult<Transformed<PhysicalPlanRef>> {
        let plan_context = PartitionContext::new_default(plan);

        let res_transformed = plan_context.transform_down(|c| {
            let plan = c.plan.clone();
            match plan.as_ref() {
                // 0-input nodes
                #[cfg(feature = "python")]
                PhysicalPlan::InMemoryScan(..) => return Ok(Transformed::no(c)),
                PhysicalPlan::EmptyScan(..) |
                PhysicalPlan::TabularScan(..) => return Ok(Transformed::no(c)),
                // 2-input nodes
                // for concat, hash partitioning shouldn't change
                PhysicalPlan::Concat(..) => return Ok(Transformed::no(c.propagate())),
                // for hash join, send separate partitionings to children
                PhysicalPlan::HashJoin(HashJoin { left_on, right_on, .. }) => {
                    let (c, old_children) = c.take_children();
                    let num_children = old_children.len();
                    let Ok([left_child, right_child]) = TryInto::<[_; 2]>::try_into(old_children) else {
                        panic!("HashJoin has {} children, expected 2", num_children);
                    };
                    let left_child = left_child.with_context(left_on.clone());
                    let right_child = right_child.with_context(right_on.clone());
                    return Ok(Transformed::no(c.with_new_children(vec![left_child, right_child])?.propagate()))
                }
                // for other joins, hash partitioning doesn't matter
                PhysicalPlan::BroadcastJoin(..) |
                PhysicalPlan::SortMergeJoin(..) => return Ok(Transformed::no(c)),
                _ => {},
            };

            // check clustering spec for compatibility
            let clustering_spec = c.plan.clustering_spec();
            match clustering_spec.as_ref() {
                ClusteringSpec::Hash(HashClusteringConfig { by, .. }) => {
                    if *by == c.context {
                        // partition is already perfect
                        return Ok(Transformed::no(c.propagate()));
                    }
                    if !is_partition_compatible(&c.context, by) {
                        // we are hash partitioned, just by something different
                        return Ok(Transformed::no(c.with_context(by.clone()).propagate()));
                    }
                    // otherwise we need to reorder the columns
                }
                _ => return Ok(Transformed::no(c)),
            };

            let new_spec = ClusteringSpec::Hash(HashClusteringConfig::new(
                clustering_spec.num_partitions(),
                c.context.clone(),
            ));

            // we are hash partitioned but we might need to transform the expression
            match c.plan.as_ref() {
                // these store their clustering spec inside
                PhysicalPlan::Project(Project { input, projection, resource_request, .. }) => {
                    let new_plan = PhysicalPlan::Project(Project::new_with_clustering_spec(
                        input.clone(),
                        projection.clone(),
                        resource_request.clone(),
                        new_spec.into(),
                    )?);
                    Ok(Transformed::yes(c.with_plan(new_plan.into()).propagate()))
                }
                PhysicalPlan::Explode(Explode { input, to_explode, .. }) => {
                    // can't use try_new because we are setting the clustering spec ourselves
                    let new_plan = PhysicalPlan::Explode(Explode {
                        input: input.clone(),
                        to_explode: to_explode.clone(),
                        clustering_spec: new_spec.into(),
                    });
                    Ok(Transformed::yes(c.with_plan(new_plan.into()).propagate()))
                }
                PhysicalPlan::Unpivot(Unpivot { input, ids, values, value_name, variable_name, .. }) => {
                    // can't use new because we are setting the clustering spec ourselves
                    let new_plan = PhysicalPlan::Unpivot(Unpivot {
                        input: input.clone(),
                        ids: ids.clone(),
                        values: values.clone(),
                        value_name: value_name.clone(),
                        variable_name: variable_name.clone(),
                        clustering_spec: new_spec.into()
                    });
                    Ok(Transformed::yes(c.with_plan(new_plan.into()).propagate()))
                }
                PhysicalPlan::FanoutByHash(FanoutByHash { input, num_partitions, .. }) => {
                    let new_plan = PhysicalPlan::FanoutByHash(FanoutByHash {
                        input: input.clone(),
                        num_partitions: *num_partitions,
                        partition_by: c.context.clone()
                    });
                    Ok(Transformed::yes(c.with_plan(new_plan.into()).propagate()))
                }
                PhysicalPlan::Aggregate(Aggregate { input, aggregations, .. }) => {
                    let new_plan = PhysicalPlan::Aggregate(Aggregate {
                        input: input.clone(),
                        aggregations: aggregations.clone(),
                        groupby: c.context.clone(),
                    });
                    Ok(Transformed::yes(c.with_plan(new_plan.into()).propagate()))
                }

                // these depend solely on their input
                PhysicalPlan::Filter(..) |
                PhysicalPlan::Limit(..) |
                PhysicalPlan::Sample(..) |
                PhysicalPlan::MonotonicallyIncreasingId(..) |
                PhysicalPlan::Flatten(..) |
                PhysicalPlan::ReduceMerge(..) |
                PhysicalPlan::Pivot(..) |
                PhysicalPlan::TabularWriteCsv(..) |
                PhysicalPlan::TabularWriteJson(..) |
                PhysicalPlan::TabularWriteParquet(..) => Ok(Transformed::no(c.propagate())),

                // the rest should have been dealt with earlier
                _ => unreachable!("Catch-all in match for ReorderPartitionKeys physical optimizer rule should not be reachable")
            }
        })?;
        res_transformed.map_data(|c| Ok(c.plan))
    }
}
