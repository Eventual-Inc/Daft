use common_error::DaftResult;
use common_treenode::{ConcreteTreeNode, Transformed, TreeNode};
use daft_dsl::{is_partition_compatible, ExprRef};
use daft_logical_plan::partitioning::{ClusteringSpec, HashClusteringConfig};

use crate::{
    ops::{
        ActorPoolProject, Aggregate, Explode, HashJoin, Project, ShuffleExchange,
        ShuffleExchangeStrategy, Unpivot,
    },
    optimization::{plan_context::PlanContext, rules::PhysicalOptimizerRule},
    PhysicalPlan, PhysicalPlanRef,
};

pub struct ReorderPartitionKeys {}

type PartitionContext = PlanContext<Vec<ExprRef>>;

// Reorders columns in partitions so that they can be removed later.
// This works by maintaining a "canonical" ordering of the columns as we walk
// down the plan tree.
// For instance, if we see a hash partition by [col("b"), col("a")], then if
// we see something that partitions by [col("a"), col("b")], we reorder it
// to also partition by [col("b"), col("a")].
// This allows us to remove redundant repartitions, which is done in another rule.
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
                    return Ok(Transformed::no(c.with_new_children(vec![left_child, right_child])?))
                }
                // for other joins, hash partitioning doesn't matter
                PhysicalPlan::BroadcastJoin(..) |
                PhysicalPlan::SortMergeJoin(..) => return Ok(Transformed::no(c)),
                _ => {},
            }

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
            }

            let new_spec = ClusteringSpec::Hash(HashClusteringConfig::new(
                clustering_spec.num_partitions(),
                c.context.clone(),
            ));

            // we are hash partitioned but we might need to transform the expression
            match c.plan.as_ref() {
                // these store their clustering spec inside
                PhysicalPlan::Project(Project { input, projection, .. }) => {
                    let new_plan = PhysicalPlan::Project(Project::new_with_clustering_spec(
                        input.clone(),
                        projection.clone(),
                        new_spec.into(),
                    )?);
                    Ok(Transformed::yes(c.with_plan(new_plan.into()).propagate()))
                }
                PhysicalPlan::ActorPoolProject(ActorPoolProject { input, projection, clustering_spec: _ }) => {
                    let new_plan = PhysicalPlan::ActorPoolProject(ActorPoolProject {
                        input: input.clone(),
                        projection: projection.clone(),
                        clustering_spec: new_spec.into(),
                    });
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
                PhysicalPlan::Aggregate(Aggregate { input, aggregations, .. }) => {
                    let new_plan = PhysicalPlan::Aggregate(Aggregate {
                        input: input.clone(),
                        aggregations: aggregations.clone(),
                        groupby: c.context.clone(),
                    });
                    Ok(Transformed::yes(c.with_plan(new_plan.into()).propagate()))
                }
                PhysicalPlan::ShuffleExchange(ShuffleExchange{input, strategy: ShuffleExchangeStrategy::NaiveFullyMaterializingMapReduce { .. }}) => {
                    let new_plan = PhysicalPlan::ShuffleExchange(ShuffleExchange {
                        input: input.clone(),
                        strategy: ShuffleExchangeStrategy::NaiveFullyMaterializingMapReduce { target_spec: new_spec.into() }
                    });
                    Ok(Transformed::yes(c.with_plan(new_plan.into()).propagate()))
                }
                PhysicalPlan::ShuffleExchange(ShuffleExchange{input, strategy: ShuffleExchangeStrategy::MapReduceWithPreShuffleMerge { pre_shuffle_merge_threshold,.. }}) => {
                    let new_plan = PhysicalPlan::ShuffleExchange(ShuffleExchange {
                        input: input.clone(),
                        strategy: ShuffleExchangeStrategy::MapReduceWithPreShuffleMerge { target_spec: new_spec.into(), pre_shuffle_merge_threshold: *pre_shuffle_merge_threshold }
                    });
                    Ok(Transformed::yes(c.with_plan(new_plan.into()).propagate()))
                }
                PhysicalPlan::ShuffleExchange(ShuffleExchange{input, strategy: ShuffleExchangeStrategy::FlightShuffle { shuffle_dirs, .. }}) => {
                    let new_plan = PhysicalPlan::ShuffleExchange(ShuffleExchange {
                        input: input.clone(),
                        strategy: ShuffleExchangeStrategy::FlightShuffle { target_spec: new_spec.into(), shuffle_dirs: shuffle_dirs.clone() }
                    });
                    Ok(Transformed::yes(c.with_plan(new_plan.into()).propagate()))
                }

                // these depend solely on their input
                PhysicalPlan::Dedup(..) |
                PhysicalPlan::Filter(..) |
                PhysicalPlan::Limit(..) |
                PhysicalPlan::Sample(..) |
                PhysicalPlan::MonotonicallyIncreasingId(..) |
                PhysicalPlan::Pivot(..) |
                PhysicalPlan::TabularWriteCsv(..) |
                PhysicalPlan::TabularWriteJson(..) |
                PhysicalPlan::TabularWriteParquet(..) => Ok(Transformed::no(c.propagate())),

                // the rest should have been dealt with earlier
                PhysicalPlan::ShuffleExchange(ShuffleExchange {strategy: ShuffleExchangeStrategy::SplitOrCoalesceToTargetNum { .. }, ..}) |
                PhysicalPlan::Sort(..) |
                PhysicalPlan::TopN(..) |
                PhysicalPlan::InMemoryScan(..) |
                PhysicalPlan::TabularScan(..) |
                PhysicalPlan::EmptyScan(..) |
                PhysicalPlan::PreviousStageScan(..) |
                PhysicalPlan::Concat(..) |
                PhysicalPlan::HashJoin(..) |
                PhysicalPlan::SortMergeJoin(..) |
                PhysicalPlan::BroadcastJoin(..) |
                PhysicalPlan::CrossJoin(..) => unreachable!("PhysicalPlan match for ReorderPartitionKeys physical optimizer rule should not be reachable"),
                #[cfg(feature = "python")]
                PhysicalPlan::IcebergWrite(..) | PhysicalPlan::DeltaLakeWrite(..) | PhysicalPlan::LanceWrite(..) | PhysicalPlan::DataSink(..) => {
                    unreachable!("PhysicalPlan match for ReorderPartitionKeys physical optimizer rule should not be reachable")
                }
            }
        })?;
        res_transformed.map_data(|c| Ok(c.plan))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_core::prelude::*;
    use daft_dsl::{resolved_col, ExprRef};
    use daft_logical_plan::partitioning::{ClusteringSpec, UnknownClusteringConfig};

    use crate::{
        ops::{EmptyScan, HashJoin, ShuffleExchangeFactory},
        optimization::rules::{
            reorder_partition_keys::ReorderPartitionKeys, PhysicalOptimizerRule,
        },
        PhysicalPlan, PhysicalPlanRef,
    };

    fn create_dummy_plan(schema: SchemaRef, num_partitions: usize) -> PhysicalPlanRef {
        PhysicalPlan::EmptyScan(EmptyScan::new(
            schema,
            ClusteringSpec::Unknown(UnknownClusteringConfig::new(num_partitions)).into(),
        ))
        .into()
    }

    fn add_repartition(
        plan: PhysicalPlanRef,
        num_partitions: usize,
        partition_by: Vec<ExprRef>,
    ) -> PhysicalPlanRef {
        PhysicalPlan::ShuffleExchange(
            ShuffleExchangeFactory::new(plan)
                .get_hash_partitioning(partition_by, num_partitions, None)
                .unwrap(),
        )
        .into()
    }

    // makes sure trivial repartitions are modified
    #[test]
    fn test_repartition_modified() -> DaftResult<()> {
        let base = create_dummy_plan(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32),
                Field::new("b", DataType::Int32),
                Field::new("c", DataType::Int32),
            ])),
            1,
        );
        let plan = add_repartition(base.clone(), 1, vec![resolved_col("a"), resolved_col("b")]);
        let plan = add_repartition(plan, 1, vec![resolved_col("b"), resolved_col("a")]);
        let rule = ReorderPartitionKeys {};
        let res = rule.rewrite(plan)?;
        assert!(res.transformed);

        // expected is two repartitions by b, a
        let expected_plan = add_repartition(base, 1, vec![resolved_col("b"), resolved_col("a")]);
        let expected_plan =
            add_repartition(expected_plan, 1, vec![resolved_col("b"), resolved_col("a")]);
        assert_eq!(res.data, expected_plan);
        Ok(())
    }

    // makes sure different repartitions are not modified
    #[test]
    fn test_repartition_not_modified() -> DaftResult<()> {
        let plan = create_dummy_plan(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32),
                Field::new("b", DataType::Int32),
                Field::new("c", DataType::Int32),
            ])),
            1,
        );
        let plan = add_repartition(plan, 1, vec![resolved_col("a"), resolved_col("b")]);
        let plan = add_repartition(plan, 1, vec![resolved_col("a"), resolved_col("c")]);
        let plan = add_repartition(
            plan,
            1,
            vec![resolved_col("a"), resolved_col("c"), resolved_col("b")],
        );
        let plan = add_repartition(plan, 1, vec![resolved_col("b")]);
        let rule = ReorderPartitionKeys {};
        let res = rule.rewrite(plan.clone())?;
        assert!(!res.transformed);
        assert_eq!(res.data, plan);
        Ok(())
    }

    // makes sure hash joins reorder the columns
    #[test]
    fn test_repartition_hash_join_reorder() -> DaftResult<()> {
        let base1 = create_dummy_plan(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32),
                Field::new("b", DataType::Int32),
                Field::new("c", DataType::Int32),
            ])),
            1,
        );
        let plan1 = add_repartition(base1.clone(), 1, vec![resolved_col("a"), resolved_col("b")]);

        let base2 = create_dummy_plan(
            Arc::new(Schema::new(vec![
                Field::new("x", DataType::Int32),
                Field::new("y", DataType::Int32),
                Field::new("z", DataType::Int32),
            ])),
            1,
        );
        let plan2 = add_repartition(base2.clone(), 1, vec![resolved_col("x"), resolved_col("y")]);

        let plan = PhysicalPlan::HashJoin(HashJoin::new(
            plan1,
            plan2,
            vec![resolved_col("b"), resolved_col("a")],
            vec![resolved_col("x"), resolved_col("y")],
            None,
            JoinType::Inner,
        ))
        .arced();

        let rule = ReorderPartitionKeys {};
        let res = rule.rewrite(plan)?;
        assert!(res.transformed);

        let expected_plan = PhysicalPlan::HashJoin(HashJoin::new(
            add_repartition(base1, 1, vec![resolved_col("b"), resolved_col("a")]),
            add_repartition(base2, 1, vec![resolved_col("x"), resolved_col("y")]),
            vec![resolved_col("b"), resolved_col("a")],
            vec![resolved_col("x"), resolved_col("y")],
            None,
            JoinType::Inner,
        ))
        .arced();
        assert_eq!(res.data, expected_plan);
        Ok(())
    }
}
