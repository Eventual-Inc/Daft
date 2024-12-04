use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use common_treenode::{DynTreeNode, Transformed, TreeNode};
use daft_core::prelude::*;
use daft_dsl::{
    col, has_stateful_udf,
    optimization::{get_required_columns, replace_columns_with_expressions, requires_computation},
    Expr, ExprRef,
};
use indexmap::IndexSet;
use itertools::Itertools;

use super::OptimizerRule;
use crate::{
    ops::{ActorPoolProject, Aggregate, Join, Pivot, Project, Source},
    source_info::SourceInfo,
    LogicalPlan, LogicalPlanRef,
};

#[derive(Default, Debug)]
pub struct PushDownProjection {}

impl PushDownProjection {
    pub fn new() -> Self {
        Self {}
    }

    fn try_optimize_project(
        &self,
        projection: &Project,
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        let upstream_plan = &projection.input;
        let upstream_schema = upstream_plan.schema();

        // First, drop this projection if it is a no-op
        // (selecting exactly all parent columns in the same order and nothing else).
        let projection_is_noop = {
            // Short circuit early if the projection length is different (obviously not a no-op).
            upstream_schema.names().len() == projection.projection.len()
                && projection
                    .projection
                    .iter()
                    .zip(upstream_schema.names().iter())
                    .all(|(expr, upstream_col)| match expr.as_ref() {
                        Expr::Column(colname) => colname.as_ref() == upstream_col,
                        _ => false,
                    })
        };
        if projection_is_noop {
            // Projection discarded but new root node has not been looked at;
            // look at the new root node.
            let new_plan = self
                .try_optimize_node(upstream_plan.clone())?
                .or(Transformed::yes(upstream_plan.clone()));
            return Ok(new_plan);
        }

        // Next, check if the upstream is another projection we can merge with.
        // This is possible iff the upstream projection's computation-required columns
        // are each only used once in this downstream projection.
        if let LogicalPlan::Project(upstream_projection) = upstream_plan.as_ref() {
            // Get all the computation-required columns from the upstream projection.
            let upstream_computations = upstream_projection
                .projection
                .iter()
                .filter_map(|e| {
                    e.input_mapping().map_or_else(
                        // None means computation required -> Some(colname)
                        || Some(e.name().to_string()),
                        // Some(computation not required) -> None
                        |_| None,
                    )
                })
                .collect::<IndexSet<_>>();

            // For each of them, make sure they are used only once in this downstream projection.
            let mut exprs_to_walk: Vec<Arc<Expr>> = projection.projection.clone();

            let mut upstream_computations_used = IndexSet::new();
            let mut okay_to_merge = true;

            while !exprs_to_walk.is_empty() {
                exprs_to_walk = exprs_to_walk
                    .iter()
                    .flat_map(|expr| {
                        // If it's a reference for a column that requires computation,
                        // record it.
                        if okay_to_merge
                            && let Expr::Column(name) = expr.as_ref()
                            && upstream_computations.contains(name.as_ref())
                        {
                            okay_to_merge = okay_to_merge
                                && upstream_computations_used.insert(name.to_string());
                        };
                        if okay_to_merge {
                            expr.children()
                        } else {
                            // Short circuit to avoid continuing walking the tree.
                            vec![]
                        }
                    })
                    .collect();
            }

            // If the upstream is okay to merge into the current projection,
            // do the merge.
            if okay_to_merge {
                // Get the name and expression for each of the upstream columns.
                let upstream_names_to_exprs = upstream_projection
                    .projection
                    .iter()
                    .map(|e| (e.name().to_string(), e.clone()))
                    .collect::<HashMap<_, _>>();

                // Merge the projections by applying the upstream expression substitutions
                // to the current projection.
                let merged_projection = projection
                    .projection
                    .iter()
                    .map(|e| replace_columns_with_expressions(e.clone(), &upstream_names_to_exprs))
                    .collect();

                // Make a new projection node with the merged projections.
                let new_plan: LogicalPlan =
                    Project::try_new(upstream_projection.input.clone(), merged_projection)?.into();
                let new_plan: Arc<LogicalPlan> = new_plan.into();

                // Root node is changed, look at it again.
                let new_plan = self
                    .try_optimize_node(new_plan.clone())?
                    .or(Transformed::yes(new_plan));
                return Ok(new_plan);
            }
        }

        match upstream_plan.as_ref() {
            LogicalPlan::Source(source) => {
                // Prune unnecessary columns directly from the source.
                let [required_columns] = &plan.required_columns()[..] else {
                    panic!()
                };
                match source.source_info.as_ref() {
                    SourceInfo::Physical(external_info) => {
                        if required_columns.len() < upstream_schema.names().len() {
                            let pruned_upstream_schema = upstream_schema
                                .fields
                                .iter()
                                .filter(|&(name, _)| required_columns.contains(name))
                                .map(|(_, field)| field.clone())
                                .collect::<Vec<_>>();
                            let schema = Schema::new(pruned_upstream_schema)?;
                            let new_source: LogicalPlan = Source::new(
                                schema.into(),
                                Arc::new(SourceInfo::Physical(external_info.with_pushdowns(
                                    external_info.pushdowns.with_columns(Some(Arc::new(
                                        required_columns.iter().cloned().collect(),
                                    ))),
                                ))),
                            )
                            .into();
                            let new_plan = Arc::new(plan.with_new_children(&[new_source.into()]));
                            // Retry optimization now that the upstream node is different.
                            let new_plan = self
                                .try_optimize_node(new_plan.clone())?
                                .or(Transformed::yes(new_plan));
                            Ok(new_plan)
                        } else {
                            Ok(Transformed::no(plan))
                        }
                    }
                    SourceInfo::InMemory(_) => Ok(Transformed::no(plan)),
                    SourceInfo::PlaceHolder(..) => {
                        panic!("PlaceHolderInfo should not exist for optimization!");
                    }
                }
            }
            LogicalPlan::Project(upstream_projection) => {
                // Prune columns from the child projection that are not used in this projection.
                let required_columns = &plan.required_columns()[0];
                if required_columns.len() < upstream_schema.names().len() {
                    let pruned_upstream_projections = upstream_projection
                        .projection
                        .iter()
                        .filter(|&e| required_columns.contains(e.name()))
                        .cloned()
                        .collect::<Vec<_>>();

                    let new_upstream: LogicalPlan = Project::try_new(
                        upstream_projection.input.clone(),
                        pruned_upstream_projections,
                    )?
                    .into();

                    let new_plan = Arc::new(plan.with_new_children(&[new_upstream.into()]));
                    // Retry optimization now that the upstream node is different.
                    let new_plan = self
                        .try_optimize_node(new_plan.clone())?
                        .or(Transformed::yes(new_plan));
                    Ok(new_plan)
                } else {
                    Ok(Transformed::no(plan))
                }
            }
            LogicalPlan::Aggregate(aggregate) => {
                // Prune unnecessary columns from the child aggregate.
                let required_columns = &plan.required_columns()[0];
                let pruned_aggregate_exprs = aggregate
                    .aggregations
                    .iter()
                    .filter(|&e| required_columns.contains(e.name()))
                    .cloned()
                    .collect::<Vec<_>>();

                if pruned_aggregate_exprs.len() < aggregate.aggregations.len() {
                    let new_upstream: LogicalPlan = Aggregate::try_new(
                        aggregate.input.clone(),
                        pruned_aggregate_exprs,
                        aggregate.groupby.clone(),
                    )?
                    .into();

                    let new_plan = Arc::new(plan.with_new_children(&[new_upstream.into()]));
                    // Retry optimization now that the upstream node is different.
                    let new_plan = self
                        .try_optimize_node(new_plan.clone())?
                        .or(Transformed::yes(new_plan));
                    Ok(new_plan)
                } else {
                    Ok(Transformed::no(plan))
                }
            }
            LogicalPlan::ActorPoolProject(upstream_actor_pool_projection) => {
                // Attempt to merge the current Projection into the upstream ActorPoolProject
                // if there aren't any actual computations being performed in the Projection, and
                // if each upstream column is used only once (no common subtrees)
                if projection
                    .projection
                    .iter()
                    .all(|e| !requires_computation(e))
                {
                    // Only perform this optimization if all required column names are distinct
                    let required_column_names = projection
                        .projection
                        .iter()
                        .flat_map(get_required_columns)
                        .collect_vec();
                    let mut all_required_column_names_distinct = true;
                    let mut distinct_required_column_names = IndexSet::new();
                    for required_col_name in required_column_names {
                        if distinct_required_column_names.contains(&required_col_name) {
                            all_required_column_names_distinct = false;
                            break;
                        } else {
                            distinct_required_column_names.insert(required_col_name);
                        }
                    }

                    if all_required_column_names_distinct {
                        let actor_pool_projection_map = upstream_actor_pool_projection
                            .projection
                            .iter()
                            .map(|e| (e.name().to_string(), e.clone()))
                            .collect::<HashMap<String, ExprRef>>();
                        let new_actor_pool_projections = projection
                            .projection
                            .iter()
                            .map(|p| {
                                replace_columns_with_expressions(
                                    p.clone(),
                                    &actor_pool_projection_map,
                                )
                            })
                            .collect_vec();

                        // Construct either a new ActorPoolProject or Project, depending on whether the pruned projection still has StatefulUDFs
                        let new_plan = if new_actor_pool_projections.iter().any(has_stateful_udf) {
                            LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
                                upstream_actor_pool_projection.input.clone(),
                                new_actor_pool_projections,
                            )?)
                            .arced()
                        } else {
                            LogicalPlan::Project(Project::try_new(
                                upstream_actor_pool_projection.input.clone(),
                                new_actor_pool_projections,
                            )?)
                            .arced()
                        };

                        // Retry optimization now that the node is different.
                        let new_plan = self
                            .try_optimize_node(new_plan.clone())?
                            .or(Transformed::yes(new_plan));
                        return Ok(new_plan);
                    }
                }

                // Prune columns from the child ActorPoolProjection that are not used in this projection.
                let required_columns = &plan.required_columns()[0];
                if required_columns.len() < upstream_schema.names().len() {
                    let pruned_upstream_projections = upstream_actor_pool_projection
                        .projection
                        .iter()
                        .filter(|&e| required_columns.contains(e.name()))
                        .cloned()
                        .collect::<Vec<_>>();

                    // If all StatefulUDF expressions end up being pruned, the ActorPoolProject should essentially become
                    // a no-op passthrough projection for the rest of the columns. In this case, we should just get rid of it
                    // altogether since it serves no purpose.
                    let all_projections_are_just_colexprs =
                        pruned_upstream_projections.iter().all(|proj| {
                            !proj.exists(|e| match e.as_ref() {
                                Expr::Column(_) => false,
                                // Check for existence of any non-ColumnExprs
                                _ => true,
                            })
                        });
                    let new_upstream = if all_projections_are_just_colexprs {
                        upstream_plan.arc_children()[0].clone()
                    } else {
                        LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
                            upstream_actor_pool_projection.input.clone(),
                            pruned_upstream_projections,
                        )?)
                        .arced()
                    };
                    let new_plan = Arc::new(plan.with_new_children(&[new_upstream]));

                    // Retry optimization now that the upstream node is different.
                    let new_plan = self
                        .try_optimize_node(new_plan.clone())?
                        .or(Transformed::yes(new_plan));
                    Ok(new_plan)
                } else {
                    Ok(Transformed::no(plan))
                }
            }
            LogicalPlan::Sort(..)
            | LogicalPlan::Repartition(..)
            | LogicalPlan::Limit(..)
            | LogicalPlan::Filter(..)
            | LogicalPlan::Sample(..)
            | LogicalPlan::Explode(..)
            | LogicalPlan::Unpivot(..) => {
                // Get required columns from projection and upstream.
                let combined_dependencies = plan
                    .required_columns()
                    .iter()
                    .flatten()
                    .chain(upstream_plan.required_columns().iter().flatten())
                    .cloned()
                    .collect::<IndexSet<_>>();

                // Skip optimization if no columns would be pruned.
                let grand_upstream_plan = &upstream_plan.arc_children()[0];
                let grand_upstream_columns = grand_upstream_plan.schema().names();
                if grand_upstream_columns.len() == combined_dependencies.len() {
                    return Ok(Transformed::no(plan));
                }

                let new_subprojection: LogicalPlan = {
                    let pushdown_column_exprs = combined_dependencies
                        .into_iter()
                        .map(col)
                        .collect::<Vec<_>>();

                    Project::try_new(grand_upstream_plan.clone(), pushdown_column_exprs)?.into()
                };

                let new_upstream = upstream_plan.with_new_children(&[new_subprojection.into()]);
                let new_plan = Arc::new(plan.with_new_children(&[new_upstream.into()]));
                // Retry optimization now that the upstream node is different.
                let new_plan = self
                    .try_optimize_node(new_plan.clone())?
                    .or(Transformed::yes(new_plan));
                Ok(new_plan)
            }
            LogicalPlan::Concat(concat) => {
                // Get required columns from projection and upstream.
                let combined_dependencies = plan
                    .required_columns()
                    .iter()
                    .flatten()
                    .chain(upstream_plan.required_columns().iter().flatten())
                    .cloned()
                    .collect::<IndexSet<_>>();

                // Skip optimization if no columns would be pruned.
                let grand_upstream_plan = &upstream_plan.children()[0];
                let grand_upstream_columns = grand_upstream_plan.schema().names();
                if grand_upstream_columns.len() == combined_dependencies.len() {
                    return Ok(Transformed::no(plan));
                }

                let pushdown_column_exprs: Vec<ExprRef> = combined_dependencies
                    .into_iter()
                    .map(col)
                    .collect::<Vec<_>>();
                let new_left_subprojection: LogicalPlan = {
                    Project::try_new(concat.input.clone(), pushdown_column_exprs.clone())?.into()
                };
                let new_right_subprojection: LogicalPlan =
                    { Project::try_new(concat.other.clone(), pushdown_column_exprs)?.into() };

                let new_upstream = upstream_plan.with_new_children(&[
                    new_left_subprojection.into(),
                    new_right_subprojection.into(),
                ]);
                let new_plan = Arc::new(plan.with_new_children(&[new_upstream.into()]));
                // Retry optimization now that the upstream node is different.
                let new_plan = self
                    .try_optimize_node(new_plan.clone())?
                    .or(Transformed::yes(new_plan));
                Ok(new_plan)
            }
            LogicalPlan::Union(_) => unreachable!("Union should have been optimized away"),
            LogicalPlan::Join(join) => {
                // Get required columns from projection and both upstreams.
                let [projection_dependencies] = &plan.required_columns()[..] else {
                    panic!()
                };
                let [left_dependencies, right_dependencies] = &upstream_plan.required_columns()[..]
                else {
                    panic!()
                };

                /// For one side of the join, see if a non-vacuous pushdown is possible.
                fn maybe_project_upstream_input(
                    side: &LogicalPlanRef,
                    side_dependencies: &IndexSet<String>,
                    projection_dependencies: &IndexSet<String>,
                ) -> DaftResult<Transformed<LogicalPlanRef>> {
                    let schema = side.schema();
                    let upstream_names: IndexSet<String> = schema.fields.keys().cloned().collect();

                    let combined_dependencies: IndexSet<_> = side_dependencies
                        .union(
                            &upstream_names
                                .intersection(projection_dependencies)
                                .cloned()
                                .collect::<IndexSet<_>>(),
                        )
                        .cloned()
                        .collect();

                    if combined_dependencies.len() < upstream_names.len() {
                        let pushdown_column_exprs: Vec<ExprRef> =
                            combined_dependencies.into_iter().map(col).collect();
                        let new_project: LogicalPlan =
                            Project::try_new(side.clone(), pushdown_column_exprs)?.into();
                        Ok(Transformed::yes(new_project.into()))
                    } else {
                        Ok(Transformed::no(side.clone()))
                    }
                }

                let new_left_upstream = maybe_project_upstream_input(
                    &join.left,
                    left_dependencies,
                    projection_dependencies,
                )?;
                let new_right_upstream = maybe_project_upstream_input(
                    &join.right,
                    right_dependencies,
                    projection_dependencies,
                )?;

                if !new_left_upstream.transformed && !new_right_upstream.transformed {
                    Ok(Transformed::no(plan))
                } else {
                    // If either pushdown is possible, create a new Join node.
                    let new_join = upstream_plan
                        .with_new_children(&[new_left_upstream.data, new_right_upstream.data]);

                    let new_plan = Arc::new(plan.with_new_children(&[new_join.into()]));

                    // Retry optimization now that the upstream node is different.
                    let new_plan = self
                        .try_optimize_node(new_plan.clone())?
                        .or(Transformed::yes(new_plan));

                    Ok(new_plan)
                }
            }
            LogicalPlan::Distinct(_) => {
                // Cannot push down past a Distinct,
                // since Distinct implicitly requires all parent columns.
                Ok(Transformed::no(plan))
            }
            LogicalPlan::Intersect(_) => {
                // Cannot push down past an Intersect,
                // since Intersect implicitly requires all parent columns.
                Ok(Transformed::no(plan))
            }
            LogicalPlan::Pivot(_) | LogicalPlan::MonotonicallyIncreasingId(_) => {
                // Cannot push down past a Pivot/MonotonicallyIncreasingId because it changes the schema.
                Ok(Transformed::no(plan))
            }
            LogicalPlan::Sink(_) => {
                panic!("Bad projection due to upstream sink node: {:?}", projection)
            }
        }
    }

    fn try_optimize_actor_pool_project(
        &self,
        actor_pool_project: &ActorPoolProject,
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        // If this ActorPoolPorject prunes columns from its upstream,
        // then explicitly create a projection to do so.
        let upstream_plan = &actor_pool_project.input;
        let upstream_schema = upstream_plan.schema();

        let actor_pool_project_required_cols = &plan.required_columns()[0];
        if actor_pool_project_required_cols.len() < upstream_schema.names().len() {
            let new_subprojection: LogicalPlan = {
                let pushdown_column_exprs = actor_pool_project_required_cols
                    .iter()
                    .map(|s| col(s.as_str()))
                    .collect::<Vec<_>>();

                Project::try_new(upstream_plan.clone(), pushdown_column_exprs)?.into()
            };

            let new_actor_pool_project = plan.with_new_children(&[new_subprojection.into()]);
            Ok(Transformed::yes(new_actor_pool_project.into()))
        } else {
            Ok(Transformed::no(plan))
        }
    }

    fn try_optimize_aggregation(
        &self,
        aggregation: &Aggregate,
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        // If this aggregation prunes columns from its upstream,
        // then explicitly create a projection to do so.
        let upstream_plan = &aggregation.input;
        let upstream_schema = upstream_plan.schema();

        let aggregation_required_cols = &plan.required_columns()[0];
        if aggregation_required_cols.len() < upstream_schema.names().len() {
            let new_subprojection: LogicalPlan = {
                let pushdown_column_exprs = aggregation_required_cols
                    .iter()
                    .map(|s| col(s.as_str()))
                    .collect::<Vec<_>>();

                Project::try_new(upstream_plan.clone(), pushdown_column_exprs)?.into()
            };

            let new_aggregation = plan.with_new_children(&[new_subprojection.into()]);
            Ok(Transformed::yes(new_aggregation.into()))
        } else {
            Ok(Transformed::no(plan))
        }
    }

    fn try_optimize_join(
        &self,
        join: &Join,
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        // If this join prunes columns from its upstream,
        // then explicitly create a projection to do so.
        // this is the case for semi and anti joins.

        if matches!(join.join_type, JoinType::Anti | JoinType::Semi) {
            let required_cols = plan.required_columns();
            let right_required_cols = required_cols
                .get(1)
                .expect("we expect 2 set of required columns for join");
            let right_schema = join.right.schema();

            if right_required_cols.len() < right_schema.fields.len() {
                let new_subprojection: LogicalPlan = {
                    let pushdown_column_exprs = right_required_cols
                        .iter()
                        .map(|s| col(s.as_str()))
                        .collect::<Vec<_>>();

                    Project::try_new(join.right.clone(), pushdown_column_exprs)?.into()
                };

                let new_join = plan
                    .with_new_children(&[(join.left).clone(), new_subprojection.into()])
                    .arced();

                Ok(self
                    .try_optimize_node(new_join.clone())?
                    .or(Transformed::yes(new_join)))
            } else {
                Ok(Transformed::no(plan))
            }
        } else {
            Ok(Transformed::no(plan))
        }
    }

    fn try_optimize_pivot(
        &self,
        pivot: &Pivot,
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        // If this pivot prunes columns from its upstream,
        // then explicitly create a projection to do so.
        let upstream_plan = &pivot.input;
        let upstream_schema = upstream_plan.schema();

        let pivot_required_cols = &plan.required_columns()[0];
        if pivot_required_cols.len() < upstream_schema.names().len() {
            let new_subprojection: LogicalPlan = {
                let pushdown_column_exprs = pivot_required_cols
                    .iter()
                    .map(|s| col(s.as_str()))
                    .collect::<Vec<_>>();

                Project::try_new(upstream_plan.clone(), pushdown_column_exprs)?.into()
            };

            let new_pivot = plan.with_new_children(&[new_subprojection.into()]);
            Ok(Transformed::yes(new_pivot.into()))
        } else {
            Ok(Transformed::no(plan))
        }
    }

    fn try_optimize_node(
        &self,
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        match plan.as_ref() {
            LogicalPlan::Project(projection) => self.try_optimize_project(projection, plan.clone()),
            // ActorPoolProjects also do column projection
            LogicalPlan::ActorPoolProject(actor_pool_project) => {
                self.try_optimize_actor_pool_project(actor_pool_project, plan.clone())
            }
            // Aggregations also do column projection
            LogicalPlan::Aggregate(aggregation) => {
                self.try_optimize_aggregation(aggregation, plan.clone())
            }
            // Joins also do column projection
            LogicalPlan::Join(join) => self.try_optimize_join(join, plan.clone()),
            // Pivots also do column projection
            LogicalPlan::Pivot(pivot) => self.try_optimize_pivot(pivot, plan.clone()),
            _ => Ok(Transformed::no(plan)),
        }
    }
}

impl OptimizerRule for PushDownProjection {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform_down(|node| self.try_optimize_node(node))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use common_scan_info::Pushdowns;
    use daft_core::prelude::*;
    use daft_dsl::{
        col,
        functions::python::{RuntimePyObject, UDFRuntimeBinding},
        lit,
    };

    use crate::{
        optimization::{rules::PushDownProjection, test::assert_optimized_plan_with_rules_eq},
        test::{dummy_scan_node, dummy_scan_node_with_pushdowns, dummy_scan_operator},
        LogicalPlan,
    };

    /// Helper that creates an optimizer with the PushDownProjection rule registered, optimizes
    /// the provided plan with said optimizer, and compares the optimized plan with
    /// the provided expected plan.
    fn assert_optimized_plan_eq(
        plan: Arc<LogicalPlan>,
        expected: Arc<LogicalPlan>,
    ) -> DaftResult<()> {
        assert_optimized_plan_with_rules_eq(
            plan,
            expected,
            vec![Box::new(PushDownProjection::new())],
        )
    }

    /// Projection merging: Ensure factored projections do not get merged.
    #[test]
    fn test_merge_does_not_unfactor() -> DaftResult<()> {
        let a2 = col("a").add(col("a"));
        let a4 = a2.clone().add(a2);
        let a8 = a4.clone().add(a4);
        let expressions = vec![a8.alias("x")];
        let scan_op = dummy_scan_operator(vec![Field::new("a", DataType::Int64)]);
        let plan = dummy_scan_node(scan_op).select(expressions)?.build();

        assert_optimized_plan_eq(plan.clone(), plan)?;
        Ok(())
    }

    /// Projection merging: Ensure merging happens even when there is computation
    /// in both the parent and the child.
    #[test]
    fn test_merge_projections() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]);
        let proj1 = vec![
            col("a").add(lit(1)),
            col("b").add(lit(2)),
            col("a").alias("c"),
        ];
        let proj2 = vec![col("a").add(lit(3)), col("b"), col("c").add(lit(4))];
        let plan = dummy_scan_node(scan_op.clone())
            .select(proj1)?
            .select(proj2)?
            .build();

        let merged_proj = vec![
            col("a").add(lit(1)).add(lit(3)),
            col("b").add(lit(2)),
            col("a").alias("c").add(lit(4)),
        ];
        let expected = dummy_scan_node(scan_op).select(merged_proj)?.build();

        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Projection dropping: Test that a no-op projection is dropped.
    #[test]
    fn test_drop_projection() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .select(vec![col("a"), col("b")])?
            .build();

        let expected = dummy_scan_node(scan_op).build();

        assert_optimized_plan_eq(plan, expected)?;

        Ok(())
    }

    /// Projection dropping: Test that projections doing reordering are not dropped.
    #[test]
    fn test_dont_drop_projection() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]);
        let proj = vec![col("b"), col("a")];
        let plan = dummy_scan_node(scan_op.clone())
            .select(proj.clone())?
            .build();

        let expected = dummy_scan_node(scan_op).select(proj)?.build();

        assert_optimized_plan_eq(plan, expected)?;

        Ok(())
    }

    /// Projection<-Source
    #[test]
    fn test_projection_source() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]);
        let proj = vec![col("b").add(lit(3))];
        let plan = dummy_scan_node(scan_op.clone())
            .select(proj.clone())?
            .build();

        let proj_pushdown = vec!["b".to_string()];
        let expected = dummy_scan_node_with_pushdowns(
            scan_op,
            Pushdowns::default().with_columns(Some(Arc::new(proj_pushdown))),
        )
        .select(proj)?
        .build();

        assert_optimized_plan_eq(plan, expected)?;

        Ok(())
    }

    /// Projection<-Projection column pruning
    #[test]
    fn test_projection_projection() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]);
        let proj1 = vec![col("b").add(lit(3)), col("a"), col("a").alias("x")];
        let proj2 = vec![col("a"), col("b"), col("b").alias("c")];
        let plan = dummy_scan_node(scan_op.clone())
            .select(proj1)?
            .select(proj2.clone())?
            .build();

        let new_proj1 = vec![col("b").add(lit(3)), col("a")];
        let expected = dummy_scan_node(scan_op)
            .select(new_proj1)?
            .select(proj2)?
            .build();

        assert_optimized_plan_eq(plan, expected)?;

        Ok(())
    }

    /// Projection<-Aggregation column pruning
    #[test]
    fn test_projection_aggregation() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
            Field::new("c", DataType::Int64),
        ]);
        let agg = vec![col("a").mean(), col("b").mean()];
        let group_by = vec![col("c")];
        let proj = vec![col("a")];
        let plan = dummy_scan_node(scan_op.clone())
            .aggregate(agg, group_by.clone())?
            .select(proj.clone())?
            .build();

        let proj_pushdown = vec!["a".to_string(), "c".to_string()];
        let new_agg = vec![col("a").mean()];
        let expected = dummy_scan_node_with_pushdowns(
            scan_op,
            Pushdowns::default().with_columns(Some(Arc::new(proj_pushdown))),
        )
        .aggregate(new_agg, group_by)?
        .select(proj)?
        .build();

        assert_optimized_plan_eq(plan, expected)?;

        Ok(())
    }

    /// Projection<-X pushes down the combined required columns
    #[test]
    fn test_projection_pushdown() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Boolean),
            Field::new("c", DataType::Int64),
        ]);
        let pred = col("b");
        let proj = vec![col("a")];
        let plan = dummy_scan_node(scan_op.clone())
            .filter(pred.clone())?
            .select(proj.clone())?
            .build();

        let proj_pushdown = vec!["a".to_string(), "b".to_string()];
        let expected = dummy_scan_node_with_pushdowns(
            scan_op,
            Pushdowns::default().with_columns(Some(Arc::new(proj_pushdown))),
        )
        .filter(pred)?
        .select(proj)?
        .build();

        assert_optimized_plan_eq(plan, expected)?;

        Ok(())
    }

    /// Projection does not push down past monotonically increasing id
    #[test]
    fn test_projection_no_pushdown_monotonically_increasing_id() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .add_monotonically_increasing_id(Some("id"))?
            .select(vec![col("id")])?
            .build();
        let expected = plan.clone();
        assert_optimized_plan_eq(plan, expected)?;

        Ok(())
    }

    /// Projection<-ActorPoolProject prunes columns from the ActorPoolProject
    #[test]
    fn test_projection_pushdown_into_actorpoolproject() -> DaftResult<()> {
        use common_resource_request::ResourceRequest;
        use daft_dsl::{
            functions::{
                python::{PythonUDF, StatefulPythonUDF},
                FunctionExpr,
            },
            Expr,
        };

        use crate::ops::{ActorPoolProject, Project};

        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Boolean),
            Field::new("c", DataType::Int64),
        ]);
        let scan_node = dummy_scan_node(scan_op.clone());
        let mock_stateful_udf = Expr::Function {
            func: FunctionExpr::Python(PythonUDF::Stateful(StatefulPythonUDF {
                name: Arc::new("my-udf".to_string()),
                stateful_partial_func: RuntimePyObject::new_testing_none(),
                num_expressions: 1,
                return_dtype: DataType::Utf8,
                resource_request: Some(ResourceRequest::default_cpu()),
                batch_size: None,
                concurrency: Some(8),
                init_args: None,
                runtime_binding: UDFRuntimeBinding::Unbound,
            })),
            inputs: vec![col("c")],
        }
        .arced();

        // Select the `udf_results` column, so the ActorPoolProject should apply column pruning to the other columns
        let actor_pool_project = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            scan_node.build(),
            vec![col("a"), col("b"), mock_stateful_udf.alias("udf_results")],
        )?)
        .arced();
        let project = LogicalPlan::Project(Project::try_new(
            actor_pool_project,
            vec![col("udf_results")],
        )?)
        .arced();

        let expected_actor_pool_project = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            dummy_scan_node_with_pushdowns(
                scan_op,
                Pushdowns::default().with_columns(Some(Arc::new(vec!["c".to_string()]))),
            )
            .build(),
            vec![mock_stateful_udf.alias("udf_results")],
        )?)
        .arced();

        assert_optimized_plan_eq(project, expected_actor_pool_project)?;
        Ok(())
    }

    /// Projection<-ActorPoolProject<-ActorPoolProject prunes columns from both ActorPoolProjects
    #[test]
    fn test_projection_pushdown_into_double_actorpoolproject() -> DaftResult<()> {
        use common_resource_request::ResourceRequest;
        use daft_dsl::{
            functions::{
                python::{PythonUDF, StatefulPythonUDF},
                FunctionExpr,
            },
            Expr,
        };

        use crate::ops::{ActorPoolProject, Project};

        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Boolean),
            Field::new("c", DataType::Int64),
        ]);
        let scan_node = dummy_scan_node(scan_op.clone()).build();
        let mock_stateful_udf = Expr::Function {
            func: FunctionExpr::Python(PythonUDF::Stateful(StatefulPythonUDF {
                name: Arc::new("my-udf".to_string()),
                stateful_partial_func: RuntimePyObject::new_testing_none(),
                num_expressions: 1,
                return_dtype: DataType::Utf8,
                resource_request: Some(ResourceRequest::default_cpu()),
                batch_size: None,
                concurrency: Some(8),
                init_args: None,
                runtime_binding: UDFRuntimeBinding::Unbound,
            })),
            inputs: vec![col("a")],
        }
        .arced();

        // Select the `udf_results` column, so the ActorPoolProject should apply column pruning to the other columns
        let plan = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            scan_node,
            vec![col("a"), col("b"), mock_stateful_udf.alias("udf_results_0")],
        )?)
        .arced();

        let plan = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            plan,
            vec![
                col("a"),
                col("b"),
                col("udf_results_0"),
                mock_stateful_udf.alias("udf_results_1"),
            ],
        )?)
        .arced();

        let plan = LogicalPlan::Project(Project::try_new(
            plan,
            vec![
                col("udf_results_0").alias("udf_results_0_alias"),
                col("udf_results_1"),
            ],
        )?)
        .arced();

        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            dummy_scan_node_with_pushdowns(
                scan_op,
                Pushdowns::default().with_columns(Some(Arc::new(vec!["a".to_string()]))),
            )
            .build(),
            // col("b") is pruned
            vec![mock_stateful_udf.alias("udf_results_0"), col("a")],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                // Absorbed a non-computational expression (alias) from the Projection
                col("udf_results_0").alias("udf_results_0_alias"),
                mock_stateful_udf.alias("udf_results_1"),
            ],
        )?)
        .arced();

        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Projection<-ActorPoolProject prunes ActorPoolProject entirely if the stateful projection column is pruned
    #[test]
    fn test_projection_pushdown_into_actorpoolproject_completely_removed() -> DaftResult<()> {
        use common_resource_request::ResourceRequest;
        use daft_dsl::{
            functions::{
                python::{PythonUDF, StatefulPythonUDF},
                FunctionExpr,
            },
            Expr,
        };

        use crate::ops::{ActorPoolProject, Project};

        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Boolean),
            Field::new("c", DataType::Int64),
        ]);
        let scan_node = dummy_scan_node(scan_op.clone()).build();
        let mock_stateful_udf = Expr::Function {
            func: FunctionExpr::Python(PythonUDF::Stateful(StatefulPythonUDF {
                name: Arc::new("my-udf".to_string()),
                stateful_partial_func: RuntimePyObject::new_testing_none(),
                num_expressions: 1,
                return_dtype: DataType::Utf8,
                resource_request: Some(ResourceRequest::default_cpu()),
                batch_size: None,
                concurrency: Some(8),
                init_args: None,
                runtime_binding: UDFRuntimeBinding::Unbound,
            })),
            inputs: vec![col("c")],
        }
        .arced();

        // Select only col("a"), so the ActorPoolProject node is now redundant and should be removed
        let actor_pool_project = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            scan_node,
            vec![col("a"), col("b"), mock_stateful_udf.alias("udf_results")],
        )?)
        .arced();
        let project =
            LogicalPlan::Project(Project::try_new(actor_pool_project, vec![col("a")])?).arced();

        // Optimized plan will push the projection all the way down into the scan
        let expected_scan = dummy_scan_node_with_pushdowns(
            scan_op.clone(),
            Pushdowns {
                limit: None,
                partition_filters: None,
                columns: Some(Arc::new(vec!["a".to_string()])),
                filters: None,
            },
        )
        .build();

        assert_optimized_plan_eq(project, expected_scan)?;
        Ok(())
    }
}
