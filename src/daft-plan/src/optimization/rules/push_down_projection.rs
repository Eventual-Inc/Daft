use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;

use daft_core::schema::Schema;
use daft_dsl::{optimization::replace_columns_with_expressions, Expr};
use indexmap::IndexSet;

use crate::{
    logical_ops::{Aggregate, Project, Source},
    LogicalPlan, ResourceRequest,
};

use super::{ApplyOrder, OptimizerRule, Transformed};

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
                    .all(|(expr, upstream_col)| match expr {
                        Expr::Column(colname) => colname.as_ref() == upstream_col,
                        _ => false,
                    })
        };
        if projection_is_noop {
            // Projection discarded but new root node has not been looked at;
            // look at the new root node.
            let new_plan = self
                .try_optimize(upstream_plan.clone())?
                .or(Transformed::Yes(upstream_plan.clone()));
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
                .flat_map(|e| {
                    e.input_mapping().map_or_else(
                        // None means computation required -> Some(colname)
                        || Some(e.name().unwrap().to_string()),
                        // Some(computation not required) -> None
                        |_| None,
                    )
                })
                .collect::<IndexSet<_>>();

            // For each of them, make sure they are used only once in this downstream projection.
            let mut exprs_to_walk: Vec<Arc<Expr>> = projection
                .projection
                .iter()
                .map(|e| e.clone().into())
                .collect();

            let mut upstream_computations_used = IndexSet::new();
            let mut okay_to_merge = true;

            while !exprs_to_walk.is_empty() {
                exprs_to_walk = exprs_to_walk
                    .iter()
                    .flat_map(|expr| {
                        // If it's a reference for a column that requires computation,
                        // record it.
                        if okay_to_merge && let Expr::Column(name) = expr.as_ref() && upstream_computations.contains(name.as_ref()) {
                            okay_to_merge = okay_to_merge
                                && upstream_computations_used.insert(name.to_string())
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
                    .map(|e| (e.name().unwrap().to_string(), e.clone()))
                    .collect::<HashMap<_, _>>();

                // Merge the projections by applying the upstream expression substitutions
                // to the current projection.
                let merged_projection = projection
                    .projection
                    .iter()
                    .map(|e| replace_columns_with_expressions(e, &upstream_names_to_exprs))
                    .collect();

                // Make a new projection node with the merged projections.
                let new_plan: LogicalPlan = Project::try_new(
                    upstream_projection.input.clone(),
                    merged_projection,
                    ResourceRequest::max(&[
                        &upstream_projection.resource_request,
                        &projection.resource_request,
                    ]),
                )?
                .into();
                let new_plan: Arc<LogicalPlan> = new_plan.into();

                // Root node is changed, look at it again.
                let new_plan = self
                    .try_optimize(new_plan.clone())?
                    .or(Transformed::Yes(new_plan.clone()));
                return Ok(new_plan);
            }
        }

        match upstream_plan.as_ref() {
            LogicalPlan::Source(source) => {
                // Prune unnecessary columns directly from the source.
                let [required_columns] = &plan.required_columns()[..] else {
                    panic!()
                };
                if required_columns.len() < upstream_schema.names().len() {
                    let pruned_upstream_schema = upstream_schema
                        .fields
                        .iter()
                        .filter_map(|(name, field)| {
                            required_columns.contains(name).then(|| field.clone())
                        })
                        .collect::<Vec<_>>();
                    let schema = Schema::new(pruned_upstream_schema)?;
                    let new_source: LogicalPlan = Source::new(
                        schema.into(),
                        source.source_info.clone(),
                        source.partition_spec.clone(),
                        source.limit,
                    )
                    .into();

                    let new_plan = plan.with_new_children(&[new_source.into()]);
                    // Retry optimization now that the upstream node is different.
                    let new_plan = self
                        .try_optimize(new_plan.clone())?
                        .or(Transformed::Yes(new_plan));
                    Ok(new_plan)
                } else {
                    Ok(Transformed::No(plan))
                }
            }
            LogicalPlan::Project(upstream_projection) => {
                // Prune columns from the child projection that are not used in this projection.
                let required_columns = &plan.required_columns()[0];
                if required_columns.len() < upstream_schema.names().len() {
                    let pruned_upstream_projections = upstream_projection
                        .projection
                        .iter()
                        .filter_map(|e| {
                            required_columns
                                .contains(e.name().unwrap())
                                .then(|| e.clone())
                        })
                        .collect::<Vec<_>>();

                    let new_upstream: LogicalPlan = Project::try_new(
                        upstream_projection.input.clone(),
                        pruned_upstream_projections,
                        upstream_projection.resource_request.clone(),
                    )?
                    .into();

                    let new_plan = plan.with_new_children(&[new_upstream.into()]);
                    // Retry optimization now that the upstream node is different.
                    let new_plan = self
                        .try_optimize(new_plan.clone())?
                        .or(Transformed::Yes(new_plan));
                    Ok(new_plan)
                } else {
                    Ok(Transformed::No(plan))
                }
            }
            LogicalPlan::Aggregate(aggregate) => {
                // Prune unnecessary columns from the child aggregate.
                let required_columns = &plan.required_columns()[0];
                let pruned_aggregate_exprs = aggregate
                    .aggregations
                    .iter()
                    .filter_map(|e| {
                        required_columns
                            .contains(e.name().unwrap())
                            .then(|| e.clone())
                    })
                    .collect::<Vec<_>>();

                if pruned_aggregate_exprs.len() < aggregate.aggregations.len() {
                    let new_upstream: LogicalPlan = Aggregate::try_new(
                        aggregate.input.clone(),
                        pruned_aggregate_exprs,
                        aggregate.groupby.clone(),
                    )?
                    .into();

                    let new_plan = plan.with_new_children(&[new_upstream.into()]);
                    // Retry optimization now that the upstream node is different.
                    let new_plan = self
                        .try_optimize(new_plan.clone())?
                        .or(Transformed::Yes(new_plan));
                    Ok(new_plan)
                } else {
                    Ok(Transformed::No(plan))
                }
            }
            LogicalPlan::Sort(..)
            | LogicalPlan::Repartition(..)
            | LogicalPlan::Coalesce(..)
            | LogicalPlan::Limit(..)
            | LogicalPlan::Filter(..)
            | LogicalPlan::Explode(..) => {
                // Get required columns from projection and upstream.
                let combined_dependencies = plan
                    .required_columns()
                    .iter()
                    .flatten()
                    .chain(upstream_plan.required_columns().iter().flatten())
                    .cloned()
                    .collect::<IndexSet<_>>();

                // Skip optimization if no columns would be pruned.
                let grand_upstream_plan = upstream_plan.children()[0];
                let grand_upstream_columns = grand_upstream_plan.schema().names();
                if grand_upstream_columns.len() == combined_dependencies.len() {
                    return Ok(Transformed::No(plan));
                }

                let new_subprojection: LogicalPlan = {
                    let pushdown_column_exprs = combined_dependencies
                        .into_iter()
                        .map(|s| Expr::Column(s.into()))
                        .collect::<Vec<_>>();

                    Project::try_new(
                        grand_upstream_plan.clone(),
                        pushdown_column_exprs,
                        Default::default(),
                    )?
                    .into()
                };

                let new_upstream = upstream_plan.with_new_children(&[new_subprojection.into()]);
                let new_plan = plan.with_new_children(&[new_upstream]);
                // Retry optimization now that the upstream node is different.
                let new_plan = self
                    .try_optimize(new_plan.clone())?
                    .or(Transformed::Yes(new_plan));
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
                let grand_upstream_plan = upstream_plan.children()[0];
                let grand_upstream_columns = grand_upstream_plan.schema().names();
                if grand_upstream_columns.len() == combined_dependencies.len() {
                    return Ok(Transformed::No(plan));
                }

                let pushdown_column_exprs = combined_dependencies
                    .into_iter()
                    .map(|s| Expr::Column(s.into()))
                    .collect::<Vec<_>>();
                let new_left_subprojection: LogicalPlan = {
                    Project::try_new(
                        concat.input.clone(),
                        pushdown_column_exprs.clone(),
                        Default::default(),
                    )?
                    .into()
                };
                let new_right_subprojection: LogicalPlan = {
                    Project::try_new(
                        concat.other.clone(),
                        pushdown_column_exprs.clone(),
                        Default::default(),
                    )?
                    .into()
                };

                let new_upstream = upstream_plan.with_new_children(&[
                    new_left_subprojection.into(),
                    new_right_subprojection.into(),
                ]);
                let new_plan = plan.with_new_children(&[new_upstream]);
                // Retry optimization now that the upstream node is different.
                let new_plan = self
                    .try_optimize(new_plan.clone())?
                    .or(Transformed::Yes(new_plan));
                Ok(new_plan)
            }
            LogicalPlan::Join(join) => {
                // Get required columns from projection and both upstreams.
                let [projection_required_columns] = &plan.required_columns()[..] else {
                    panic!()
                };
                let [left_dependencies, right_dependencies] = &upstream_plan.required_columns()[..]
                else {
                    panic!()
                };

                let left_upstream_names = join
                    .left
                    .schema()
                    .names()
                    .iter()
                    .cloned()
                    .collect::<IndexSet<_>>();
                let right_upstream_names = join
                    .right
                    .schema()
                    .names()
                    .iter()
                    .cloned()
                    .collect::<IndexSet<_>>();

                let right_combined_dependencies = projection_required_columns
                    .iter()
                    .filter_map(|colname| join.right_input_mapping.get(colname))
                    .chain(right_dependencies.iter())
                    .cloned()
                    .collect::<IndexSet<_>>();

                let left_combined_dependencies = projection_required_columns
                    .iter()
                    .filter_map(|colname| left_upstream_names.get(colname))
                    .chain(left_dependencies.iter())
                    // We also have to keep any name conflict columns referenced by the right side.
                    // E.g. if the user wants "right.c", left must also provide "c", or "right.c" disappears.
                    // This is mostly an artifact of https://github.com/Eventual-Inc/Daft/issues/1303
                    .chain(
                        right_combined_dependencies
                            .iter()
                            .filter_map(|rname| left_upstream_names.get(rname)),
                    )
                    .cloned()
                    .collect::<IndexSet<_>>();

                // For each upstream, see if a non-vacuous pushdown is possible.
                let maybe_new_left_upstream: Option<Arc<LogicalPlan>> = {
                    if left_combined_dependencies.len() < left_upstream_names.len() {
                        let pushdown_column_exprs = left_combined_dependencies
                            .into_iter()
                            .map(|s| Expr::Column(s.into()))
                            .collect::<Vec<_>>();
                        let new_project: LogicalPlan = Project::try_new(
                            join.left.clone(),
                            pushdown_column_exprs,
                            Default::default(),
                        )?
                        .into();
                        Some(new_project.into())
                    } else {
                        None
                    }
                };

                let maybe_new_right_upstream: Option<Arc<LogicalPlan>> = {
                    if right_combined_dependencies.len() < right_upstream_names.len() {
                        let pushdown_column_exprs = right_combined_dependencies
                            .into_iter()
                            .map(|s| Expr::Column(s.into()))
                            .collect::<Vec<_>>();
                        let new_project: LogicalPlan = Project::try_new(
                            join.right.clone(),
                            pushdown_column_exprs,
                            Default::default(),
                        )?
                        .into();
                        Some(new_project.into())
                    } else {
                        None
                    }
                };

                // If either pushdown is possible, create a new Join node.
                if maybe_new_left_upstream.is_some() || maybe_new_right_upstream.is_some() {
                    let new_left_upstream = maybe_new_left_upstream.unwrap_or(join.left.clone());
                    let new_right_upstream = maybe_new_right_upstream.unwrap_or(join.right.clone());
                    let new_join =
                        upstream_plan.with_new_children(&[new_left_upstream, new_right_upstream]);
                    let new_plan = plan.with_new_children(&[new_join]);
                    // Retry optimization now that the upstream node is different.
                    let new_plan = self
                        .try_optimize(new_plan.clone())?
                        .or(Transformed::Yes(new_plan));
                    Ok(new_plan)
                } else {
                    Ok(Transformed::No(plan))
                }
            }
            LogicalPlan::Distinct(_) => {
                // Cannot push down past a Distinct,
                // since Distinct implicitly requires all parent columns.
                Ok(Transformed::No(plan))
            }
            LogicalPlan::Sink(_) => {
                panic!("Bad projection due to upstream sink node: {:?}", projection)
            }
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
                    .map(|s| Expr::Column(s.clone().into()))
                    .collect::<Vec<_>>();

                Project::try_new(
                    upstream_plan.clone(),
                    pushdown_column_exprs,
                    Default::default(),
                )?
                .into()
            };

            let new_aggregation = plan.with_new_children(&[new_subprojection.into()]);
            Ok(Transformed::Yes(new_aggregation))
        } else {
            Ok(Transformed::No(plan))
        }
    }
}

impl OptimizerRule for PushDownProjection {
    fn apply_order(&self) -> ApplyOrder {
        ApplyOrder::TopDown
    }

    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        match plan.as_ref() {
            LogicalPlan::Project(projection) => self.try_optimize_project(projection, plan.clone()),
            // Aggregations also do column projection
            LogicalPlan::Aggregate(aggregation) => {
                self.try_optimize_aggregation(aggregation, plan.clone())
            }
            _ => Ok(Transformed::No(plan)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_core::{datatypes::Field, DataType};
    use daft_dsl::{col, lit};

    use crate::{
        optimization::{
            optimizer::{RuleBatch, RuleExecutionStrategy},
            rules::PushDownProjection,
            Optimizer,
        },
        test::dummy_scan_node,
        LogicalPlan,
    };

    /// Helper that creates an optimizer with the PushDownProjection rule registered, optimizes
    /// the provided plan with said optimizer, and compares the optimized plan's repr with
    /// the provided expected repr.
    fn assert_optimized_plan_eq(plan: Arc<LogicalPlan>, expected: &str) -> DaftResult<()> {
        let optimizer = Optimizer::with_rule_batches(
            vec![RuleBatch::new(
                vec![Box::new(PushDownProjection::new())],
                RuleExecutionStrategy::Once,
            )],
            Default::default(),
        );
        let optimized_plan = optimizer
            .optimize_with_rules(
                optimizer.rule_batches[0].rules.as_slice(),
                plan.clone(),
                &optimizer.rule_batches[0].order,
            )?
            .unwrap()
            .clone();
        assert_eq!(optimized_plan.repr_indent(), expected);

        Ok(())
    }

    /// Projection merging: Ensure factored projections do not get merged.
    #[test]
    fn test_merge_does_not_unfactor() -> DaftResult<()> {
        let a2 = col("a") + col("a");
        let a4 = &a2 + &a2;
        let a8 = &a4 + &a4;
        let expressions = vec![a8.alias("x")];
        let unoptimized = dummy_scan_node(vec![Field::new("a", DataType::Int64)])
            .project(expressions, Default::default())?
            .build();

        let expected = unoptimized.repr_indent();
        assert_optimized_plan_eq(unoptimized, expected.as_str())?;
        Ok(())
    }

    /// Projection merging: Ensure merging happens even when there is computation
    /// in both the parent and the child.
    #[test]
    fn test_merge_projections() -> DaftResult<()> {
        let unoptimized = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ])
        .project(
            vec![col("a") + lit(1), col("b") + lit(2), col("a").alias("c")],
            Default::default(),
        )?
        .project(
            vec![col("a") + lit(3), col("b"), col("c") + lit(4)],
            Default::default(),
        )?
        .build();

        let expected = "\
        Project: [col(a) + lit(1)] + lit(3), col(b) + lit(2), col(a) + lit(4), Partition spec = PartitionSpec { scheme: Unknown, num_partitions: 1, by: None }\
        \n  Source: Json, File paths = [/foo], File schema = a (Int64), b (Int64), Format-specific config = Json(JsonSourceConfig), Storage config = Native(NativeStorageConfig { io_config: None }), Output schema = a (Int64), b (Int64)";
        assert_optimized_plan_eq(unoptimized, expected)?;
        Ok(())
    }

    /// Projection dropping: Test that a no-op projection is dropped.
    #[test]
    fn test_drop_projection() -> DaftResult<()> {
        let unoptimized = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ])
        .project(vec![col("a"), col("b")], Default::default())?
        .build();

        let expected = "\
        Source: Json, File paths = [/foo], File schema = a (Int64), b (Int64), Format-specific config = Json(JsonSourceConfig), Storage config = Native(NativeStorageConfig { io_config: None }), Output schema = a (Int64), b (Int64)";
        assert_optimized_plan_eq(unoptimized, expected)?;

        Ok(())
    }
    /// Projection dropping: Test that projections doing reordering are not dropped.
    #[test]
    fn test_dont_drop_projection() -> DaftResult<()> {
        let unoptimized = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ])
        .project(vec![col("b"), col("a")], Default::default())?
        .build();

        let expected = "\
        Project: col(b), col(a), Partition spec = PartitionSpec { scheme: Unknown, num_partitions: 1, by: None }\
        \n  Source: Json, File paths = [/foo], File schema = a (Int64), b (Int64), Format-specific config = Json(JsonSourceConfig), Storage config = Native(NativeStorageConfig { io_config: None }), Output schema = a (Int64), b (Int64)";
        assert_optimized_plan_eq(unoptimized, expected)?;

        Ok(())
    }

    /// Projection<-Source
    #[test]
    fn test_projection_source() -> DaftResult<()> {
        let unoptimized = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ])
        .project(vec![col("b") + lit(3)], Default::default())?
        .build();

        let expected = "\
        Project: col(b) + lit(3), Partition spec = PartitionSpec { scheme: Unknown, num_partitions: 1, by: None }\
        \n  Source: Json, File paths = [/foo], File schema = a (Int64), b (Int64), Format-specific config = Json(JsonSourceConfig), Storage config = Native(NativeStorageConfig { io_config: None }), Output schema = b (Int64)";
        assert_optimized_plan_eq(unoptimized, expected)?;

        Ok(())
    }

    /// Projection<-Projection column pruning
    #[test]
    fn test_projection_projection() -> DaftResult<()> {
        let unoptimized = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ])
        .project(
            vec![col("b") + lit(3), col("a"), col("a").alias("x")],
            Default::default(),
        )?
        .project(
            vec![col("a"), col("b"), col("b").alias("c")],
            Default::default(),
        )?
        .build();

        let expected = "\
        Project: col(a), col(b), col(b) AS c, Partition spec = PartitionSpec { scheme: Unknown, num_partitions: 1, by: None }\
        \n  Project: col(b) + lit(3), col(a), Partition spec = PartitionSpec { scheme: Unknown, num_partitions: 1, by: None }\
        \n    Source: Json, File paths = [/foo], File schema = a (Int64), b (Int64), Format-specific config = Json(JsonSourceConfig), Storage config = Native(NativeStorageConfig { io_config: None }), Output schema = a (Int64), b (Int64)";
        assert_optimized_plan_eq(unoptimized, expected)?;

        Ok(())
    }

    /// Projection<-Aggregation column pruning
    #[test]
    fn test_projection_aggregation() -> DaftResult<()> {
        let unoptimized = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
            Field::new("c", DataType::Int64),
        ])
        .aggregate(vec![col("a").mean(), col("b").mean()], vec![col("c")])?
        .project(vec![col("a")], Default::default())?
        .build();

        let expected = "\
        Project: col(a), Partition spec = PartitionSpec { scheme: Unknown, num_partitions: 1, by: None }\
        \n  Aggregation: mean(col(a)), Group by = col(c), Output schema = c (Int64), a (Float64)\
        \n    Source: Json, File paths = [/foo], File schema = a (Int64), b (Int64), c (Int64), Format-specific config = Json(JsonSourceConfig), Storage config = Native(NativeStorageConfig { io_config: None }), Output schema = a (Int64), c (Int64)";
        assert_optimized_plan_eq(unoptimized, expected)?;

        Ok(())
    }

    /// Projection<-X pushes down the combined required columns
    #[test]
    fn test_projection_pushdown() -> DaftResult<()> {
        let unoptimized = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Boolean),
            Field::new("c", DataType::Int64),
        ])
        .filter(col("b"))?
        .project(vec![col("a")], Default::default())?
        .build();

        let expected = "\
        Project: col(a), Partition spec = PartitionSpec { scheme: Unknown, num_partitions: 1, by: None }\
        \n  Filter: col(b)\
        \n    Source: Json, File paths = [/foo], File schema = a (Int64), b (Boolean), c (Int64), Format-specific config = Json(JsonSourceConfig), Storage config = Native(NativeStorageConfig { io_config: None }), Output schema = a (Int64), b (Boolean)";
        assert_optimized_plan_eq(unoptimized, expected)?;

        Ok(())
    }
}
