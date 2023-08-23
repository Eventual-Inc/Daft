use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use common_error::DaftResult;
use daft_dsl::{
    col,
    optimization::{get_required_columns, replace_columns_with_expressions},
    Expr,
};

use crate::{
    ops::{Concat, Filter, Project},
    LogicalPlan,
};

use super::{
    utils::{conjuct, split_conjuction},
    ApplyOrder, OptimizerRule,
};

/// Optimization rules for pushing Filters further into the logical plan.

#[derive(Default)]
pub struct PushDownFilter {}

impl PushDownFilter {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for PushDownFilter {
    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn try_optimize(&self, plan: &LogicalPlan) -> DaftResult<Option<Arc<LogicalPlan>>> {
        let filter = match plan {
            LogicalPlan::Filter(filter) => filter,
            _ => return Ok(None),
        };
        let child_plan = filter.input.as_ref();
        let new_plan = match child_plan {
            LogicalPlan::Filter(child_filter) => {
                // Combine filters.
                //
                // Filter-Filter --> Filter

                // Split predicate expression on conjunctions (ANDs).
                let parent_predicates = split_conjuction(&filter.predicate);
                let predicate_set: HashSet<&&Expr> = parent_predicates.iter().collect();
                // Add child predicate expressions to parent predicate expressions, eliminating duplicates.
                let new_predicates = parent_predicates
                    .iter()
                    .chain(
                        split_conjuction(&child_filter.predicate)
                            .iter()
                            .filter(|e| !predicate_set.contains(e)),
                    )
                    .map(|e| (*e).clone())
                    .collect::<Vec<_>>();
                // Reconjunct predicate expressions.
                let new_predicate = conjuct(new_predicates).unwrap();
                let new_filter: LogicalPlan =
                    Filter::new(new_predicate, child_filter.input.clone()).into();
                self.try_optimize(&new_filter)?.unwrap_or(new_filter.into())
            }
            LogicalPlan::Project(child_project) => {
                // Commute filter with projection if predicate only depends on projection columns that
                // don't involve compute.
                //
                // Filter-Projection --> {Filter-}Projection-Filter
                let predicates = split_conjuction(&filter.predicate);
                let projection_input_mapping = child_project
                    .projection
                    .iter()
                    .filter_map(|e| {
                        e.input_mapping()
                            .map(|s| (e.name().unwrap().to_string(), col(s)))
                    })
                    .collect::<HashMap<String, Expr>>();
                // Split predicate expressions into those that don't depend on projection compute (can_push) and those
                // that do (can_not_push).
                // TODO(Clark): Push Filters depending on Projection columns involving compute if those expressions are
                // (1) determinstic && (pure || idempotent),
                // (2) inexpensive to recompute.
                // This can be done by rewriting the Filter predicate expression to contain the relevant Projection expression.
                let mut can_push = vec![];
                let mut can_not_push = vec![];
                for predicate in predicates {
                    let predicate_cols = get_required_columns(predicate);
                    if predicate_cols
                        .iter()
                        .all(|col| projection_input_mapping.contains_key(col))
                    {
                        // Can push predicate through expression.
                        let new_predicate =
                            replace_columns_with_expressions(predicate, &projection_input_mapping);
                        can_push.push(new_predicate);
                    } else {
                        // Can't push predicate expression through projection.
                        can_not_push.push(predicate.clone());
                    }
                }
                if can_push.is_empty() {
                    // No predicate expressions can be pushed through projection.
                    return Ok(None);
                }
                // Create new Filter with predicates that can be pushed past Projection.
                let predicates_to_push = conjuct(can_push).unwrap();
                let push_down_filter: LogicalPlan =
                    Filter::new(predicates_to_push, child_project.input.clone()).into();
                // Create new Projection.
                let new_projection: LogicalPlan = Project::new(
                    child_project.projection.clone(),
                    child_project.resource_request.clone(),
                    push_down_filter.into(),
                )?
                .into();
                if can_not_push.is_empty() {
                    // If all Filter predicate expressions were pushable past Projection, return new
                    // Projection-Filter subplan.
                    new_projection.into()
                } else {
                    // Otherwise, add a Filter after Projection that filters with predicate expressions
                    // that couldn't be pushed past the Projection, returning a Filter-Projection-Filter subplan.
                    let post_projection_predicate = conjuct(can_not_push).unwrap();
                    let post_projection_filter: LogicalPlan =
                        Filter::new(post_projection_predicate, new_projection.into()).into();
                    post_projection_filter.into()
                }
            }
            LogicalPlan::Sort(_) | LogicalPlan::Repartition(_) | LogicalPlan::Coalesce(_) => {
                // Naive commuting with unary ops.
                let new_filter = plan.with_new_children(&[child_plan.children()[0].clone()]);
                child_plan.with_new_children(&[new_filter])
            }
            LogicalPlan::Concat(Concat { input, other }) => {
                // Push filter into each side of the concat.
                let new_input: LogicalPlan =
                    Filter::new(filter.predicate.clone(), input.clone()).into();
                let new_other: LogicalPlan =
                    Filter::new(filter.predicate.clone(), other.clone()).into();
                let new_concat: LogicalPlan =
                    Concat::new(new_other.into(), new_input.into()).into();
                new_concat.into()
            }
            LogicalPlan::Join(child_join) => {
                // Push filter into each side of the join.
                // TODO(Clark): Merge filter predicate with on predicate, if present.
                // TODO(Clark): Duplicate filters for joined columns so filters can be pushed down to both sides.

                // Get all input columns for predicate.
                let predicate_cols: HashSet<_> = get_required_columns(&filter.predicate)
                    .iter()
                    .cloned()
                    .collect();
                // Only push the filter into the left side of the join if the left side of the join has all columns
                // required by the predicate.
                let left_cols: HashSet<_> =
                    child_join.input.schema().names().iter().cloned().collect();
                let can_push_left = left_cols
                    .intersection(&predicate_cols)
                    .collect::<HashSet<_>>()
                    .len()
                    == predicate_cols.len();
                // Only push the filter into the right side of the join if the right side of the join has all columns
                // required by the predicate.
                let right_cols: HashSet<_> =
                    child_join.right.schema().names().iter().cloned().collect();
                let can_push_right = right_cols
                    .intersection(&predicate_cols)
                    .collect::<HashSet<_>>()
                    .len()
                    == predicate_cols.len();
                if !can_push_left && !can_push_right {
                    return Ok(None);
                }
                let new_left: Arc<LogicalPlan> = if can_push_left {
                    LogicalPlan::from(Filter::new(
                        filter.predicate.clone(),
                        child_join.input.clone(),
                    ))
                    .into()
                } else {
                    child_join.input.clone()
                };
                let new_right: Arc<LogicalPlan> = if can_push_right {
                    LogicalPlan::from(Filter::new(
                        filter.predicate.clone(),
                        child_join.right.clone(),
                    ))
                    .into()
                } else {
                    child_join.right.clone()
                };
                child_plan.with_new_children(&[new_left, new_right])
            }
            _ => return Ok(None),
        };
        Ok(Some(new_plan))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_core::{datatypes::Field, schema::Schema, DataType};
    use daft_dsl::{col, lit};

    use crate::{
        display::TreeDisplay,
        ops::{Coalesce, Concat, Filter, Join, Project, Repartition, Sort, Source},
        optimization::{rules::PushDownFilter, Optimizer},
        source_info::{ExternalInfo, FileFormatConfig, FileInfo, SourceInfo},
        JoinType, JsonSourceConfig, LogicalPlan, PartitionScheme, PartitionSpec,
    };

    fn assert_optimized_plan_eq(plan: Arc<LogicalPlan>, expected: &str) -> DaftResult<()> {
        let optimizer = Optimizer::with_rules(vec![Arc::new(PushDownFilter::new())]);
        let optimized_plan = optimizer
            .optimize_with_rule(optimizer.rules.get(0).unwrap(), &plan)?
            .unwrap_or_else(|| plan.clone());
        let mut formatted_plan = String::new();
        optimized_plan.fmt_tree_indent_style(0, &mut formatted_plan)?;
        println!("{}", formatted_plan);
        assert_eq!(formatted_plan, expected);

        Ok(())
    }

    fn dummy_scan_node(fields: Vec<Field>) -> Source {
        let schema = Arc::new(Schema::new(fields).unwrap());
        Source::new(
            schema.clone(),
            SourceInfo::ExternalInfo(ExternalInfo::new(
                schema.clone(),
                FileInfo::new(vec!["/foo".to_string()], vec![None], vec![None]).into(),
                FileFormatConfig::Json(JsonSourceConfig {}).into(),
            ))
            .into(),
            PartitionSpec::default().into(),
        )
    }

    #[test]
    fn filter_combine_with_filter() -> DaftResult<()> {
        let source: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .into();
        let first_filter: LogicalPlan = Filter::new(col("a").lt(&lit(2)), source.into()).into();
        let second_filter: LogicalPlan =
            Filter::new(col("b").eq(&lit("foo")), first_filter.into()).into();
        let expected = "\
        Filter: [col(b) == lit(\"foo\")] & [col(a) < lit(2)]\
        \n  Source: \"Json\", File paths = /foo, File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Output schema = a (Int64), b (Utf8)";
        assert_optimized_plan_eq(second_filter.into(), expected)?;
        Ok(())
    }

    #[test]
    fn filter_commutes_with_projection() -> DaftResult<()> {
        let source: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .into();
        let projection: LogicalPlan =
            Project::new(vec![col("a")], Default::default(), source.into())?.into();
        let filter: LogicalPlan = Filter::new(col("a").lt(&lit(2)), projection.into()).into();
        let expected = "\
        Project: col(a)\
        \n  Filter: col(a) < lit(2)\
        \n    Source: \"Json\", File paths = /foo, File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Output schema = a (Int64), b (Utf8)";
        assert_optimized_plan_eq(filter.into(), expected)?;
        Ok(())
    }

    #[test]
    fn filter_commutes_with_projection_multi() -> DaftResult<()> {
        let source: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .into();
        let projection: LogicalPlan =
            Project::new(vec![col("a"), col("b")], Default::default(), source.into())?.into();
        let filter: LogicalPlan = Filter::new(
            col("a").lt(&lit(2)).and(&col("b").eq(&lit("foo"))),
            projection.into(),
        )
        .into();
        let expected = "\
        Project: col(a), col(b)\
        \n  Filter: [col(a) < lit(2)] & [col(b) == lit(\"foo\")]\
        \n    Source: \"Json\", File paths = /foo, File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Output schema = a (Int64), b (Utf8)";
        assert_optimized_plan_eq(filter.into(), expected)?;
        Ok(())
    }

    #[test]
    fn filter_does_not_commute_with_projection_if_compute() -> DaftResult<()> {
        let source: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .into();
        // Projection involves compute on filtered column "a".
        let projection: LogicalPlan =
            Project::new(vec![col("a") + lit(1)], Default::default(), source.into())?.into();
        let filter: LogicalPlan = Filter::new(col("a").lt(&lit(2)), projection.into()).into();
        // Filter should NOT commute with Project, since this would involve redundant computation.
        let expected = "\
        Filter: col(a) < lit(2)\
        \n  Project: col(a) + lit(1)\
        \n    Source: \"Json\", File paths = /foo, File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Output schema = a (Int64), b (Utf8)";
        assert_optimized_plan_eq(filter.into(), expected)?;
        Ok(())
    }

    // REASON - No expression attribute indicating whether deterministic && (pure || idempotent).
    #[ignore]
    #[test]
    fn filter_commutes_with_projection_deterministic_compute() -> DaftResult<()> {
        let source: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .into();
        let projection: LogicalPlan =
            Project::new(vec![col("a") + lit(1)], Default::default(), source.into())?.into();
        let filter: LogicalPlan = Filter::new(col("a").lt(&lit(2)), projection.into()).into();
        let expected = "\
        Project: col(a) + lit(1)\
        \n  Filter: [col(a) + lit(1)] < lit(2)\
        \n    Source: \"Json\", File paths = /foo, File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Output schema = a (Int64), b (Utf8)";
        assert_optimized_plan_eq(filter.into(), expected)?;
        Ok(())
    }

    #[test]
    fn filter_commutes_with_sort() -> DaftResult<()> {
        let source: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .into();
        let sort: LogicalPlan = Sort::new(vec![col("a")], vec![true], source.into()).into();
        let filter: LogicalPlan = Filter::new(col("a").lt(&lit(2)), sort.into()).into();
        let expected = "\
        Sort: Sort by = (col(a), descending)\
        \n  Filter: col(a) < lit(2)\
        \n    Source: \"Json\", File paths = /foo, File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Output schema = a (Int64), b (Utf8)";
        // TODO(Clark): For tests in which we only care about reordering of operators, maybe switch to a form that leverages the single-node display?
        // let expected = format!("{sort}\n  {filter}\n    {source}");
        assert_optimized_plan_eq(filter.into(), expected)?;
        Ok(())
    }

    #[test]
    fn filter_commutes_with_repartition() -> DaftResult<()> {
        let source: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .into();
        let repartition: LogicalPlan =
            Repartition::new(1, vec![col("a")], PartitionScheme::Hash, source.into()).into();
        let filter: LogicalPlan = Filter::new(col("a").lt(&lit(2)), repartition.into()).into();
        let expected = "\
        Repartition: Scheme = Hash, Number of partitions = 1, Partition by = col(a)\
        \n  Filter: col(a) < lit(2)\
        \n    Source: \"Json\", File paths = /foo, File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Output schema = a (Int64), b (Utf8)";
        assert_optimized_plan_eq(filter.into(), expected)?;
        Ok(())
    }

    #[test]
    fn filter_commutes_with_coalesce() -> DaftResult<()> {
        let source: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .into();
        let coalesce: LogicalPlan = Coalesce::new(1, source.into()).into();
        let filter: LogicalPlan = Filter::new(col("a").lt(&lit(2)), coalesce.into()).into();
        let expected = "\
        Coalesce: To = 1\
        \n  Filter: col(a) < lit(2)\
        \n    Source: \"Json\", File paths = /foo, File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Output schema = a (Int64), b (Utf8)";
        assert_optimized_plan_eq(filter.into(), expected)?;
        Ok(())
    }

    #[test]
    fn filter_commutes_with_concat() -> DaftResult<()> {
        let fields = vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ];
        let source1: LogicalPlan = dummy_scan_node(fields.clone()).into();
        let source2: LogicalPlan = dummy_scan_node(fields).into();
        let concat: LogicalPlan = Concat::new(source2.into(), source1.into()).into();
        let filter: LogicalPlan = Filter::new(col("a").lt(&lit(2)), concat.into()).into();
        let expected = "\
        Concat\
        \n  Filter: col(a) < lit(2)\
        \n    Source: \"Json\", File paths = /foo, File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Output schema = a (Int64), b (Utf8)\
        \n  Filter: col(a) < lit(2)\
        \n    Source: \"Json\", File paths = /foo, File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Output schema = a (Int64), b (Utf8)";
        assert_optimized_plan_eq(filter.into(), expected)?;
        Ok(())
    }

    #[test]
    fn filter_commutes_with_join_left_side() -> DaftResult<()> {
        let source1: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .into();
        let source2: LogicalPlan = dummy_scan_node(vec![
            Field::new("b", DataType::Utf8),
            Field::new("c", DataType::Float64),
        ])
        .into();
        let output_schema = source1.schema().union(source2.schema().as_ref())?;
        let join: LogicalPlan = Join::new(
            source2.into(),
            vec![col("b")],
            vec![col("b")],
            vec![],
            output_schema.into(),
            JoinType::Inner,
            source1.into(),
        )
        .into();
        let filter: LogicalPlan = Filter::new(col("a").lt(&lit(2)), join.into()).into();
        let expected = "\
        Join: Type = Inner, On = col(b), Output schema = a (Int64), b (Utf8), c (Float64)\
        \n  Filter: col(a) < lit(2)\
        \n    Source: \"Json\", File paths = /foo, File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Output schema = a (Int64), b (Utf8)\
        \n  Source: \"Json\", File paths = /foo, File schema = b (Utf8), c (Float64), Format-specific config = Json(JsonSourceConfig), Output schema = b (Utf8), c (Float64)";
        assert_optimized_plan_eq(filter.into(), expected)?;
        Ok(())
    }

    #[test]
    fn filter_commutes_with_join_right_side() -> DaftResult<()> {
        let source1: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .into();
        let source2: LogicalPlan = dummy_scan_node(vec![
            Field::new("b", DataType::Utf8),
            Field::new("c", DataType::Float64),
        ])
        .into();
        let output_schema = source1.schema().union(source2.schema().as_ref())?;
        let join: LogicalPlan = Join::new(
            source2.into(),
            vec![col("b")],
            vec![col("b")],
            vec![],
            output_schema.into(),
            JoinType::Inner,
            source1.into(),
        )
        .into();
        let filter: LogicalPlan = Filter::new(col("c").lt(&lit(2.0)), join.into()).into();
        let expected = "\
        Join: Type = Inner, On = col(b), Output schema = a (Int64), b (Utf8), c (Float64)\
        \n  Source: \"Json\", File paths = /foo, File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Output schema = a (Int64), b (Utf8)\
        \n  Filter: col(c) < lit(2.0)\
        \n    Source: \"Json\", File paths = /foo, File schema = b (Utf8), c (Float64), Format-specific config = Json(JsonSourceConfig), Output schema = b (Utf8), c (Float64)";
        assert_optimized_plan_eq(filter.into(), expected)?;
        Ok(())
    }

    #[test]
    fn filter_commutes_with_join_both_sides() -> DaftResult<()> {
        let source1: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
            Field::new("c", DataType::Float64),
        ])
        .into();
        let source2: LogicalPlan = dummy_scan_node(vec![
            Field::new("b", DataType::Utf8),
            Field::new("c", DataType::Float64),
        ])
        .into();
        let output_schema = source1.schema().union(source2.schema().as_ref())?;
        let join: LogicalPlan = Join::new(
            source2.into(),
            vec![col("b")],
            vec![col("b")],
            vec![],
            output_schema.into(),
            JoinType::Inner,
            source1.into(),
        )
        .into();
        let filter: LogicalPlan = Filter::new(col("c").lt(&lit(2.0)), join.into()).into();
        let expected = "\
        Join: Type = Inner, On = col(b), Output schema = a (Int64), b (Utf8), c (Float64)\
        \n  Filter: col(c) < lit(2.0)\
        \n    Source: \"Json\", File paths = /foo, File schema = a (Int64), b (Utf8), c (Float64), Format-specific config = Json(JsonSourceConfig), Output schema = a (Int64), b (Utf8), c (Float64)\
        \n  Filter: col(c) < lit(2.0)\
        \n    Source: \"Json\", File paths = /foo, File schema = b (Utf8), c (Float64), Format-specific config = Json(JsonSourceConfig), Output schema = b (Utf8), c (Float64)";
        assert_optimized_plan_eq(filter.into(), expected)?;
        Ok(())
    }
}
