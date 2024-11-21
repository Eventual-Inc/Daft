/// Heavily inspired by DataFusion's EliminateCrossJoin rule: https://github.com/apache/datafusion/blob/b978cf8236436038a106ed94fb0d7eaa6ba99962/datafusion/optimizer/src/eliminate_cross_join.rs
use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_core::{
    join::JoinType,
    prelude::{Schema, SchemaRef, TimeUnit},
};
use daft_dsl::{optimization::get_required_columns, Expr, ExprRef, Operator};
use daft_schema::dtype::DataType;

use super::OptimizerRule;
use crate::{
    ops::{Filter, Join, Project},
    optimization::join_key_set::JoinKeySet,
    LogicalPlan, LogicalPlanRef,
};

#[derive(Default, Debug)]
pub struct EliminateCrossJoin {}

impl EliminateCrossJoin {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateCrossJoin {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        let schema = plan.schema();
        let mut possible_join_keys = JoinKeySet::new();
        let mut all_inputs: Vec<Arc<LogicalPlan>> = vec![];
        let plan = Arc::unwrap_or_clone(plan);

        let parent_predicate = if let LogicalPlan::Filter(filter) = plan {
            // if input isn't a join that can potentially be rewritten
            // avoid unwrapping the input
            let rewriteable = matches!(
                filter.input.as_ref(),
                LogicalPlan::Join(Join {
                    join_type: JoinType::Inner,
                    join_strategy: None,
                    // TODO: consider support eliminate cross join with null_equals_nulls
                    null_equals_nulls: None,
                    ..
                })
            );
            if !rewriteable {
                return rewrite_children(self, Arc::new(LogicalPlan::Filter(filter)));
            }
            if !can_flatten_join_inputs(filter.input.as_ref()) {
                return Ok(Transformed::no(Arc::new(LogicalPlan::Filter(filter))));
            }
            let Filter { input, predicate } = filter;
            flatten_join_inputs(
                Arc::unwrap_or_clone(input),
                &mut possible_join_keys,
                &mut all_inputs,
            )?;
            extract_possible_join_keys(&predicate, &mut possible_join_keys);
            Some(predicate)
        } else if matches!(
            plan,
            LogicalPlan::Join(Join {
                join_type: JoinType::Inner,
                join_strategy: None,
                // TODO: consider support eliminate cross join with null_equals_nulls
                null_equals_nulls: None,
                ..
            })
        ) {
            if !can_flatten_join_inputs(&plan) {
                return Ok(Transformed::no(plan.arced()));
            }
            flatten_join_inputs(plan, &mut possible_join_keys, &mut all_inputs)?;
            None
        } else {
            // recursively try to rewrite children
            return rewrite_children(self, Arc::new(plan));
        };
        // Join keys are handled locally:
        let mut all_join_keys = JoinKeySet::new();
        let mut left = all_inputs.remove(0);
        while !all_inputs.is_empty() {
            left = find_inner_join(
                left,
                &mut all_inputs,
                &possible_join_keys,
                &mut all_join_keys,
            )?;
        }
        left = rewrite_children(self, left)?.data;
        if schema != left.schema() {
            let project = Project::new_from_schema(left, schema)?;

            left = Arc::new(LogicalPlan::Project(project));
        }
        let Some(predicate) = parent_predicate else {
            return Ok(Transformed::yes(left));
        };

        // If there are no join keys then do nothing:
        if all_join_keys.is_empty() {
            let f = Filter::try_new(left, predicate)?;

            Ok(Transformed::yes(Arc::new(LogicalPlan::Filter(f))))
        } else {
            // Remove join expressions from filter:
            match remove_join_expressions(predicate, &all_join_keys) {
                Some(filter_expr) => {
                    let f = Filter::try_new(left, Arc::new(filter_expr))?;

                    Ok(Transformed::yes(Arc::new(LogicalPlan::Filter(f))))
                }
                _ => Ok(Transformed::yes(left)),
            }
        }
    }
}

fn rewrite_children(
    optimizer: &impl OptimizerRule,
    plan: Arc<LogicalPlan>,
) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
    plan.map_children(|input| optimizer.try_optimize(input))
}

fn flatten_join_inputs(
    plan: LogicalPlan,
    possible_join_keys: &mut JoinKeySet,
    all_inputs: &mut Vec<LogicalPlanRef>,
) -> DaftResult<()> {
    if let LogicalPlan::Join(
        join @ Join {
            join_type: JoinType::Inner,
            join_strategy: None,
            ..
        },
    ) = plan
    {
        let keys = join.left_on.into_iter().zip(join.right_on);
        possible_join_keys.insert_all_owned(keys);
        flatten_join_inputs(
            Arc::unwrap_or_clone(join.left),
            possible_join_keys,
            all_inputs,
        )?;
        flatten_join_inputs(
            Arc::unwrap_or_clone(join.right),
            possible_join_keys,
            all_inputs,
        )?;
    } else {
        all_inputs.push(Arc::new(plan));
    }

    Ok(())
}

/// Returns true if the plan is a Join or Cross join could be flattened with
/// `flatten_join_inputs`
///
/// Must stay in sync with `flatten_join_inputs`
fn can_flatten_join_inputs(plan: &LogicalPlan) -> bool {
    // can only flatten inner / cross joins
    match plan {
        LogicalPlan::Join(join) if join.join_type == JoinType::Inner => {}
        _ => return false,
    };

    for child in plan.children() {
        if matches!(
            child,
            LogicalPlan::Join(Join {
                join_strategy: None,
                join_type: JoinType::Inner,
                ..
            })
        ) && !can_flatten_join_inputs(child)
        {
            return false;
        }
    }
    true
}

/// Extract join keys from a WHERE clause
fn extract_possible_join_keys(expr: &Expr, join_keys: &mut JoinKeySet) {
    if let Expr::BinaryOp { left, op, right } = expr {
        match op {
            Operator::Eq => {
                // insert handles ensuring  we don't add the same Join keys multiple times
                join_keys.insert(left, right);
            }
            Operator::And => {
                extract_possible_join_keys(left, join_keys);
                extract_possible_join_keys(right, join_keys);
            }
            // Fix for join predicates from inside of OR expr also pulled up properly.
            Operator::Or => {
                let mut left_join_keys = JoinKeySet::new();
                let mut right_join_keys = JoinKeySet::new();

                extract_possible_join_keys(left, &mut left_join_keys);
                extract_possible_join_keys(right, &mut right_join_keys);

                join_keys.insert_intersection(&left_join_keys, &right_join_keys);
            }
            _ => (),
        };
    }
}

/// Remove join expressions from a filter expression
///
/// # Returns
/// * `Some()` when there are few remaining predicates in filter_expr
/// * `None` otherwise
fn remove_join_expressions(expr: ExprRef, join_keys: &JoinKeySet) -> Option<Expr> {
    match Arc::unwrap_or_clone(expr) {
        Expr::BinaryOp {
            left,
            op: Operator::Eq,
            right,
        } if join_keys.contains(&left, &right) => {
            // was a join key, so remove it
            None
        }
        // Fix for join predicates from inside of OR expr also pulled up properly.
        Expr::BinaryOp { left, op, right } if op == Operator::And => {
            let l = remove_join_expressions(left, join_keys);
            let r = remove_join_expressions(right, join_keys);
            match (l, r) {
                (Some(ll), Some(rr)) => Some(Expr::BinaryOp {
                    left: Arc::new(ll),
                    op,
                    right: Arc::new(rr),
                }),
                (Some(ll), _) => Some(ll),
                (_, Some(rr)) => Some(rr),
                _ => None,
            }
        }
        Expr::BinaryOp { left, op, right } if op == Operator::Or => {
            let l = remove_join_expressions(left, join_keys);
            let r = remove_join_expressions(right, join_keys);
            match (l, r) {
                (Some(ll), Some(rr)) => Some(Expr::BinaryOp {
                    left: Arc::new(ll),
                    op,
                    right: Arc::new(rr),
                }),
                // When either `left` or `right` is empty, it means they are `true`
                // so OR'ing anything with them will also be true
                _ => None,
            }
        }
        other => Some(other),
    }
}

/// Finds the next to join with the left input plan,
///
/// Finds the next `right` from `rights` that can be joined with `left_input`
/// plan based on the join keys in `possible_join_keys`.
///
/// If such a matching `right` is found:
/// 1. Adds the matching join keys to `all_join_keys`.
/// 2. Returns `left_input JOIN right ON (all join keys)`.
///
/// If no matching `right` is found:
/// 1. Removes the first plan from `rights`
/// 2. Returns `left_input CROSS JOIN right`.
fn find_inner_join(
    left_input: LogicalPlanRef,
    rights: &mut Vec<LogicalPlanRef>,
    possible_join_keys: &JoinKeySet,
    all_join_keys: &mut JoinKeySet,
) -> DaftResult<LogicalPlanRef> {
    for (i, right_input) in rights.iter().enumerate() {
        let mut join_keys = vec![];

        for (l, r) in possible_join_keys.iter() {
            let key_pair = find_valid_equijoin_key_pair(
                l.clone(),
                r.clone(),
                left_input.schema(),
                right_input.schema(),
            )?;

            // Save join keys
            if let Some((valid_l, valid_r)) = key_pair {
                if can_hash(&valid_l.get_type(left_input.schema().as_ref())?) {
                    join_keys.push((valid_l, valid_r));
                }
            }
        }

        // Found one or more matching join keys
        if !join_keys.is_empty() {
            all_join_keys.insert_all(join_keys.iter());
            let right_input = rights.remove(i);
            let join_schema = left_input
                .schema()
                .non_distinct_union(right_input.schema().as_ref());

            let (left_keys, right_keys) = join_keys.iter().cloned().unzip();
            return Ok(LogicalPlan::Join(Join {
                left: left_input,
                right: right_input,
                left_on: left_keys,
                right_on: right_keys,
                null_equals_nulls: None,
                join_type: JoinType::Inner,
                join_strategy: None,
                output_schema: Arc::new(join_schema),
            })
            .arced());
        }
    }

    // no matching right plan had any join keys, cross join with the first right
    // plan
    let right = rights.remove(0);
    let join_schema = left_input
        .schema()
        .non_distinct_union(right.schema().as_ref());

    Ok(LogicalPlan::Join(Join {
        left: left_input,
        right,
        left_on: vec![],
        right_on: vec![],
        null_equals_nulls: None,
        join_type: JoinType::Inner,
        join_strategy: None,
        output_schema: Arc::new(join_schema),
    })
    .arced())
}

/// Check whether all columns are from the schema.
pub fn check_all_columns_from_schema(columns: &[String], schema: &Schema) -> DaftResult<bool> {
    for col in columns {
        let exist = schema.get_index(col).is_ok();

        if !exist {
            return Ok(false);
        }
    }

    Ok(true)
}

/// Give two sides of the equijoin predicate, return a valid join key pair.
/// If there is no valid join key pair, return None.
///
/// A valid join means:
/// 1. All referenced column of the left side is from the left schema, and
///    all referenced column of the right side is from the right schema.
/// 2. Or opposite. All referenced column of the left side is from the right schema,
///    and the right side is from the left schema.
///
pub fn find_valid_equijoin_key_pair(
    left_key: ExprRef,
    right_key: ExprRef,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
) -> DaftResult<Option<(ExprRef, ExprRef)>> {
    let left_using_columns = get_required_columns(&left_key);
    let right_using_columns = get_required_columns(&right_key);

    // Conditions like a = 10, will be added to non-equijoin.
    if left_using_columns.is_empty() || right_using_columns.is_empty() {
        return Ok(None);
    }

    if check_all_columns_from_schema(&left_using_columns, &left_schema)?
        && check_all_columns_from_schema(&right_using_columns, &right_schema)?
    {
        return Ok(Some((left_key, right_key)));
    } else if check_all_columns_from_schema(&right_using_columns, &left_schema)?
        && check_all_columns_from_schema(&left_using_columns, &right_schema)?
    {
        return Ok(Some((right_key, left_key)));
    }

    Ok(None)
}

/// Can this data type be used in hash join equal conditions??
/// Data types here come from function 'equal_rows', if more data types are supported
/// in equal_rows(hash join), add those data types here to generate join logical plan.
pub fn can_hash(data_type: &DataType) -> bool {
    match data_type {
        DataType::Null => true,
        DataType::Boolean => true,
        DataType::Int8 => true,
        DataType::Int16 => true,
        DataType::Int32 => true,
        DataType::Int64 => true,
        DataType::UInt8 => true,
        DataType::UInt16 => true,
        DataType::UInt32 => true,
        DataType::UInt64 => true,
        DataType::Float32 => true,
        DataType::Float64 => true,
        DataType::Timestamp(time_unit, _) => match time_unit {
            TimeUnit::Seconds => true,
            TimeUnit::Milliseconds => true,
            TimeUnit::Microseconds => true,
            TimeUnit::Nanoseconds => true,
        },
        DataType::Utf8 => true,

        DataType::Decimal128(_, _) => true,
        DataType::Date => true,

        DataType::FixedSizeBinary(_) => true,

        DataType::List(_) => true,

        DataType::FixedSizeList(_, _) => true,
        DataType::Struct(fields) => fields.iter().all(|f| can_hash(&f.dtype)),
        _ => false,
    }
}
#[cfg(test)]
mod tests {
    use common_display::mermaid::{MermaidDisplay, MermaidDisplayOptions};
    use daft_dsl::{col, lit};
    use daft_schema::field::Field;
    use rstest::*;

    use super::*;
    use crate::{
        logical_plan::Source, source_info::PlaceHolderInfo, ClusteringSpec, LogicalPlan,
        LogicalPlanBuilder, LogicalPlanRef, SourceInfo,
    };

    #[fixture]
    fn t1() -> LogicalPlanRef {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("a", DataType::UInt32),
                Field::new("b", DataType::UInt32),
                Field::new("c", DataType::UInt32),
            ])
            .unwrap(),
        );
        LogicalPlan::Source(Source {
            output_schema: schema.clone(),
            source_info: Arc::new(SourceInfo::PlaceHolder(PlaceHolderInfo {
                source_schema: schema,
                clustering_spec: Arc::new(ClusteringSpec::unknown()),
                source_id: 0,
            })),
        })
        .arced()
    }

    #[fixture]
    fn t2() -> LogicalPlanRef {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("a", DataType::UInt32),
                Field::new("b", DataType::UInt32),
                Field::new("c", DataType::UInt32),
            ])
            .unwrap(),
        );
        LogicalPlan::Source(Source {
            output_schema: schema.clone(),
            source_info: Arc::new(SourceInfo::PlaceHolder(PlaceHolderInfo {
                source_schema: schema,
                clustering_spec: Arc::new(ClusteringSpec::unknown()),
                source_id: 0,
            })),
        })
        .arced()
    }

    fn assert_optimized_plan_eq(plan: LogicalPlanRef, expected: LogicalPlanRef) {
        let starting_schema = plan.schema();

        let rule = EliminateCrossJoin::new();
        let transformed_plan = rule.try_optimize(plan).unwrap();
        assert!(transformed_plan.transformed, "failed to optimize plan");
        let actual = transformed_plan.data;

        if actual != expected {
            println!(
                "expected:\n{}\nactual:\n{}",
                expected.repr_mermaid(MermaidDisplayOptions::default()),
                actual.repr_mermaid(MermaidDisplayOptions::default())
            );
        }
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );
        assert_eq!(starting_schema, actual.schema())
    }

    #[rstest]
    fn eliminate_cross_with_simple_and(t1: LogicalPlanRef, t2: LogicalPlanRef) -> DaftResult<()> {
        // could eliminate to inner join since filter has Join predicates
        let plan = LogicalPlanBuilder::from(t1.clone())
            .cross_join(t2.clone(), None, None)?
            .filter(col("a").eq(col("right.a")).and(col("b").eq(col("right.b"))))?
            .build();

        let expected = LogicalPlanBuilder::from(t1)
            .join(
                LogicalPlanBuilder::from(t2).select(vec![
                    col("a").alias("right.a"),
                    col("b").alias("right.b"),
                    col("c").alias("right.c"),
                ])?,
                vec![col("a"), col("b")],
                vec![col("right.a"), col("right.b")],
                JoinType::Inner,
                None,
                None,
                None,
                false,
            )?
            .build();

        assert_optimized_plan_eq(plan, expected);

        Ok(())
    }

    #[rstest]
    fn eliminate_cross_with_simple_or(t1: LogicalPlanRef, t2: LogicalPlanRef) -> DaftResult<()> {
        // could not eliminate to inner join since filter OR expression and there is no common
        // Join predicates in left and right of OR expr.
        let plan = LogicalPlanBuilder::from(t1.clone())
            .cross_join(t2.clone(), None, None)?
            .filter(col("a").eq(col("right.a")).or(col("right.b").eq(col("a"))))?
            .build();

        let expected = LogicalPlanBuilder::from(t1)
            .join(
                LogicalPlanBuilder::from(t2).select(vec![
                    col("a").alias("right.a"),
                    col("b").alias("right.b"),
                    col("c").alias("right.c"),
                ])?,
                vec![],
                vec![],
                JoinType::Inner,
                None,
                None,
                None,
                false,
            )?
            .filter(col("a").eq(col("right.a")).or(col("right.b").eq(col("a"))))?
            .build();

        assert_optimized_plan_eq(plan, expected);

        Ok(())
    }

    #[rstest]
    fn eliminate_cross_with_and(t1: LogicalPlanRef, t2: LogicalPlanRef) -> DaftResult<()> {
        let expr1 = col("a").eq(col("right.a"));
        let expr2 = col("right.c").lt(lit(20u32));
        let expr3 = col("a").eq(col("right.a"));
        let expr4 = col("right.c").eq(lit(10u32));
        // could eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1.clone())
            .cross_join(t2.clone(), None, None)?
            .filter(expr1.and(expr2.clone()).and(expr3).and(expr4.clone()))?
            .build();

        let expected = LogicalPlanBuilder::from(t1)
            .join(
                LogicalPlanBuilder::from(t2).select(vec![
                    col("a").alias("right.a"),
                    col("b").alias("right.b"),
                    col("c").alias("right.c"),
                ])?,
                vec![col("a")],
                vec![col("right.a")],
                JoinType::Inner,
                None,
                None,
                None,
                false,
            )?
            .filter(expr2.and(expr4))?
            .build();

        assert_optimized_plan_eq(plan, expected);

        Ok(())
    }

    #[rstest]
    fn eliminate_cross_with_or(t1: LogicalPlanRef, t2: LogicalPlanRef) -> DaftResult<()> {
        // could eliminate to inner join since Or predicates have common Join predicates
        let expr1 = col("a").eq(col("right.a"));
        let expr2 = col("right.c").lt(lit(15u32));
        let expr3 = col("a").eq(col("right.a"));
        let expr4 = col("right.c").eq(lit(688u32));
        let plan = LogicalPlanBuilder::from(t1.clone())
            .cross_join(t2.clone(), None, None)?
            .filter(expr1.and(expr2.clone()).or(expr3.and(expr4.clone())))?
            .build();

        let expected = LogicalPlanBuilder::from(t1)
            .join(
                LogicalPlanBuilder::from(t2).select(vec![
                    col("a").alias("right.a"),
                    col("b").alias("right.b"),
                    col("c").alias("right.c"),
                ])?,
                vec![col("a")],
                vec![col("right.a")],
                JoinType::Inner,
                None,
                None,
                None,
                false,
            )?
            .filter(expr2.or(expr4))?
            .build();

        assert_optimized_plan_eq(plan, expected);

        Ok(())
    }

    #[rstest]
    fn eliminate_cross_join_multi_tables(
        t1: LogicalPlanRef,
        t2: LogicalPlanRef,
        #[from(t1)] t3: LogicalPlanRef,
        #[from(t1)] t4: LogicalPlanRef,
    ) -> DaftResult<()> {
        // could eliminate to inner join
        let plan1 = LogicalPlanBuilder::from(t1.clone())
            .cross_join(t2.clone(), None, Some("t2."))?
            .filter(
                col("a")
                    .eq(col("t2.a"))
                    .and(col("t2.c").lt(lit(15u32)))
                    .or(col("a").eq(col("t2.a")).and(col("t2.c").eq(lit(688u32)))),
            )?
            .build();

        let plan2 = LogicalPlanBuilder::from(t3.clone())
            .cross_join(t4.clone(), None, Some("t4."))?
            .filter(
                (col("a")
                    .eq(col("t4.a"))
                    .and(col("t4.c").lt(lit(15u32)))
                    .or(col("a").eq(col("t4.a")).and(col("c").eq(lit(688u32)))))
                .or(col("a").eq(col("t4.a")).and(col("b").eq(col("t4.b")))),
            )?
            .build();

        let plan = LogicalPlanBuilder::from(plan1.clone())
            .cross_join(plan2.clone(), None, Some("t3."))?
            .filter(
                col("t3.a")
                    .eq(col("a"))
                    .and(col("t4.c").lt(lit(15u32)))
                    .or(col("t3.a").eq(col("a")).and(col("t4.c").eq(lit(688u32)))),
            )?
            .build();
        let plan_1 = LogicalPlanBuilder::from(t1)
            .join(
                LogicalPlanBuilder::from(t2).select(vec![
                    col("a").alias("t2.a"),
                    col("b").alias("t2.b"),
                    col("c").alias("t2.c"),
                ])?,
                vec![col("a")],
                vec![col("t2.a")],
                JoinType::Inner,
                None,
                None,
                None,
                false,
            )?
            .filter(col("t2.c").lt(lit(15u32)).or(col("t2.c").eq(lit(688u32))))?
            .build();

        let plan_2 = LogicalPlanBuilder::from(t3)
            .join(
                LogicalPlanBuilder::from(t4).select(vec![
                    col("a").alias("t4.a"),
                    col("b").alias("t4.b"),
                    col("c").alias("t4.c"),
                ])?,
                vec![col("a")],
                vec![col("t4.a")],
                JoinType::Inner,
                None,
                None,
                None,
                false,
            )?
            .filter(
                col("t4.c")
                    .lt(lit(15u32))
                    .or(col("c").eq(lit(688u32)))
                    .or(col("b").eq(col("t4.b"))),
            )?
            .select(vec![
                col("a").alias("t3.a"),
                col("b").alias("t3.b"),
                col("c").alias("t3.c"),
                col("t4.a"),
                col("t4.b"),
                col("t4.c"),
            ])?
            .build();
        let expected = LogicalPlanBuilder::from(plan_1)
            .join(
                plan_2,
                vec![col("a")],
                vec![col("t3.a")],
                JoinType::Inner,
                None,
                None,
                None,
                false,
            )?
            .filter(col("t4.c").lt(lit(15u32)).or(col("t4.c").eq(lit(688u32))))?
            .build();

        assert_optimized_plan_eq(plan, expected);

        Ok(())
    }
}
