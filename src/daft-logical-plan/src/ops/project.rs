use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode, TreeNodeRecursion};
use daft_core::prelude::*;
use daft_dsl::{
    functions::FunctionArgs, optimization, resolved_col, AggExpr, ApproxPercentileParams, Column,
    Expr, ExprRef,
};
use indexmap::{IndexMap, IndexSet};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    logical_plan::{self},
    stats::StatsState,
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Project {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub projection: Vec<ExprRef>,
    pub projected_schema: SchemaRef,
    pub stats_state: StatsState,
}

impl Project {
    pub fn new(input: Arc<LogicalPlan>, projection: Vec<ExprRef>) -> DaftResult<Self> {
        Ok(Self::try_new(input, projection)?)
    }

    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        projection: Vec<ExprRef>,
    ) -> logical_plan::Result<Self> {
        // Factor the projection and see if there are any substitutions to factor out.
        let (factored_input, factored_projection) =
            Self::try_factor_subexpressions(input, projection)?;

        let fields = factored_projection
            .iter()
            .map(|expr| expr.to_field(&factored_input.schema()))
            .collect::<DaftResult<Vec<_>>>()?;

        let projected_schema = Schema::new(fields).into();

        Ok(Self {
            plan_id: None,
            node_id: None,
            input: factored_input,
            projection: factored_projection,
            projected_schema,
            stats_state: StatsState::NotMaterialized,
        })
    }

    pub fn with_plan_id(mut self, plan_id: usize) -> Self {
        self.plan_id = Some(plan_id);
        self
    }

    pub fn with_node_id(mut self, node_id: usize) -> Self {
        self.node_id = Some(node_id);
        self
    }

    /// Create a new Projection using the specified output schema
    pub(crate) fn new_from_schema(
        input: Arc<LogicalPlan>,
        schema: SchemaRef,
    ) -> logical_plan::Result<Self> {
        let expr: Vec<ExprRef> = schema.names().into_iter().map(resolved_col).collect();
        Self::try_new(input, expr)
    }

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        // TODO(desmond): We can do better estimations with the projection schema. For now, reuse the old logic.
        let input_stats = self.input.materialized_stats();
        self.stats_state = StatsState::Materialized(input_stats.clone().into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![format!(
            "Project: {}",
            self.projection.iter().map(|e| e.to_string()).join(", ")
        )];
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }

    fn try_factor_subexpressions(
        input: Arc<LogicalPlan>,
        projection: Vec<ExprRef>,
    ) -> logical_plan::Result<(Arc<LogicalPlan>, Vec<ExprRef>)> {
        // Check if projection contains any window functions, if so, return without factoring.
        // This is because window functions may implicitly reference columns via the window spec.
        let has_window = projection.iter().any(|expr| {
            let mut has_window = false;
            expr.apply(|e| {
                if matches!(e.as_ref(), Expr::Over(..)) {
                    has_window = true;
                    Ok(TreeNodeRecursion::Stop)
                } else {
                    Ok(TreeNodeRecursion::Continue)
                }
            })
            .unwrap();

            has_window
        });

        if has_window {
            return Ok((input, projection));
        }

        // Given construction parameters for a projection,
        // see if we can factor out common subexpressions.
        // Returns a new set of projection parameters
        // (a maybe new input node, and a maybe new list of projection expressions).
        let upstream_schema = input.schema();
        let (projection, substitutions) = Self::factor_expressions(projection, &upstream_schema);
        // If there are substitutions to factor out,
        // create a child projection node to do the factoring.
        let input = if substitutions.is_empty() {
            input
        } else {
            let child_projection = projection
                .iter()
                .flat_map(optimization::get_required_columns)
                .collect::<IndexSet<_>>()
                .into_iter()
                .map(|colname| {
                    let expr = &substitutions[&colname];
                    expr.clone().alias(colname)
                })
                .collect::<Vec<_>>();

            let plan: LogicalPlan = Self::try_new(input, child_projection)?.into();
            plan.into()
        };
        Ok((input, projection))
    }

    fn factor_expressions(
        exprs: Vec<ExprRef>,
        schema: &Schema,
    ) -> (Vec<ExprRef>, IndexMap<String, ExprRef>) {
        // Returns
        //  1. original expressions with substitutions
        //  2. substitution definitions
        //
        // E.g. factor_expressions([a+b as e, (a+b)+c as f]) ->
        //  1. [some_id as e, some_id + c as f]
        //  2. {some_id: a+b}

        // Find all top-level repeated subexpressions;
        // these are the expressions we will cache.
        let mut seen_subexpressions = IndexSet::new();
        let mut subexpressions_to_cache = IndexMap::new();

        // While walking, also find all column name references.
        // If we do substitutions and create new semantic ID names,
        // all existing names must also be converted to semantic IDs.
        let mut column_name_substitutions = IndexMap::new();

        let mut exprs_to_walk: Vec<Arc<Expr>> = exprs.clone();
        while !exprs_to_walk.is_empty() {
            exprs_to_walk = exprs_to_walk
                .iter()
                .flat_map(|expr| {
                    // If expr is an alias, ignore and continue recursing
                    // (otherwise the unaliased child will be double counted)
                    if matches!(expr.as_ref(), Expr::Alias(..)) {
                        expr.children()
                    } else {
                        let expr_id = expr.semantic_id(schema);
                        if let Expr::Column(Column::Resolved(..)) = expr.as_ref() {
                            column_name_substitutions.insert(expr_id.clone(), expr.clone());
                        }
                        // Mark expr as seen
                        let newly_seen = seen_subexpressions.insert(expr_id.clone());
                        if newly_seen {
                            // If not previously seen, continue recursing down children
                            expr.children()
                        } else {
                            // If previously seen, cache the expression (if it involves computation)
                            if optimization::requires_computation(expr) {
                                subexpressions_to_cache.insert(expr_id, expr.clone());
                            }
                            // Stop recursing if previously seen;
                            // we only want top-level repeated subexpressions
                            vec![]
                        }
                    }
                })
                .collect();
        }

        if subexpressions_to_cache.is_empty() {
            (exprs, IndexMap::new())
        } else {
            // Then, substitute all the cached subexpressions in the original expressions.
            let subexprs_to_replace = subexpressions_to_cache
                .keys()
                .chain(column_name_substitutions.keys())
                .cloned()
                .collect::<IndexSet<_>>();
            let substituted_expressions = exprs
                .iter()
                .map(|e| {
                    let new_expr =
                        replace_column_with_semantic_id(e.clone(), &subexprs_to_replace, schema);
                    let new_expr = new_expr.data;
                    // The substitution can unintentionally change the expression's name
                    // (since the name depends on the first column referenced, which can be substituted away)
                    // so re-alias the original name here if it has changed.
                    let old_name = e.name();
                    if new_expr.name() != old_name {
                        new_expr.alias(old_name)
                    } else {
                        new_expr
                    }
                })
                .collect::<Vec<_>>();

            let substitutions = subexpressions_to_cache
                .iter()
                .chain(column_name_substitutions.iter())
                .map(|(k, v)| (k.id.as_ref().to_string(), v.clone()))
                .collect::<IndexMap<_, _>>();

            (substituted_expressions, substitutions)
        }
    }
}

/// Constructs a new copy of this expression
/// with all occurrences of subexprs_to_replace replaced with a column selection.
/// e.g. e := (a+b)+c, subexprs := {FieldID("(a + b)")}
///  -> Col("(a + b)") + c
fn replace_column_with_semantic_id(
    e: ExprRef,
    subexprs_to_replace: &IndexSet<FieldID>,
    schema: &Schema,
) -> Transformed<ExprRef> {
    let sem_id = e.semantic_id(schema);
    if subexprs_to_replace.contains(&sem_id) {
        let new_expr = resolved_col(sem_id.id);
        let new_expr = match e.as_ref() {
            Expr::Alias(_, name) => Expr::Alias(new_expr, name.clone()).into(),
            _ => new_expr,
        };
        Transformed::yes(new_expr)
    } else {
        match e.as_ref() {
            Expr::Column(_) | Expr::Literal(_) | Expr::Subquery(_) | Expr::Exists(_) => {
                Transformed::no(e)
            }
            Expr::Agg(agg_expr) => replace_column_with_semantic_id_aggexpr(
                agg_expr.clone(),
                subexprs_to_replace,
                schema,
            )
            .map_yes_no(
                |transformed_child| Expr::Agg(transformed_child).into(),
                |_| e,
            ),
            Expr::Over(inner_expr, window_spec) => {
                let expr_ref: ExprRef = ExprRef::from(inner_expr);

                replace_column_with_semantic_id(expr_ref, subexprs_to_replace, schema).map_yes_no(
                    |transformed_child| {
                        Expr::Over(transformed_child.try_into().unwrap(), window_spec.clone())
                            .into()
                    },
                    |_| e.clone(),
                )
            }
            Expr::WindowFunction(_) => Transformed::no(e),
            Expr::Alias(child, name) => {
                replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                    .map_yes_no(
                        |transformed_child| Expr::Alias(transformed_child, name.clone()).into(),
                        |_| e.clone(),
                    )
            }

            Expr::Cast(child, datatype) => {
                replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                    .map_yes_no(
                        |transformed_child| Expr::Cast(transformed_child, datatype.clone()).into(),
                        |_| e.clone(),
                    )
            }
            Expr::Not(child) => {
                replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                    .map_yes_no(
                        |transformed_child| Expr::Not(transformed_child).into(),
                        |_| e,
                    )
            }
            Expr::IsNull(child) => {
                replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                    .map_yes_no(
                        |transformed_child| Expr::IsNull(transformed_child).into(),
                        |_| e,
                    )
            }
            Expr::NotNull(child) => {
                replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                    .map_yes_no(
                        |transformed_child| Expr::NotNull(transformed_child).into(),
                        |_| e,
                    )
            }
            Expr::FillNull(child, fill_value) => {
                let child =
                    replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema);
                let fill_value = replace_column_with_semantic_id(
                    fill_value.clone(),
                    subexprs_to_replace,
                    schema,
                );
                if !child.transformed && !fill_value.transformed {
                    Transformed::no(e)
                } else {
                    Transformed::yes(Expr::FillNull(child.data, fill_value.data).into())
                }
            }
            Expr::IsIn(child, items) => {
                let child =
                    replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema);

                let transforms = items
                    .iter()
                    .map(|e| {
                        replace_column_with_semantic_id(e.clone(), subexprs_to_replace, schema)
                    })
                    .collect::<Vec<_>>();
                if !child.transformed && transforms.iter().all(|e| !e.transformed) {
                    Transformed::no(e)
                } else {
                    Transformed::yes(
                        Expr::IsIn(
                            child.data,
                            transforms.iter().map(|t| t.data.clone()).collect(),
                        )
                        .into(),
                    )
                }
            }
            Expr::List(items) => {
                let mut transformed = false;
                let mut new_items = Vec::<ExprRef>::new();
                for item in items {
                    let new_item =
                        replace_column_with_semantic_id(item.clone(), subexprs_to_replace, schema);
                    if new_item.transformed {
                        new_items.push(new_item.data.clone());
                        transformed = true;
                    }
                }
                if transformed {
                    return Transformed::yes(Expr::List(new_items).into());
                }
                Transformed::no(e)
            }
            Expr::Between(child, lower, upper) => {
                let child =
                    replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema);
                let lower =
                    replace_column_with_semantic_id(lower.clone(), subexprs_to_replace, schema);
                let upper =
                    replace_column_with_semantic_id(upper.clone(), subexprs_to_replace, schema);
                if !child.transformed && !lower.transformed && !upper.transformed {
                    Transformed::no(e)
                } else {
                    Transformed::yes(Expr::Between(child.data, lower.data, upper.data).into())
                }
            }
            Expr::BinaryOp { op, left, right } => {
                let left =
                    replace_column_with_semantic_id(left.clone(), subexprs_to_replace, schema);
                let right =
                    replace_column_with_semantic_id(right.clone(), subexprs_to_replace, schema);
                if !left.transformed && !right.transformed {
                    Transformed::no(e)
                } else {
                    Transformed::yes(
                        Expr::BinaryOp {
                            op: *op,
                            left: left.data,
                            right: right.data,
                        }
                        .into(),
                    )
                }
            }
            Expr::IfElse {
                if_true,
                if_false,
                predicate,
            } => {
                let predicate =
                    replace_column_with_semantic_id(predicate.clone(), subexprs_to_replace, schema);
                let if_true =
                    replace_column_with_semantic_id(if_true.clone(), subexprs_to_replace, schema);
                let if_false =
                    replace_column_with_semantic_id(if_false.clone(), subexprs_to_replace, schema);
                if !predicate.transformed && !if_true.transformed && !if_false.transformed {
                    Transformed::no(e)
                } else {
                    Transformed::yes(
                        Expr::IfElse {
                            predicate: predicate.data,
                            if_true: if_true.data,
                            if_false: if_false.data,
                        }
                        .into(),
                    )
                }
            }
            Expr::Function { func, inputs } => {
                let transforms = inputs
                    .iter()
                    .map(|e| {
                        replace_column_with_semantic_id(e.clone(), subexprs_to_replace, schema)
                    })
                    .collect::<Vec<_>>();
                if transforms.iter().all(|e| !e.transformed) {
                    Transformed::no(e)
                } else {
                    Transformed::yes(
                        Expr::Function {
                            func: func.clone(),
                            inputs: transforms.iter().map(|t| t.data.clone()).collect(),
                        }
                        .into(),
                    )
                }
            }
            Expr::ScalarFunction(func) => {
                let mut func = func.clone();
                let transforms = func
                    .inputs
                    .iter()
                    .map(|e| {
                        e.map(|e| {
                            replace_column_with_semantic_id(e.clone(), subexprs_to_replace, schema)
                        })
                    })
                    .collect::<Vec<_>>();
                if transforms.iter().all(|e| !e.inner().transformed) {
                    Transformed::no(e)
                } else {
                    let inputs = transforms
                        .iter()
                        .map(|t| t.map(|t| t.data.clone()))
                        .collect::<Vec<_>>();
                    func.inputs = FunctionArgs::new_unchecked(inputs);
                    Transformed::yes(Expr::ScalarFunction(func).into())
                }
            }
            Expr::InSubquery(expr, subquery) => {
                let expr =
                    replace_column_with_semantic_id(expr.clone(), subexprs_to_replace, schema);
                if !expr.transformed {
                    Transformed::no(e)
                } else {
                    Transformed::yes(Expr::InSubquery(expr.data, subquery.clone()).into())
                }
            }
        }
    }
}

fn replace_column_with_semantic_id_aggexpr(
    e: AggExpr,
    subexprs_to_replace: &IndexSet<FieldID>,
    schema: &Schema,
) -> Transformed<AggExpr> {
    // Constructs a new copy of this expression
    // with all occurrences of subexprs_to_replace replaced with a column selection.
    // e.g. e := (a+b)+c, subexprs := {FieldID("(a + b)")}
    //  -> Col("(a + b)") + c

    match e {
        AggExpr::Count(ref child, mode) => {
            replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema).map_yes_no(
                |transformed_child| AggExpr::Count(transformed_child, mode),
                |_| e,
            )
        }
        AggExpr::CountDistinct(ref child) => {
            replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                .map_yes_no(AggExpr::CountDistinct, |_| e)
        }
        AggExpr::Sum(ref child) => {
            replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                .map_yes_no(AggExpr::Sum, |_| e)
        }
        AggExpr::ApproxPercentile(ApproxPercentileParams {
            ref child,
            ref percentiles,
            force_list_output,
        }) => replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
            .map_yes_no(
                |transformed_child| {
                    AggExpr::ApproxPercentile(ApproxPercentileParams {
                        child: transformed_child,
                        percentiles: percentiles.clone(),
                        force_list_output,
                    })
                },
                |_| e.clone(),
            ),
        AggExpr::ApproxCountDistinct(ref child) => {
            replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                .map_yes_no(AggExpr::ApproxCountDistinct, |_| e.clone())
        }
        AggExpr::ApproxSketch(ref child, sketch_type) => {
            replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema).map_yes_no(
                |transformed_child| AggExpr::ApproxSketch(transformed_child, sketch_type),
                |_| e,
            )
        }
        AggExpr::MergeSketch(ref child, sketch_type) => {
            replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema).map_yes_no(
                |transformed_child| AggExpr::MergeSketch(transformed_child, sketch_type),
                |_| e,
            )
        }
        AggExpr::Mean(ref child) => {
            replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                .map_yes_no(AggExpr::Mean, |_| e)
        }
        AggExpr::Stddev(ref child) => {
            replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                .map_yes_no(AggExpr::Stddev, |_| e)
        }
        AggExpr::Min(ref child) => {
            replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                .map_yes_no(AggExpr::Min, |_| e)
        }
        AggExpr::Max(ref child) => {
            replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                .map_yes_no(AggExpr::Max, |_| e)
        }
        AggExpr::BoolAnd(ref child) => {
            replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                .map_yes_no(AggExpr::BoolAnd, |_| e)
        }
        AggExpr::BoolOr(ref child) => {
            replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                .map_yes_no(AggExpr::BoolOr, |_| e)
        }
        AggExpr::AnyValue(ref child, ignore_nulls) => {
            replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema).map_yes_no(
                |transformed_child| AggExpr::AnyValue(transformed_child, ignore_nulls),
                |_| e,
            )
        }
        AggExpr::List(ref child) => {
            replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                .map_yes_no(AggExpr::List, |_| e)
        }
        AggExpr::Set(ref child) => {
            replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                .map_yes_no(AggExpr::Set, |_| e)
        }
        AggExpr::Concat(ref child) => {
            replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                .map_yes_no(AggExpr::Concat, |_| e)
        }
        AggExpr::Skew(ref child) => {
            replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                .map_yes_no(AggExpr::Skew, |_| e)
        }
        AggExpr::MapGroups { func, inputs } => {
            let transforms = inputs
                .iter()
                .map(|e| replace_column_with_semantic_id(e.clone(), subexprs_to_replace, schema))
                .collect::<Vec<_>>();
            if transforms.iter().all(|e| !e.transformed) {
                Transformed::no(AggExpr::MapGroups { func, inputs })
            } else {
                Transformed::yes(AggExpr::MapGroups {
                    func,
                    inputs: transforms.iter().map(|t| t.data.clone()).collect(),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;
    use daft_core::prelude::*;
    use daft_dsl::{binary_op, lit, resolved_col, Operator};

    use crate::{
        ops::Project,
        test::{dummy_scan_node, dummy_scan_operator},
        LogicalPlan,
    };

    /// Test that nested common subexpressions are correctly split
    /// into multiple levels of projections.
    /// e.g.
    /// ((a+a)+(a+a))+((a+a)+(a+a)) as x
    /// ->
    /// 1. aaaa+aaaa as x
    /// 2. aa+aa as aaaa
    /// 3: a+a as aa
    #[test]
    fn test_nested_subexpression() -> DaftResult<()> {
        let source = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]))
        .build();
        let a2 = binary_op(Operator::Plus, resolved_col("a"), resolved_col("a"));
        let a2_colname = a2.semantic_id(&source.schema()).id;

        let a4 = binary_op(Operator::Plus, a2.clone(), a2.clone());
        let a4_colname = a4.semantic_id(&source.schema()).id;

        let a8 = binary_op(Operator::Plus, a4.clone(), a4);
        let expressions = vec![a8.alias("x")];
        let result_projection = Project::try_new(source, expressions)?;

        let a4_col = resolved_col(a4_colname.clone());
        let expected_result_projection =
            vec![binary_op(Operator::Plus, a4_col.clone(), a4_col).alias("x")];
        assert_eq!(result_projection.projection, expected_result_projection);

        let a2_col = resolved_col(a2_colname.clone());
        let expected_subprojection =
            vec![binary_op(Operator::Plus, a2_col.clone(), a2_col).alias(a4_colname)];
        let LogicalPlan::Project(subprojection) = result_projection.input.as_ref() else {
            panic!()
        };
        assert_eq!(subprojection.projection, expected_subprojection);

        let expected_third_projection = vec![a2.alias(a2_colname)];
        let LogicalPlan::Project(third_projection) = subprojection.input.as_ref() else {
            panic!()
        };
        assert_eq!(third_projection.projection, expected_third_projection);

        Ok(())
    }

    /// Test that common subexpressions are correctly identified
    /// across separate expressions.
    /// e.g.
    /// (a+a) as x, (a+a)+a as y
    /// ->
    /// 1. aa as x, aa+a as y
    /// 2. a+a as aa, a
    #[test]
    fn test_shared_subexpression() -> DaftResult<()> {
        let source = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]))
        .build();
        let a2 = binary_op(Operator::Plus, resolved_col("a"), resolved_col("a"));
        let a2_colname = a2.semantic_id(&source.schema()).id;

        let expressions = vec![
            a2.alias("x"),
            binary_op(Operator::Plus, a2.clone(), resolved_col("a")).alias("y"),
        ];
        let result_projection = Project::try_new(source, expressions)?;

        let a2_col = resolved_col(a2_colname.clone());
        let expected_result_projection = vec![
            a2_col.alias("x"),
            binary_op(Operator::Plus, a2_col, resolved_col("a")).alias("y"),
        ];
        assert_eq!(result_projection.projection, expected_result_projection);

        let expected_subprojection = vec![a2.alias(a2_colname), resolved_col("a").alias("a")];
        let LogicalPlan::Project(subprojection) = result_projection.input.as_ref() else {
            panic!()
        };
        assert_eq!(subprojection.projection, expected_subprojection);

        Ok(())
    }

    /// Test that common leaf expressions are not factored out
    /// (since this would not save computation and only introduces another materialization)
    /// e.g.
    /// 3 as x, 3 as y, a as w, a as z
    /// ->
    /// (unchanged)
    #[test]
    fn test_vacuous_subexpression() -> DaftResult<()> {
        let source = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]))
        .build();
        let expressions = vec![
            lit(3).alias("x"),
            lit(3).alias("y"),
            resolved_col("a").alias("w"),
            resolved_col("a").alias("z"),
        ];
        let result_projection = Project::try_new(source, expressions.clone())?;

        assert_eq!(result_projection.projection, expressions);

        Ok(())
    }
}
