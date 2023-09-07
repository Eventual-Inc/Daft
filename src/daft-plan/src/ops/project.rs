use std::sync::Arc;

use daft_core::datatypes::FieldID;
use daft_core::schema::{Schema, SchemaRef};
use daft_dsl::{optimization, AggExpr, Expr, ExprRef};
use indexmap::{IndexMap, IndexSet};
use snafu::ResultExt;

use crate::logical_plan::{CreationSnafu, Result};
use crate::optimization::Transformed;
use crate::{LogicalPlan, ResourceRequest};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Project {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub projection: Vec<Expr>,
    pub resource_request: ResourceRequest,
    pub projected_schema: SchemaRef,
}

impl Project {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        projection: Vec<Expr>,
        resource_request: ResourceRequest,
    ) -> Result<Self> {
        // Factor the projection and see if there are any substitutions to factor out.
        let (factored_input, factored_projection) =
            Self::try_factor_subexpressions(input, projection, &resource_request)?;

        let upstream_schema = factored_input.schema();
        let projected_schema = {
            let fields = factored_projection
                .iter()
                .map(|e| e.to_field(&upstream_schema))
                .collect::<common_error::DaftResult<Vec<_>>>()
                .context(CreationSnafu)?;
            Schema::new(fields).context(CreationSnafu)?.into()
        };
        Ok(Self {
            input: factored_input,
            projection: factored_projection,
            resource_request,
            projected_schema,
        })
    }

    fn try_factor_subexpressions(
        input: Arc<LogicalPlan>,
        projection: Vec<Expr>,
        resource_request: &ResourceRequest,
    ) -> Result<(Arc<LogicalPlan>, Vec<Expr>)> {
        // Given construction parameters for a projection,
        // see if we can factor out common subexpressions.
        // Returns a new set of projection parameters
        // (a maybe new input node, and a maybe new list of projection expressions).
        let upstream_schema = input.schema();
        let (projection, substitutions) = Self::factor_expressions(&projection, &upstream_schema);

        // If there are substitutions to factor out,
        // create a child projection node to do the factoring.
        let input = if substitutions.is_empty() {
            input
        } else {
            let child_projection = projection
                .iter()
                .flat_map(optimization::get_required_columns)
                .map(|colname| {
                    substitutions.get(&colname).map_or_else(
                        || (colname.clone(), Expr::Column(colname.clone().into())),
                        |expr| {
                            (
                                colname.clone(),
                                Expr::Alias(expr.clone().into(), colname.clone().into()),
                            )
                        },
                    )
                })
                .collect::<IndexMap<_, _>>()
                .into_values()
                .collect::<Vec<_>>();
            let plan: LogicalPlan =
                Self::try_new(input, child_projection, resource_request.clone())?.into();
            plan.into()
        };
        Ok((input, projection))
    }

    fn factor_expressions(exprs: &[Expr], schema: &Schema) -> (Vec<Expr>, IndexMap<String, Expr>) {
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

        let mut exprs_to_walk: Vec<Arc<Expr>> = exprs.iter().map(|e| e.clone().into()).collect();
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
                        // Mark expr as seen
                        let newly_seen = seen_subexpressions.insert(expr_id.clone());
                        if newly_seen {
                            // If not previously seen, continue recursing down children
                            expr.children()
                        } else {
                            // If previously seen, cache the expression (if it involves computation)
                            if optimization::requires_computation(expr) {
                                subexpressions_to_cache.insert(expr_id.clone(), expr.clone());
                            }
                            // Stop recursing if previously seen;
                            // we only want top-level repeated subexpressions
                            vec![]
                        }
                    }
                })
                .collect();
        }

        // Then, substitute all the cached subexpressions in the original expressions.
        let subexprs_to_replace = subexpressions_to_cache
            .keys()
            .cloned()
            .collect::<IndexSet<_>>();
        let substituted_expressions = exprs
            .iter()
            .map(|e| {
                replace_column_with_semantic_id(e, &subexprs_to_replace, schema)
                    .unwrap()
                    .as_ref()
                    .clone()
            })
            .collect::<Vec<_>>();

        let substitutions = subexpressions_to_cache
            .iter()
            .map(|(k, v)| (k.id.as_ref().to_string(), v.as_ref().clone()))
            .collect::<IndexMap<_, _>>();

        (substituted_expressions, substitutions)
    }
}

fn replace_column_with_semantic_id(
    e: &Expr,
    subexprs_to_replace: &IndexSet<FieldID>,
    schema: &Schema,
) -> Transformed<ExprRef> {
    // Constructs a new copy of this expression
    // with all occurences of subexprs_to_replace replaced with a column selection.
    // e.g. e := (a+b)+c, subexprs := {FieldID("(a + b)")}
    //  -> Col("(a + b)") + c

    let sem_id = e.semantic_id(schema);
    if subexprs_to_replace.contains(&sem_id) {
        Transformed::Yes(Expr::Column(sem_id.id.clone()).into())
    } else {
        match e {
            Expr::Column(_) | Expr::Literal(_) => Transformed::No(e.clone().into()),
            Expr::Agg(agg_expr) => {
                replace_column_with_semantic_id_aggexpr(agg_expr, subexprs_to_replace, schema).map(
                    |transformed_child| Expr::Agg(transformed_child).into(),
                    |_| e.clone().into(),
                )
            }
            Expr::Alias(child, name) => {
                replace_column_with_semantic_id(child, subexprs_to_replace, schema).map(
                    |transformed_child| Expr::Alias(transformed_child, name.clone()).into(),
                    |_| e.clone().into(),
                )
            }
            Expr::Cast(child, datatype) => {
                replace_column_with_semantic_id(child, subexprs_to_replace, schema).map(
                    |transformed_child| Expr::Cast(transformed_child, datatype.clone()).into(),
                    |_| e.clone().into(),
                )
            }
            Expr::Not(child) => replace_column_with_semantic_id(child, subexprs_to_replace, schema)
                .map(
                    |transformed_child| Expr::Not(transformed_child).into(),
                    |_| e.clone().into(),
                ),
            Expr::IsNull(child) => {
                replace_column_with_semantic_id(child, subexprs_to_replace, schema).map(
                    |transformed_child| Expr::IsNull(transformed_child).into(),
                    |_| e.clone().into(),
                )
            }
            Expr::BinaryOp { op, left, right } => {
                let left = replace_column_with_semantic_id(left, subexprs_to_replace, schema);
                let right = replace_column_with_semantic_id(right, subexprs_to_replace, schema);
                if left.is_no() && right.is_no() {
                    Transformed::No(e.clone().into())
                } else {
                    Transformed::Yes(
                        Expr::BinaryOp {
                            op: *op,
                            left: left.unwrap().clone(),
                            right: right.unwrap().clone(),
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
                    replace_column_with_semantic_id(predicate, subexprs_to_replace, schema);
                let if_true = replace_column_with_semantic_id(if_true, subexprs_to_replace, schema);
                let if_false =
                    replace_column_with_semantic_id(if_false, subexprs_to_replace, schema);
                if predicate.is_no() && if_true.is_no() && if_false.is_no() {
                    Transformed::No(e.clone().into())
                } else {
                    Transformed::Yes(
                        Expr::IfElse {
                            predicate: predicate.unwrap().clone(),
                            if_true: if_true.unwrap().clone(),
                            if_false: if_false.unwrap().clone(),
                        }
                        .into(),
                    )
                }
            }
            Expr::Function { func, inputs } => {
                let transforms = inputs
                    .iter()
                    .map(|e| replace_column_with_semantic_id(e, subexprs_to_replace, schema))
                    .collect::<Vec<_>>();
                if transforms.iter().all(|e| e.is_no()) {
                    Transformed::No(e.clone().into())
                } else {
                    Transformed::Yes(
                        Expr::Function {
                            func: func.clone(),
                            inputs: transforms
                                .iter()
                                .map(|t| t.unwrap().as_ref())
                                .cloned()
                                .collect(),
                        }
                        .into(),
                    )
                }
            }
        }
    }
}

fn replace_column_with_semantic_id_aggexpr(
    e: &AggExpr,
    subexprs_to_replace: &IndexSet<FieldID>,
    schema: &Schema,
) -> Transformed<AggExpr> {
    // Constructs a new copy of this expression
    // with all occurences of subexprs_to_replace replaced with a column selection.
    // e.g. e := (a+b)+c, subexprs := {FieldID("(a + b)")}
    //  -> Col("(a + b)") + c

    match e {
        AggExpr::Count(child, mode) => {
            replace_column_with_semantic_id(child, subexprs_to_replace, schema).map(
                |transformed_child| AggExpr::Count(transformed_child, *mode),
                |_| e.clone(),
            )
        }
        AggExpr::Sum(child) => replace_column_with_semantic_id(child, subexprs_to_replace, schema)
            .map(AggExpr::Sum, |_| e.clone()),
        AggExpr::Mean(child) => replace_column_with_semantic_id(child, subexprs_to_replace, schema)
            .map(AggExpr::Mean, |_| e.clone()),
        AggExpr::Min(child) => replace_column_with_semantic_id(child, subexprs_to_replace, schema)
            .map(AggExpr::Min, |_| e.clone()),
        AggExpr::Max(child) => replace_column_with_semantic_id(child, subexprs_to_replace, schema)
            .map(AggExpr::Max, |_| e.clone()),
        AggExpr::List(child) => replace_column_with_semantic_id(child, subexprs_to_replace, schema)
            .map(AggExpr::List, |_| e.clone()),
        AggExpr::Concat(child) => {
            replace_column_with_semantic_id(child, subexprs_to_replace, schema)
                .map(AggExpr::Concat, |_| e.clone())
        }
    }
}
