use std::{collections::HashSet, sync::Arc};

use common_error::{DaftError, DaftResult};
use common_treenode::{Transformed, TreeNode, TreeNodeRecursion};
use daft_core::prelude::*;
use daft_dsl::{
    functions::{struct_::StructExpr, FunctionExpr},
    has_agg, is_actor_pool_udf, resolved_col, AggExpr, Column, Expr, ExprRef, PlanRef,
    ResolvedColumn, UnresolvedColumn,
};
use typed_builder::TypedBuilder;

use crate::LogicalPlanRef;

fn contains_monotonic_id(expr: &ExprRef) -> bool {
    expr.exists(|e| match e.as_ref() {
        Expr::ScalarFunction(func) => func.name() == "monotonically_increasing_id",
        _ => false,
    })
}

/// Duplicate an expression tree for each wildcard match in a column or struct get.
fn expand_wildcard(expr: ExprRef, plan: LogicalPlanRef) -> DaftResult<Vec<ExprRef>> {
    let mut wildcard_expansion = None;

    fn set_wildcard_expansion(
        wildcard_expansion: &mut Option<Vec<String>>,
        expr: &Expr,
        names: impl Iterator<Item = String>,
    ) -> DaftResult<()> {
        if wildcard_expansion.is_some() {
            Err(DaftError::ValueError(format!(
                "Error resolving expression {expr}: cannot have multiple wildcard columns in one expression tree.")))
        } else {
            *wildcard_expansion = Some(names.collect());

            Ok(())
        }
    }

    expr.apply(|e| {
        match e.as_ref() {
            Expr::Column(Column::Unresolved(UnresolvedColumn {
                name,
                plan_ref,
                plan_schema
            })) if *name == "*".into() => {
                match plan_ref {
                    PlanRef::Alias(alias) => {
                        match (&plan.clone().get_schema_for_alias(alias)?, plan_schema) {
                            (None, None) => {
                                return Err(DaftError::ValueError(format!("Plan alias {alias} in unresolved column is not in current scope, must have schema specified.")));
                            }
                            (Some(schema), _) | (None, Some(schema)) => {
                                set_wildcard_expansion(&mut wildcard_expansion, &expr, schema.fields.keys().cloned())?;
                            },
                        }
                    }
                    PlanRef::Id(id) => {
                        match (&plan.clone().get_schema_for_id(*id)?, plan_schema) {
                            (None, None) => {
                                return Err(DaftError::ValueError(format!("Plan id {id} in unresolved column is not in current scope, must have schema specified.")));
                            }
                            (Some(schema), _) | (None, Some(schema)) => {
                                set_wildcard_expansion(&mut wildcard_expansion, &expr, schema.fields.keys().cloned())?;
                            },
                        }

                    }
                    PlanRef::Unqualified => set_wildcard_expansion(&mut wildcard_expansion, &expr, plan.schema().fields.keys().cloned())?,
                }
            }
            Expr::Function { func: FunctionExpr::Struct(StructExpr::Get(name)), inputs } if name == "*" => {
                let [input] = inputs.as_slice() else {
                    return Err(DaftError::SchemaMismatch(format!(
                        "Expected 1 input arg, got {}",
                        inputs.len()
                    )));
                };

                let DataType::Struct(struct_fields) = input.to_field(&plan.schema())?.dtype else {
                    return Err(DaftError::SchemaMismatch(format!(
                        "Wildcard struct get on a non-struct type: {input}"
                    )));
                };

                set_wildcard_expansion(&mut wildcard_expansion, &expr, struct_fields.iter().map(|f| f.name.clone()))?;
            }
            _ => {}
        }

        Ok(TreeNodeRecursion::Continue)
    })?;

    if let Some(expansion) = wildcard_expansion {
        expansion
            .into_iter()
            .map(|new_name| {
                Ok(expr
                    .clone()
                    .transform(|e| match e.as_ref() {
                        Expr::Column(Column::Unresolved(UnresolvedColumn {
                            name,
                            plan_ref,
                            plan_schema,
                        })) if *name == "*".into() => Ok(Transformed::yes(Arc::new(Expr::Column(
                            Column::Unresolved(UnresolvedColumn {
                                name: new_name.clone().into(),
                                plan_ref: plan_ref.clone(),
                                plan_schema: plan_schema.clone(),
                            }),
                        )))),
                        Expr::Function {
                            func: FunctionExpr::Struct(StructExpr::Get(name)),
                            inputs,
                        } if name == "*" => Ok(Transformed::yes(
                            daft_dsl::functions::struct_::get(inputs[0].clone(), &new_name),
                        )),
                        _ => Ok(Transformed::no(e)),
                    })?
                    .data)
            })
            .collect()
    } else {
        Ok(vec![expr])
    }
}

fn resolve_unresolved_columns(expr: ExprRef, plan: LogicalPlanRef) -> DaftResult<ExprRef> {
    Ok(expr.transform(|e| {
        if let Expr::Column(Column::Unresolved(UnresolvedColumn {
            name,
            plan_ref,
            plan_schema
        })) = e.as_ref() {
            match plan_ref {
                PlanRef::Alias(alias) => {
                    if let Some(schema) = plan.clone().get_schema_for_alias(alias)? {
                        // make sure column exists in schema
                        schema.get_field(name)?;

                        Ok(Transformed::yes(resolved_col(name.clone())))
                    } else if let Some(schema) = plan_schema {
                        Ok(Transformed::yes(Arc::new(Expr::Column(Column::Resolved(ResolvedColumn::OuterRef(schema.get_field(name)?.clone()))))))
                    } else {
                        Err(DaftError::FieldNotFound(format!("Column {alias}.{name} is an outer column but does not have an associated schema.")))
                    }
                }
                PlanRef::Id(id) => {
                    if let Some(schema) = plan.clone().get_schema_for_id(*id)? {
                        // make sure column exists in schema
                        schema.get_field(name)?;

                        Ok(Transformed::yes(resolved_col(name.clone())))
                    } else if let Some(schema) = plan_schema {
                        Ok(Transformed::yes(Arc::new(Expr::Column(Column::Resolved(ResolvedColumn::OuterRef(schema.get_field(name)?.clone()))))))
                    } else {
                        Err(DaftError::FieldNotFound(format!("Column {id}.{name} is an outer column but does not have an associated schema.")))
                    }
                }
                PlanRef::Unqualified => {
                    if plan.schema().has_field(name) {
                        Ok(Transformed::yes(resolved_col(name.clone())))
                    } else if let Some(schema) = plan_schema {
                        Ok(Transformed::yes(Arc::new(Expr::Column(Column::Resolved(ResolvedColumn::OuterRef(schema.get_field(name)?.clone()))))))
                    } else {
                        Err(DaftError::FieldNotFound(format!(
                            "Column {name} is an outer column but does not have an associated schema."
                        )))
                    }
                },
            }
        } else {
            Ok(Transformed::no(e))
        }
    })?.data)
}

fn convert_udfs_to_map_groups(expr: &ExprRef) -> ExprRef {
    expr.clone()
        .transform(|e| match e.as_ref() {
            Expr::Function { func, inputs } if matches!(func, FunctionExpr::Python(_)) => {
                Ok(Transformed::yes(Arc::new(Expr::Agg(AggExpr::MapGroups {
                    func: func.clone(),
                    inputs: inputs.clone(),
                }))))
            }
            _ => Ok(Transformed::no(e)),
        })
        .unwrap()
        .data
}

/// Used for resolving and validating expressions.
/// Specifically, makes sure the expression does not contain aggregations or actor pool UDFs
/// where they are not allowed, and resolves struct accessors and wildcards.
#[derive(Default, TypedBuilder)]
pub struct ExprResolver<'a> {
    #[builder(default)]
    allow_actor_pool_udf: bool,
    #[builder(default)]
    allow_monotonic_id: bool,
    #[builder(via_mutators, mutators(
        pub fn in_agg_context(&mut self, in_agg_context: bool) {
            // workaround since typed_builder can't have defaults for mutator requirements
            self.in_agg_context = in_agg_context;
        }
    ))]
    in_agg_context: bool,
    #[builder(via_mutators, mutators(
        pub fn groupby(&mut self, groupby: &'a Vec<ExprRef>) {
            self.groupby = HashSet::from_iter(groupby);
            self.in_agg_context = true;
        }
    ))]
    groupby: HashSet<&'a ExprRef>,
}

impl ExprResolver<'_> {
    fn resolve_helper(&self, expr: ExprRef, plan: LogicalPlanRef) -> DaftResult<Vec<ExprRef>> {
        if !self.allow_actor_pool_udf && expr.exists(is_actor_pool_udf) {
            return Err(DaftError::ValueError(format!(
                "UDFs with concurrency set are only allowed in projections: {expr}"
            )));
        }

        if !self.allow_monotonic_id && contains_monotonic_id(&expr) {
            return Err(DaftError::ValueError(
                "monotonically_increasing_id() is only allowed in projections".to_string(),
            ));
        }

        expand_wildcard(expr, plan.clone())?
            .into_iter()
            .map(|e| resolve_unresolved_columns(e, plan.clone()))
            .map(|e| {
                if self.in_agg_context {
                    self.validate_expr_in_agg(e?)
                } else {
                    self.validate_expr(e?)
                }
            })
            .collect()
    }

    /// Resolve multiple expressions. Due to wildcards, output vec may contain more expressions than input.
    pub fn resolve(&self, exprs: Vec<ExprRef>, plan: LogicalPlanRef) -> DaftResult<Vec<ExprRef>> {
        // can't flat map because we need to deal with errors
        let resolved_exprs: DaftResult<Vec<Vec<ExprRef>>> = exprs
            .into_iter()
            .map(|e| self.resolve_helper(e, plan.clone()))
            .collect();
        Ok(resolved_exprs?.into_iter().flatten().collect())
    }

    /// Resolve a single expression, ensuring that the output is also a single expression.
    pub fn resolve_single(&self, expr: ExprRef, plan: LogicalPlanRef) -> DaftResult<ExprRef> {
        let resolved_exprs = self.resolve_helper(expr.clone(), plan)?;
        match resolved_exprs.as_slice() {
            [resolved_expr] => Ok(resolved_expr.clone()),
            _ => Err(DaftError::ValueError(format!(
                "Error resolving expression {}: expanded into {} expressions when 1 was expected",
                expr,
                resolved_exprs.len()
            ))),
        }
    }

    fn validate_expr(&self, expr: ExprRef) -> DaftResult<ExprRef> {
        if has_agg(&expr) {
            return Err(DaftError::ValueError(format!(
                "Aggregation expressions are currently only allowed in agg and pivot: {expr}\nIf you would like to have this feature, please see https://github.com/Eventual-Inc/Daft/issues/1979#issue-2170913383",
            )));
        }

        Ok(expr)
    }

    fn validate_expr_in_agg(&self, expr: ExprRef) -> DaftResult<ExprRef> {
        let converted_expr = convert_udfs_to_map_groups(&expr);

        if !self.is_valid_expr_in_agg(&converted_expr) {
            return Err(DaftError::ValueError(format!(
                "Expressions in aggregations must be composed of non-nested aggregation expressions, got {expr}"
            )));
        }

        Ok(converted_expr)
    }

    /// Checks if an expression used in an aggregation is well formed.
    /// Expressions for aggregations must be in the form (optional) non-agg expr <- [(agg exprs <- non-agg exprs) or literals or group by keys]
    ///
    /// # Examples
    ///
    /// Allowed:
    /// - lit("x")
    /// - sum(col("a"))
    /// - sum(col("a")) > 0
    /// - sum(col("a")) - sum(col("b")) > sum(col("c"))
    /// - sum(col("a")) + col("b") when "b" is a group by key
    ///
    /// Not allowed:
    /// - col("a") when "a" is not a group by key
    ///     - not an aggregation
    /// - sum(col("a")) + col("b") when "b" is not a group by key
    ///     - not all branches are aggregations, literals, or group by keys
    fn is_valid_expr_in_agg(&self, expr: &ExprRef) -> bool {
        self.groupby.contains(expr)
            || match expr.as_ref() {
                Expr::Agg(agg_expr) => !agg_expr.children().iter().any(has_agg),
                Expr::Column(_) => false,
                Expr::Literal(_) => true,
                _ => expr.children().iter().all(|e| self.is_valid_expr_in_agg(e)),
            }
    }
}
