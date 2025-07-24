use std::{any::TypeId, collections::HashSet, sync::Arc};

use common_error::{ensure, DaftError, DaftResult};
use common_treenode::{Transformed, TreeNode, TreeNodeRecursion};
use daft_core::prelude::*;
use daft_dsl::{
    expr::window::WindowSpec,
    functions::{struct_::StructExpr, FunctionArg, FunctionArgs, FunctionExpr, ScalarFunction},
    has_agg, is_actor_pool_udf, left_col, resolved_col, right_col, AggExpr, Column, Expr, ExprRef,
    PlanRef, ResolvedColumn, UnresolvedColumn,
};
use typed_builder::TypedBuilder;

use crate::LogicalPlanRef;

/// Duplicate an expression tree for each wildcard match in a column or struct get.
fn expand_wildcard(expr: ExprRef, plan: LogicalPlanRef) -> DaftResult<Vec<ExprRef>> {
    let mut wildcard_expansion = None;

    fn set_wildcard_expansion<'a>(
        wildcard_expansion: &mut Option<Vec<String>>,
        expr: &Expr,
        names: impl Iterator<Item = &'a str>,
    ) -> DaftResult<()> {
        if wildcard_expansion.is_some() {
            Err(DaftError::ValueError(format!(
                "Error resolving expression {expr}: cannot have multiple wildcard columns in one expression tree.")))
        } else {
            *wildcard_expansion = Some(names.map(ToString::to_string).collect());

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
                                set_wildcard_expansion(&mut wildcard_expansion, &expr, schema.field_names())?;
                            },
                        }
                    }
                    PlanRef::Id(id) => {
                        match (&plan.clone().get_schema_for_id(*id)?, plan_schema) {
                            (None, None) => {
                                return Err(DaftError::ValueError(format!("Plan id {id} in unresolved column is not in current scope, must have schema specified.")));
                            }
                            (Some(schema), _) | (None, Some(schema)) => {
                                set_wildcard_expansion(&mut wildcard_expansion, &expr, schema.field_names())?;
                            },
                        }

                    }
                    PlanRef::Unqualified => set_wildcard_expansion(&mut wildcard_expansion, &expr, plan.schema().field_names())?,
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

                set_wildcard_expansion(&mut wildcard_expansion, &expr, struct_fields.iter().map(|f| f.name.as_str()))?;
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

/// Check if you can resolve an unresolved column to the provided plan
fn col_resolves_to_plan(column: &UnresolvedColumn, plan: &LogicalPlanRef) -> DaftResult<bool> {
    let plan_schema = plan.schema();

    Ok(match &column.plan_ref {
        PlanRef::Alias(alias) => {
            if let Some(schema) = plan.get_schema_for_alias(alias)? {
                // make sure column exists in schemas
                schema.get_field(&column.name)?;
                plan_schema.get_field(&column.name)?;

                true
            } else {
                false
            }
        }
        PlanRef::Id(id) => {
            if let Some(schema) = plan.get_schema_for_id(*id)? {
                // make sure column exists in schemas
                schema.get_field(&column.name)?;
                plan_schema.get_field(&column.name)?;

                true
            } else {
                false
            }
        }
        PlanRef::Unqualified => plan.schema().has_field(&column.name),
    })
}

fn replace_element_with_column_ref(expr: ExprRef, replacement: ExprRef) -> DaftResult<ExprRef> {
    expr.transform(|e| {
        if matches!(
            e.as_ref(),
            Expr::Column(Column::Unresolved(UnresolvedColumn {
                name,
                plan_ref: PlanRef::Unqualified,
                plan_schema: None
            })) if name.as_ref() == ""
        ) {
            Ok(Transformed::yes(replacement.clone()))
        } else {
            Ok(Transformed::no(e))
        }
    })
    .map(|res| res.data)
}

fn resolve_list_evals(expr: ExprRef) -> DaftResult<ExprRef> {
    // Functions that can support an eval/map context

    let eval_functions: &[TypeId] = &[TypeId::of::<daft_functions_list::ListMap>()];

    expr.transform_down(|e| {
        let expr_ref = e.as_ref();
        if let Expr::ScalarFunction(sf) = expr_ref
            && eval_functions.contains(&sf.udf.type_id())
        {
            // the `list` type should always be the first element
            let inputs = sf.inputs.clone();
            let list_col = inputs.first().ok_or_else(|| {
                DaftError::ValueError("list should have at least one element".to_string())
            })?;

            let exploded = list_col.clone().explode()?;

            let mut new_inputs = Vec::with_capacity(inputs.len());
            new_inputs.push(FunctionArg::Unnamed(list_col.clone()));

            for input in inputs.iter().skip(1) {
                ensure!(
                   !matches!(input.inner().as_ref(), Expr::Column(..)),
                   ValueError: "unexpected column reference"

                );

                let replaced = match input {
                    daft_dsl::functions::FunctionArg::Named { name, arg } => {
                        daft_dsl::functions::FunctionArg::Named {
                            name: name.clone(),
                            arg: replace_element_with_column_ref(arg.clone(), exploded.clone())?,
                        }
                    }
                    daft_dsl::functions::FunctionArg::Unnamed(arg) => {
                        daft_dsl::functions::FunctionArg::Unnamed(replace_element_with_column_ref(
                            arg.clone(),
                            exploded.clone(),
                        )?)
                    }
                };
                new_inputs.push(replaced);
            }
            let sf = ScalarFunction {
                udf: sf.udf.clone(),
                inputs: FunctionArgs::new_unchecked(new_inputs),
            };
            Ok(Transformed::yes(Expr::ScalarFunction(sf).arced()))
        } else {
            Ok(Transformed::no(e.clone()))
        }
    })
    .map(|res| res.data)
}

fn resolve_to_basic_and_outer_cols(expr: ExprRef, plan: &LogicalPlanRef) -> DaftResult<ExprRef> {
    expr.transform(|e| {
        if let Expr::Column(Column::Unresolved(column)) = e.as_ref() {
            if col_resolves_to_plan(column, plan)? {
                Ok(Transformed::yes(resolved_col(column.name.clone())))
            } else if let Some(schema) = &column.plan_schema {
                Ok(Transformed::yes(Arc::new(Expr::Column(Column::Resolved(
                    ResolvedColumn::OuterRef(
                        schema.get_field(&column.name)?.clone(),
                        column.plan_ref.clone(),
                    ),
                )))))
            } else {
                Err(DaftError::FieldNotFound(format!("Column {e} not found.")))
            }
        } else {
            Ok(Transformed::no(e))
        }
    })
    .map(|res| res.data)
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
    #[builder(default)]
    allow_explode: bool,
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
    fn check_expr(&self, expr: &ExprRef) -> DaftResult<()> {
        if !self.allow_actor_pool_udf && expr.exists(is_actor_pool_udf) {
            return Err(DaftError::ValueError(format!(
                "UDFs with concurrency set are only allowed in projections: {expr}"
            )));
        }

        if !self.allow_monotonic_id
            && expr.exists(|e| match e.as_ref() {
                Expr::ScalarFunction(func) => func.name() == "monotonically_increasing_id",
                _ => false,
            })
        {
            return Err(DaftError::ValueError(
                "monotonically_increasing_id() is only allowed in projections".to_string(),
            ));
        }

        if !self.allow_explode && expr.exists(|e| matches!(e.as_ref(), Expr::ScalarFunction(sf) if sf.is_function_type::<daft_functions_list::Explode>())) {
            return Err(DaftError::ValueError(
                "explode() is only allowed in projections".to_string(),
            ));
        }

        Ok(())
    }

    fn resolve_helper(&self, expr: ExprRef, plan: LogicalPlanRef) -> DaftResult<Vec<ExprRef>> {
        self.check_expr(&expr)?;

        expand_wildcard(expr, plan.clone())?
            .into_iter()
            .map(resolve_list_evals)
            .map(|e| resolve_to_basic_and_outer_cols(e?, &plan))
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

    /// Resolve expression in `ON` clause of join
    pub fn resolve_join_on(
        &self,
        expr: ExprRef,
        left_plan: LogicalPlanRef,
        right_plan: LogicalPlanRef,
    ) -> DaftResult<ExprRef> {
        self.check_expr(&expr)?;

        expr.transform(|e| {
            if let Expr::Column(Column::Unresolved(column)) = e.as_ref() {
                let resolves_left = col_resolves_to_plan(column, &left_plan)?;
                let resolves_right = col_resolves_to_plan(column, &right_plan)?;

                match (resolves_left, resolves_right) {
                    (true, true) => Err(DaftError::ValueError(format!(
                        "Ambiguous column reference in join predicate: {e}"
                    ))),
                    (true, false) => Ok(Transformed::yes(left_col(
                        left_plan.schema().get_field(&column.name)?.clone(),
                    ))),
                    (false, true) => Ok(Transformed::yes(right_col(
                        right_plan.schema().get_field(&column.name)?.clone(),
                    ))),
                    (false, false) => {
                        Err(DaftError::FieldNotFound(format!("Column {e} not found.")))
                    }
                }
            } else {
                Ok(Transformed::no(e))
            }
        })
        .map(|res| res.data)
    }

    /// Resolve expressions in a window specification
    pub fn resolve_window_spec(
        &self,
        window_spec: WindowSpec,
        plan: LogicalPlanRef,
    ) -> DaftResult<WindowSpec> {
        let partition_by = if !window_spec.partition_by.is_empty() {
            self.resolve(window_spec.partition_by, plan.clone())?
        } else {
            vec![]
        };

        let order_by = if !window_spec.order_by.is_empty() {
            self.resolve(window_spec.order_by, plan)?
        } else {
            vec![]
        };

        Ok(WindowSpec {
            partition_by,
            order_by,
            descending: window_spec.descending,
            nulls_first: window_spec.nulls_first,
            frame: window_spec.frame,
            min_periods: window_spec.min_periods,
        })
    }

    fn validate_expr(&self, expr: ExprRef) -> DaftResult<ExprRef> {
        if has_agg(&expr) {
            return Err(DaftError::ValueError(format!(
                "Aggregation expressions are currently only allowed in agg, pivot, and window: {expr}\nIf you would like to have this feature, please see https://github.com/Eventual-Inc/Daft/issues/1979#issue-2170913383",
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
