use std::{
    cell::{Ref, RefCell, RefMut},
    collections::{HashMap, HashSet},
    path::Path,
    rc::Rc,
    sync::Arc,
};

use common_error::DaftResult;
use daft_algebra::boolean::combine_conjunction;
use daft_catalog::Identifier;
use daft_core::prelude::*;
use daft_dsl::{
    has_agg, lit, literals_to_series, null_lit, resolved_col, unresolved_col, Column, Expr,
    ExprRef, LiteralValue, Operator, PlanRef, Subquery, UnresolvedColumn,
};
use daft_functions::{
    numeric::{ceil::ceil, floor::floor},
    utf8::{ilike, like, to_date, to_datetime},
};
use daft_logical_plan::{
    ops::{SetQuantifier, UnionStrategy},
    JoinOptions, LogicalPlanBuilder, LogicalPlanRef,
};
use daft_session::Session;
use itertools::Itertools;
use sqlparser::{
    ast::{
        self, BinaryOperator, CastKind, ColumnDef, DateTimeField, Distinct, ExcludeSelectItem,
        FunctionArg, FunctionArgExpr, GroupByExpr, Ident, ObjectName, Query, SelectItem, SetExpr,
        Subscript, TableAlias, TableFunctionArgs, TableWithJoins, TimezoneInfo, UnaryOperator,
        Value, WildcardAdditionalOptions, With,
    },
    dialect::GenericDialect,
    parser::{Parser, ParserOptions},
    tokenizer::Tokenizer,
};

use crate::{
    column_not_found_err, error::*, invalid_operation_err, schema::sql_dtype_to_dtype,
    table_not_found_err, unsupported_sql_err,
};

/// Bindings are used to lookup in-scope tables, views, and columns (targets T).
/// This is an incremental step towards proper name resolution.
struct Bindings<T>(HashMap<String, T>);

impl<T> Bindings<T> {
    /// Gets a binding by identiifer
    pub fn get(&self, ident: &str) -> Option<&T> {
        // TODO use identifiers and handle case-normalization
        self.0.get(ident)
    }

    /// Inserts a new binding by name
    pub fn insert(&mut self, name: String, target: T) -> Option<T> {
        // TODO use names and handle case-normalization
        self.0.insert(name, target)
    }

    /// Clears all bound targets
    pub fn clear(&mut self) {
        self.0.clear();
    }
}

impl<T> Default for Bindings<T> {
    fn default() -> Self {
        Self(Default::default())
    }
}

struct OrderByExprs {
    exprs: Vec<ExprRef>,
    descending: Vec<bool>,
    nulls_first: Vec<bool>,
}

/// Context for the planning the statement.
/// TODO consolidate SQLPlanner state to the single context.
/// TODO move bound_ctes into per-planner scope since these are a scoped concept.
#[derive(Default)]
struct PlannerContext {
    /// Session provides access to metadata and the path for name resolution.
    /// TODO move into SQLPlanner once state is flipped.
    /// TODO consider decoupling session from planner via a resolver trait.
    session: Rc<Session>,
    /// Bindings for common table expressions (cte).
    bound_ctes: Bindings<LogicalPlanBuilder>,
}

impl PlannerContext {
    /// Creates a new context from the session
    fn new(session: Rc<Session>) -> Self {
        Self {
            session,
            bound_ctes: Bindings::default(),
        }
    }

    /// Clears the entire statement context
    fn clear(&mut self) {
        self.bound_ctes.clear();
    }
}

/// An SQLPlanner is created for each scope to bind names and translate to logical plans.
/// TODO flip SQLPlanner to pass scoped state objects rather than being stateful itself.
/// This gives us control on state management without coupling our scopes to the call stack.
/// It also eliminates extra references on the shared context and we can remove interior mutability.
#[derive(Default)]
pub struct SQLPlanner<'a> {
    /// Shared context for all planners
    context: Rc<RefCell<PlannerContext>>,
    /// Planner for the outer scope
    parent: Option<&'a SQLPlanner<'a>>,
    /// In-scope bindings introduced by the current relation's schema
    pub(crate) current_plan: Option<LogicalPlanBuilder>,
    /// Aliases from selection that can be used in other clauses
    /// but may not yet be in the schema of `current_relation`.
    bound_columns: Bindings<ExprRef>,
}

impl<'a> SQLPlanner<'a> {
    /// Create a new query planner for the session.
    pub fn new(session: Rc<Session>) -> Self {
        let context = PlannerContext::new(session);
        let context = Rc::new(RefCell::new(context));
        Self {
            context,
            ..Default::default()
        }
    }

    fn new_child(&'a self) -> Self {
        Self {
            context: self.context.clone(),
            parent: Some(self),
            ..Default::default()
        }
    }

    fn new_with_context(&self) -> Self {
        Self {
            context: self.context.clone(),
            ..Default::default()
        }
    }

    /// Set `self.current_plan`. Should only be called once per query.
    fn set_plan(&mut self, plan: LogicalPlanBuilder) {
        debug_assert!(self.current_plan.is_none());

        self.current_plan = Some(plan);
    }

    fn update_plan<E>(
        &mut self,
        f: impl FnOnce(&LogicalPlanBuilder) -> Result<LogicalPlanBuilder, E>,
    ) -> Result<(), E> {
        let plan = self.current_plan.as_ref().expect("current plan is set");
        let new_plan = f(plan)?;

        self.current_plan = Some(new_plan);

        Ok(())
    }

    fn current_plan_ref(&self) -> &LogicalPlanBuilder {
        self.current_plan.as_ref().expect("current plan is set")
    }

    fn context_mut(&self) -> RefMut<'_, PlannerContext> {
        self.context.as_ref().borrow_mut()
    }

    fn bound_ctes(&self) -> Ref<'_, Bindings<LogicalPlanBuilder>> {
        Ref::map(self.context.borrow(), |i| &i.bound_ctes)
    }

    /// Get the table associated with the name from the session and wrap in a SubqueryAlias.
    fn get_table(&self, name: &Identifier) -> SQLPlannerResult<LogicalPlanBuilder> {
        let table = self.session().get_table(name)?;
        let plan = table.get_logical_plan()?;
        Ok(LogicalPlanBuilder::from(plan).alias(name.name.clone()))
    }

    /// Borrow the planning session
    fn session(&self) -> Ref<'_, Rc<Session>> {
        Ref::map(self.context.borrow(), |i| &i.session)
    }

    /// Clears the current context used for planning a SQL query
    fn clear_context(&mut self) {
        self.current_plan = None;
        self.context_mut().clear();
    }

    fn plan_ctes(&self, with: &With) -> SQLPlannerResult<()> {
        if with.recursive {
            unsupported_sql_err!("Recursive CTEs are not supported");
        }

        for cte in &with.cte_tables {
            if cte.materialized.is_some() {
                unsupported_sql_err!("MATERIALIZED is not supported");
            }

            if cte.from.is_some() {
                invalid_operation_err!("FROM should only exist in recursive CTEs");
            }

            let plan = self.new_with_context().plan_query(&cte.query)?;

            let plan = apply_table_alias(plan, &cte.alias)?;

            self.context_mut()
                .bound_ctes
                .insert(cte.alias.name.value.clone(), plan);
        }
        Ok(())
    }

    pub fn plan_sql(&mut self, sql: &str) -> SQLPlannerResult<LogicalPlanRef> {
        let tokens = Tokenizer::new(&GenericDialect {}, sql).tokenize()?;

        let mut parser = Parser::new(&GenericDialect {})
            .with_options(ParserOptions {
                trailing_commas: true,
                ..Default::default()
            })
            .with_tokens(tokens);

        // currently only allow one statement
        let statements = parser.parse_statements()?;
        if statements.len() > 1 {
            unsupported_sql_err!(
                "Only exactly one SQL statement allowed, found {}",
                statements.len()
            )
        }

        // plan single statement
        let stmt = &statements[0];
        let plan = self.plan_statement(stmt)?.build();
        self.clear_context();

        Ok(plan)
    }

    pub(crate) fn plan_query(&mut self, query: &Query) -> SQLPlannerResult<LogicalPlanBuilder> {
        check_query_features(query)?;

        let selection = match query.body.as_ref() {
            SetExpr::Select(selection) => selection,
            SetExpr::Query(_) => unsupported_sql_err!("Subqueries are not supported"),
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => {
                use sqlparser::ast::{
                    SetOperator::{Intersect, Union},
                    SetQuantifier as SQLSetQuantifier,
                };
                fn make_query(expr: &SetExpr) -> Query {
                    Query {
                        with: None,
                        body: Box::new(expr.clone()),
                        order_by: None,
                        limit: None,
                        limit_by: vec![],
                        offset: None,
                        fetch: None,
                        locks: vec![],
                        for_clause: None,
                        settings: None,
                        format_clause: None,
                    }
                }

                let left = self.new_with_context().plan_query(&make_query(left))?;
                let right = self.new_with_context().plan_query(&make_query(right))?;

                return match (op, set_quantifier) {
                    (Union, set_quantifier) => {
                        let (set_quantifier, strategy) = match set_quantifier {
                            SQLSetQuantifier::All => {
                                (SetQuantifier::All, UnionStrategy::Positional)
                            }
                            SQLSetQuantifier::None | SQLSetQuantifier::Distinct => {
                                (SetQuantifier::Distinct, UnionStrategy::Positional)
                            }
                            SQLSetQuantifier::ByName | SQLSetQuantifier::DistinctByName => {
                                (SetQuantifier::Distinct, UnionStrategy::ByName)
                            }
                            SQLSetQuantifier::AllByName => {
                                (SetQuantifier::All, UnionStrategy::ByName)
                            }
                        };
                        left.union(&right, set_quantifier, strategy)
                            .map_err(|e| e.into())
                    }

                    (Intersect, SQLSetQuantifier::All) => {
                        left.intersect(&right, true).map_err(|e| e.into())
                    }
                    (Intersect, SQLSetQuantifier::None | SQLSetQuantifier::Distinct) => {
                        left.intersect(&right, false).map_err(|e| e.into())
                    }
                    (op, set_quantifier) => {
                        unsupported_sql_err!("{op} {set_quantifier} is not supported.")
                    }
                };
            }
            SetExpr::Values(..) => unsupported_sql_err!("VALUES are not supported"),
            SetExpr::Insert(..) => unsupported_sql_err!("INSERT is not supported"),
            SetExpr::Update(..) => unsupported_sql_err!("UPDATE is not supported"),
            SetExpr::Table(..) => unsupported_sql_err!("TABLE is not supported"),
        };
        check_select_features(selection)?;

        if let Some(with) = &query.with {
            self.plan_ctes(with)?;
        }

        // FROM/JOIN
        let from = selection.clone().from;
        self.plan_from(&from)?;

        // SELECT
        let projections = selection
            .projection
            .iter()
            .map(|p| self.select_item_to_expr(p))
            .flatten_ok()
            .collect::<Result<Vec<_>, _>>()?;

        // WHERE
        if let Some(selection) = &selection.selection {
            let filter = self.plan_expr(selection)?;
            self.update_plan(|plan| plan.filter(filter))?;
        }

        // GROUP BY
        let mut groupby_exprs = Vec::new();

        match &selection.group_by {
            GroupByExpr::All(s) => {
                if !s.is_empty() {
                    unsupported_sql_err!("GROUP BY ALL");
                }
            }
            GroupByExpr::Expressions(expressions, _) => {
                groupby_exprs = expressions
                    .iter()
                    .map(|expr| self.plan_expr(expr))
                    .collect::<SQLPlannerResult<Vec<_>>>()?;
            }
        }

        // ORDER BY
        let order_by = query
            .order_by
            .as_ref()
            .map(|order_by| {
                if order_by.interpolate.is_some() {
                    unsupported_sql_err!("ORDER BY [query] [INTERPOLATE]");
                }

                self.plan_order_by_exprs(&order_by.exprs)
            })
            .transpose()?;

        let has_aggs = projections.iter().any(has_agg) || !groupby_exprs.is_empty();

        if has_aggs {
            let having = selection
                .having
                .as_ref()
                .map(|h| self.plan_expr(h))
                .transpose()?;

            self.plan_aggregate_query(projections, order_by, groupby_exprs, having)?;
        } else {
            self.plan_non_agg_query(projections, order_by)?;
        }

        match &selection.distinct {
            Some(Distinct::Distinct) => {
                self.update_plan(|plan| plan.distinct())?;
            }
            Some(Distinct::On(_)) => unsupported_sql_err!("DISTINCT ON"),
            None => {}
        }

        if let Some(limit) = &query.limit {
            let limit = self.plan_expr(limit)?;
            if let Expr::Literal(LiteralValue::Int64(limit)) = limit.as_ref() {
                self.update_plan(|plan| plan.limit(*limit, true))?; // TODO: Should this be eager or not?
            } else {
                invalid_operation_err!(
                    "LIMIT <n> must be a constant integer, instead got: {limit}"
                );
            }
        }

        Ok(self.current_plan.clone().unwrap())
    }

    fn plan_non_agg_query(
        &mut self,
        projections: Vec<Arc<Expr>>,
        order_by: Option<OrderByExprs>,
    ) -> Result<(), PlannerError> {
        if let Some(OrderByExprs {
            exprs,
            descending,
            nulls_first,
        }) = order_by
        {
            self.update_plan(|plan| plan.sort(exprs, descending, nulls_first))?;
        }

        self.update_plan(|plan| plan.select(projections))?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_aggregate_query(
        &mut self,
        projections: Vec<Arc<Expr>>,
        order_by: Option<OrderByExprs>,
        groupby_exprs: Vec<Arc<Expr>>,
        having: Option<Arc<Expr>>,
    ) -> Result<(), PlannerError> {
        let mut aggs = HashSet::new();

        let schema = self.current_plan_ref().schema();

        let projections = projections
            .into_iter()
            .map(|expr| {
                if has_agg(&expr) {
                    aggs.insert(expr.clone());

                    resolved_col(expr.name())
                } else {
                    expr
                }
            })
            .collect();

        let having = having.map(|expr| {
            // ensure that all aggs required for having filter are present
            if has_agg(&expr) {
                let id = expr.semantic_id(&schema).id;

                aggs.insert(expr.alias(id.clone()));

                resolved_col(id)
            } else {
                expr
            }
        });

        let order_by = order_by.map(
            |OrderByExprs {
                 exprs,
                 descending,
                 nulls_first,
             }| {
                // for order by expressions with aggregations,
                // ensure aggregations are present and update expression to point to agg
                let updated_exprs = exprs
                    .into_iter()
                    .map(|e| {
                        if has_agg(&e) {
                            let id = e.semantic_id(&schema).id;

                            aggs.insert(e.alias(id.clone()));

                            resolved_col(id)
                        } else {
                            e
                        }
                    })
                    .collect();

                OrderByExprs {
                    exprs: updated_exprs,
                    descending,
                    nulls_first,
                }
            },
        );

        self.update_plan(|plan| plan.aggregate(aggs.into_iter().collect(), groupby_exprs))?;

        if let Some(having) = having {
            self.update_plan(|plan| plan.filter(having))?;
        }

        if let Some(OrderByExprs {
            exprs,
            descending,
            nulls_first,
        }) = order_by
        {
            self.update_plan(|plan| plan.sort(exprs, descending, nulls_first))?;
        }

        self.update_plan(|plan| plan.select(projections))?;

        Ok(())
    }

    fn plan_order_by_exprs(
        &self,
        expr: &[sqlparser::ast::OrderByExpr],
    ) -> SQLPlannerResult<OrderByExprs> {
        if expr.is_empty() {
            unsupported_sql_err!("ORDER BY []");
        }
        let mut exprs = Vec::with_capacity(expr.len());
        let mut descending = Vec::with_capacity(expr.len());
        let mut nulls_first = Vec::with_capacity(expr.len());
        for order_by_expr in expr {
            match (order_by_expr.asc, order_by_expr.nulls_first) {
                // ---------------------------
                // all of these are equivalent
                // ---------------------------
                // ORDER BY expr
                (None, None) |
                // ORDER BY expr ASC
                (Some(true), None) |
                // ORDER BY expr NULLS LAST
                (None, Some(false)) |
                // ORDER BY expr ASC NULLS LAST
                (Some(true), Some(false)) => {
                   nulls_first.push(false);
                   descending.push(false);
               },
                // ---------------------------


                // ---------------------------
                // ORDER BY expr NULLS FIRST
                (None, Some(true)) |
                // ORDER BY expr ASC NULLS FIRST
                (Some(true), Some(true)) => {
                    nulls_first.push(true);
                    descending.push(false);
                }
                // ---------------------------

                // ORDER BY expr DESC
                (Some(false), None) |
                // ORDER BY expr DESC NULLS FIRST
                (Some(false), Some(true)) => {
                    nulls_first.push(true);
                    descending.push(true);
                },
                // ORDER BY expr DESC NULLS LAST
                (Some(false), Some(false)) => {
                    nulls_first.push(false);
                    descending.push(true);
                }

            };
            if order_by_expr.with_fill.is_some() {
                unsupported_sql_err!("WITH FILL");
            }
            let expr = self.plan_expr(&order_by_expr.expr)?;

            exprs.push(expr);
        }

        Ok(OrderByExprs {
            exprs,
            descending,
            nulls_first,
        })
    }

    /// Plans a single set of table and joins in a FROM clause.
    fn plan_single_from(&self, from: &TableWithJoins) -> SQLPlannerResult<LogicalPlanBuilder> {
        macro_rules! return_non_ident_errors {
            ($e:expr) => {
                if !matches!(
                    $e,
                    PlannerError::ColumnNotFound { .. } | PlannerError::TableNotFound { .. }
                ) {
                    return Err($e);
                }
            };
        }

        #[allow(clippy::too_many_arguments)]
        fn process_join_on(
            sql_expr: &sqlparser::ast::Expr,
            left_planner: &SQLPlanner,
            right_planner: &SQLPlanner,
            left_on: &mut Vec<ExprRef>,
            right_on: &mut Vec<ExprRef>,
            null_eq_nulls: &mut Vec<bool>,
            left_filters: &mut Vec<ExprRef>,
            right_filters: &mut Vec<ExprRef>,
        ) -> SQLPlannerResult<()> {
            // check if join expression is actually a filter on one of the tables
            match (
                left_planner.plan_expr(sql_expr),
                right_planner.plan_expr(sql_expr),
            ) {
                (Ok(_), Ok(_)) => {
                    return Err(PlannerError::invalid_operation(format!(
                        "Ambiguous reference to column name in join: {}",
                        sql_expr
                    )));
                }
                (Ok(expr), _) => {
                    left_filters.push(expr);
                    return Ok(());
                }
                (_, Ok(expr)) => {
                    right_filters.push(expr);
                    return Ok(());
                }
                (Err(left_err), Err(right_err)) => {
                    return_non_ident_errors!(left_err);
                    return_non_ident_errors!(right_err);
                }
            }

            match sql_expr {
                // join key
                sqlparser::ast::Expr::BinaryOp {
                    left,
                    right,
                    op: op @ BinaryOperator::Eq,
                }
                | sqlparser::ast::Expr::BinaryOp {
                    left,
                    right,
                    op: op @ BinaryOperator::Spaceship,
                } => {
                    let null_equals_null = *op == BinaryOperator::Spaceship;

                    let mut last_error = None;

                    for (left, right) in [(left, right), (right, left)] {
                        let left_expr = left_planner.plan_expr(left);
                        let right_expr = right_planner.plan_expr(right);

                        if let Ok(left_expr) = &left_expr && let Ok(right_expr) = &right_expr {
                            left_on.push(left_expr.clone());
                            right_on.push(right_expr.clone());
                            null_eq_nulls.push(null_equals_null);

                            return Ok(())
                        }

                        for expr_result in [left_expr, right_expr] {
                            if let Err(e) = expr_result {
                                return_non_ident_errors!(e);

                                last_error = Some(e);
                            }
                        }
                    }

                    Err(last_error.unwrap())
                }
                // multiple expressions
                sqlparser::ast::Expr::BinaryOp {
                    left,
                    right,
                    op: BinaryOperator::And,
                } => {
                    process_join_on(left, left_planner, right_planner, left_on, right_on, null_eq_nulls, left_filters, right_filters)?;
                    process_join_on(right, left_planner, right_planner, left_on, right_on, null_eq_nulls, left_filters, right_filters)?;

                    Ok(())
                }
                // nested expression
                sqlparser::ast::Expr::Nested(expr) => process_join_on(
                    expr,
                    left_planner,
                    right_planner,
                    left_on,
                    right_on,
                    null_eq_nulls,
                    left_filters,
                    right_filters,
                ),
                _ => unsupported_sql_err!("JOIN clauses support '=' constraints and filter predicates combined with 'AND'; found expression = {:?}", sql_expr)
            }
        }

        let relation = from.relation.clone();
        let left_plan = self.plan_relation(&relation)?;
        let mut left_planner = self.new_with_context();
        left_planner.set_plan(left_plan);

        for join in &from.joins {
            use sqlparser::ast::{
                JoinConstraint,
                JoinOperator::{FullOuter, Inner, LeftAnti, LeftOuter, LeftSemi, RightOuter},
            };

            let right_plan = self.plan_relation(&join.relation)?;

            let mut join_options = JoinOptions::default();
            if let [id] = right_plan.plan.clone().get_aliases().as_slice() {
                join_options = join_options.prefix(format!("{id}."));
            }

            // construct a planner with the right table to use for expr planning
            let mut right_planner = self.new_with_context();
            right_planner.set_plan(right_plan);

            let (join_type, constraint) = match &join.join_operator {
                Inner(constraint) => (JoinType::Inner, constraint),
                LeftOuter(constraint) => (JoinType::Left, constraint),
                RightOuter(constraint) => (JoinType::Right, constraint),
                FullOuter(constraint) => (JoinType::Outer, constraint),
                LeftSemi(constraint) => (JoinType::Semi, constraint),
                LeftAnti(constraint) => (JoinType::Anti, constraint),

                _ => unsupported_sql_err!("Unsupported join type: {:?}", join.join_operator),
            };

            let mut left_on = Vec::new();
            let mut right_on = Vec::new();
            let mut left_filters = Vec::new();
            let mut right_filters = Vec::new();

            let (merge_matching_join_keys, null_eq_nulls) = match &constraint {
                JoinConstraint::On(expr) => {
                    let mut null_eq_nulls = Vec::new();

                    process_join_on(
                        expr,
                        &left_planner,
                        &right_planner,
                        &mut left_on,
                        &mut right_on,
                        &mut null_eq_nulls,
                        &mut left_filters,
                        &mut right_filters,
                    )?;

                    (false, Some(null_eq_nulls))
                }
                JoinConstraint::Using(idents) => {
                    left_on = idents
                        .iter()
                        .map(|i| unresolved_col(i.value.clone()))
                        .collect::<Vec<_>>();
                    right_on.clone_from(&left_on);

                    (true, None)
                }
                JoinConstraint::Natural => unsupported_sql_err!("NATURAL JOIN not supported"),
                JoinConstraint::None => unsupported_sql_err!("JOIN without ON/USING not supported"),
            };

            if let Some(left_predicate) = combine_conjunction(left_filters) {
                left_planner.update_plan(|plan| plan.filter(left_predicate))?;
            }

            if let Some(right_predicate) = combine_conjunction(right_filters) {
                right_planner.update_plan(|plan| plan.filter(right_predicate))?;
            }

            left_planner.update_plan(|plan| {
                plan.join_with_null_safe_equal(
                    right_planner.current_plan.unwrap(),
                    left_on,
                    right_on,
                    null_eq_nulls,
                    join_type,
                    None,
                    join_options.merge_matching_join_keys(merge_matching_join_keys),
                )
            })?;
        }

        Ok(left_planner.current_plan.unwrap())
    }

    /// Plans the FROM clause of a query and populates `self.current_relation`.
    /// Should only be called once per query.
    fn plan_from(&mut self, from: &[TableWithJoins]) -> SQLPlannerResult<()> {
        let plan = if let Some(plan) = from
            .iter()
            .map(|f| self.plan_single_from(f))
            .reduce(|left, right| {
                let left = left?;
                let right = right?;

                let mut join_options = JoinOptions::default();
                if let [id] = right.plan.clone().get_aliases().as_slice() {
                    join_options = join_options.prefix(format!("{id}."));
                }

                Ok(left.cross_join(right, join_options)?)
            })
            .transpose()?
        {
            plan
        } else {
            // singleton plan for SELECT without FROM
            singleton_plan()?
        };

        self.set_plan(plan);

        Ok(())
    }

    fn plan_relation(
        &self,
        rel: &sqlparser::ast::TableFactor,
    ) -> SQLPlannerResult<LogicalPlanBuilder> {
        let (plan, alias) = match rel {
            sqlparser::ast::TableFactor::Table {
                name,
                args: Some(args),
                alias,
                ..
            } => {
                let tbl_fn = name.0.first().unwrap().value.as_str();
                (self.plan_table_function(tbl_fn, args)?, alias)
            }
            sqlparser::ast::TableFactor::Table {
                name,
                args: None,
                alias,
                ..
            } => {
                let plan = if is_table_path(name) {
                    self.plan_relation_path(name.0[0].value.as_str())?
                } else {
                    self.plan_relation_table(name)?
                };

                (plan, alias)
            }
            sqlparser::ast::TableFactor::Derived {
                lateral,
                subquery,
                alias,
            } => {
                if *lateral {
                    unsupported_sql_err!("LATERAL");
                }
                let subquery = self.new_with_context().plan_query(subquery)?;
                (subquery, alias)
            }
            sqlparser::ast::TableFactor::TableFunction { .. } => {
                unsupported_sql_err!("Unsupported table factor: TableFunction")
            }
            sqlparser::ast::TableFactor::Function { .. } => {
                unsupported_sql_err!("Unsupported table factor: Function")
            }
            sqlparser::ast::TableFactor::UNNEST { .. } => {
                unsupported_sql_err!("Unsupported table factor: UNNEST")
            }
            sqlparser::ast::TableFactor::JsonTable { .. } => {
                unsupported_sql_err!("Unsupported table factor: JsonTable")
            }
            sqlparser::ast::TableFactor::NestedJoin { .. } => {
                unsupported_sql_err!("Unsupported table factor: NestedJoin")
            }
            sqlparser::ast::TableFactor::Pivot { .. } => {
                unsupported_sql_err!("Unsupported table factor: Pivot")
            }
            sqlparser::ast::TableFactor::Unpivot { .. } => {
                unsupported_sql_err!("Unsupported table factor: Unpivot")
            }
            sqlparser::ast::TableFactor::MatchRecognize { .. } => {
                unsupported_sql_err!("Unsupported table factor: MatchRecognize")
            }
        };
        if let Some(alias) = alias {
            apply_table_alias(plan, alias)
        } else {
            Ok(plan)
        }
    }

    /// Plan a `FROM <path>` table factor by rewriting to relevant table-value function.
    fn plan_relation_path(&self, path: &str) -> SQLPlannerResult<LogicalPlanBuilder> {
        let func = match Path::new(path).extension() {
            Some(ext) if ext.eq_ignore_ascii_case("csv") => "read_csv",
            Some(ext) if ext.eq_ignore_ascii_case("json") => "read_json",
            Some(ext) if ext.eq_ignore_ascii_case("jsonl") => "read_json",
            Some(ext) if ext.eq_ignore_ascii_case("parquet") => "read_parquet",
            Some(_) => invalid_operation_err!("unsupported file path extension: {}", path),
            None => invalid_operation_err!("unsupported file path, no extension: {}", path),
        };
        let args = TableFunctionArgs {
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                ast::Expr::Value(Value::SingleQuotedString(path.to_string())),
            ))],
            settings: None,
        };
        self.plan_table_function(func, &args)
    }

    /// Plan a `FROM <table>` table factor.
    ///
    /// All plans returned by plan_relation_table should have a SubqueryAlias with the table's name.
    pub(crate) fn plan_relation_table(
        &self,
        name: &ObjectName,
    ) -> SQLPlannerResult<LogicalPlanBuilder> {
        let ident = normalize(name);
        let table = if ident.has_qualifier() {
            // qualified search of session metadata
            self.get_table(&ident).ok()
        } else {
            // search bindings then session metadata
            self.bound_ctes()
                .get(&ident.name)
                .cloned()
                .or_else(|| self.get_table(&ident).ok())
        };
        table.ok_or_else(|| PlannerError::table_not_found(ident.to_string()))
    }

    fn plan_identifier(&self, idents: &[Ident]) -> SQLPlannerResult<ExprRef> {
        fn expr_from_idents(
            schema: SchemaRef,
            bound_columns: &Bindings<ExprRef>,
            idents: &[Ident],
            plan_ref: PlanRef,
        ) -> Option<ExprRef> {
            let full_name = compound_ident_to_str(idents);

            // TODO: remove this once we do not do join column renaming
            if schema.has_field(&full_name) {
                return Some(Arc::new(Expr::Column(Column::Unresolved(
                    UnresolvedColumn {
                        name: full_name.into(),
                        plan_ref,
                        plan_schema: Some(schema),
                    },
                ))));
            }

            let [first, rest @ ..] = idents else {
                unreachable!("identifier slice must have at least one value")
            };

            let root_expr = if schema.has_field(&first.value) {
                Arc::new(Expr::Column(Column::Unresolved(UnresolvedColumn {
                    name: first.value.clone().into(),
                    plan_ref,
                    plan_schema: Some(schema),
                })))
            } else if let Some(expr) = bound_columns.get(&first.value) {
                expr.clone()
            } else {
                return None;
            };

            let expr_with_struct_gets = rest.iter().fold(root_expr, |acc, i| {
                daft_dsl::functions::struct_::get(acc, &i.value)
            });

            Some(expr_with_struct_gets)
        }

        let full_name = compound_ident_to_str(idents);

        // if the current relation is not resolved (e.g. in a `sql_expr` call, simply wrap identifier in a unresolved_col)
        let Some(current_plan) = &self.current_plan else {
            return Ok(unresolved_col(full_name));
        };

        let schema = current_plan.plan.schema();

        if let Some(expr) =
            expr_from_idents(schema, &self.bound_columns, idents, PlanRef::Unqualified)
        {
            return Ok(expr);
        }

        if idents.len() > 1 {
            let alias = &idents[0].value;

            if let Some(schema) = current_plan.plan.clone().get_schema_for_alias(alias)? {
                if let Some(expr) = expr_from_idents(
                    schema,
                    &self.bound_columns,
                    &idents[1..],
                    PlanRef::Alias(alias.clone().into()),
                ) {
                    return Ok(expr);
                } else {
                    column_not_found_err!(compound_ident_to_str(&idents[1..]), alias);
                }
            }
        }

        if let Some(parent) = self.parent {
            parent.plan_identifier(idents)
        } else {
            column_not_found_err!(full_name, "current scope")
        }
    }

    fn select_item_to_expr(&mut self, item: &SelectItem) -> SQLPlannerResult<Vec<ExprRef>> {
        fn wildcard_exclude(
            schema: SchemaRef,
            exclusion: &ExcludeSelectItem,
        ) -> DaftResult<Schema> {
            match exclusion {
                ExcludeSelectItem::Single(column) => schema.exclude(&[&column.to_string()]),
                ExcludeSelectItem::Multiple(columns) => {
                    let items = columns
                        .iter()
                        .map(std::string::ToString::to_string)
                        .collect::<Vec<String>>();

                    schema.exclude(items.as_slice())
                }
            }
        }

        match item {
            SelectItem::ExprWithAlias { expr, alias } => {
                let expr = self.plan_expr(expr)?;
                let alias = alias.value.to_string();
                self.bound_columns.insert(alias.clone(), expr.clone());
                Ok(vec![expr.alias(alias)])
            }
            SelectItem::UnnamedExpr(expr) => self.plan_expr(expr).map(|e| vec![e]),

            SelectItem::Wildcard(wildcard_opts) => {
                check_wildcard_options(wildcard_opts)?;

                if let Some(exclude) = &wildcard_opts.opt_exclude {
                    let schema = self.current_plan_ref().schema();
                    wildcard_exclude(schema, exclude)
                        .map(|excluded| {
                            excluded
                                .names()
                                .iter()
                                .map(|n| unresolved_col(n.as_ref()))
                                .collect::<Vec<_>>()
                        })
                        .map_err(std::convert::Into::into)
                } else if let Some(current_plan) = &self.current_plan {
                    Ok(current_plan
                        .schema()
                        .names()
                        .iter()
                        .map(|n| unresolved_col(n.clone()))
                        .collect())
                } else {
                    Ok(vec![unresolved_col("*")])
                }
            }
            // TODO: support wildcard struct gets
            SelectItem::QualifiedWildcard(object_name, wildcard_opts) => {
                check_wildcard_options(wildcard_opts)?;
                let ident = normalize(object_name);
                let ident_name = ident.to_string();
                let Some(current_plan) = self.current_plan.as_ref() else {
                    table_not_found_err!(ident_name);
                };
                let Some(subquery_schema) = current_plan
                    .plan
                    .clone()
                    .get_schema_for_alias(&ident_name)?
                else {
                    table_not_found_err!(ident_name);
                };

                let columns = if let Some(exclude) = &wildcard_opts.opt_exclude {
                    Arc::new(wildcard_exclude(subquery_schema.clone(), exclude)?)
                } else {
                    // I believe clippy is wrong here. It does not compile without this clone - kevin
                    #[allow(clippy::redundant_clone)]
                    subquery_schema.clone()
                };

                let plan_schema = current_plan.plan.schema();

                Ok(columns
                    .names()
                    .iter()
                    .map(|n| {
                        let full_name = format!("{ident_name}.{n}");
                        if plan_schema.has_field(&full_name) {
                            // TODO: remove this once we do not do join column renaming
                            unresolved_col(full_name).alias(n.clone())
                        } else {
                            Arc::new(Expr::Column(Column::Unresolved(UnresolvedColumn {
                                name: n.clone().into(),
                                plan_ref: PlanRef::Alias(ident_name.clone().into()),
                                plan_schema: Some(subquery_schema.clone()),
                            })))
                        }
                    })
                    .collect())
            }
        }
    }

    fn column_to_field(&self, column_def: &ColumnDef) -> SQLPlannerResult<Field> {
        let ColumnDef {
            name,
            data_type,
            collation,
            options,
        } = column_def;

        if let Some(collation) = collation {
            unsupported_sql_err!("collation operation ({collation:?}) is not supported")
        }

        if !options.is_empty() {
            unsupported_sql_err!("unsupported options: {options:?}")
        }

        let data_type = sql_dtype_to_dtype(data_type)?;

        Ok(Field::new(name.value.clone(), data_type))
    }

    fn value_to_lit(&self, value: &Value) -> SQLPlannerResult<LiteralValue> {
        Ok(match value {
            Value::SingleQuotedString(s) => LiteralValue::Utf8(s.clone()),
            Value::Number(n, _) => n
                .parse::<i64>()
                .map(LiteralValue::Int64)
                .or_else(|_| n.parse::<f64>().map(LiteralValue::Float64))
                .map_err(|_| {
                    PlannerError::invalid_operation(format!(
                        "could not parse number literal '{n:?}'"
                    ))
                })?,
            Value::Boolean(b) => LiteralValue::Boolean(*b),
            Value::Null => LiteralValue::Null,
            _ => {
                return Err(PlannerError::invalid_operation(
                    "Only string, number, boolean and null literals are supported. Instead found: `{value}`",
                ))
            }
        })
    }

    pub(crate) fn plan_expr(&self, expr: &sqlparser::ast::Expr) -> SQLPlannerResult<ExprRef> {
        use sqlparser::ast::Expr as SQLExpr;
        match expr {
            SQLExpr::Identifier(ident) => self.plan_identifier(std::slice::from_ref(ident)),
            SQLExpr::Value(v) => self.value_to_lit(v).map(Expr::Literal).map(Arc::new),
            SQLExpr::BinaryOp { left, op, right } => {
                let left = self.plan_expr(left)?;
                let right = self.plan_expr(right)?;
                let op = self.sql_operator_to_operator(op)?;
                Ok(Expr::BinaryOp { left, op, right }.arced())
            }
            SQLExpr::Cast {
                kind: CastKind::Cast | CastKind::DoubleColon,
                expr,
                data_type,
                format: None,
            } => {
                let dtype = sql_dtype_to_dtype(data_type)?;
                let expr = self.plan_expr(expr)?;
                Ok(expr.cast(&dtype))
            }
            SQLExpr::IsFalse(expr) => Ok(self.plan_expr(expr)?.eq(lit(false))),
            SQLExpr::IsNotFalse(_) => Ok(self.plan_expr(expr)?.eq(lit(false)).not()),
            SQLExpr::IsTrue(expr) => Ok(self.plan_expr(expr)?.eq(lit(true))),
            SQLExpr::IsNotTrue(expr) => Ok(self.plan_expr(expr)?.eq(lit(true)).not()),
            SQLExpr::IsNull(expr) => Ok(self.plan_expr(expr)?.is_null()),
            SQLExpr::IsNotNull(expr) => Ok(self.plan_expr(expr)?.is_null().not()),
            SQLExpr::UnaryOp { op, expr } => self.plan_unary_op(op, expr),
            SQLExpr::CompoundIdentifier(idents) => self.plan_identifier(idents),
            SQLExpr::CompositeAccess { .. } => {
                unsupported_sql_err!("composite access")
            }
            SQLExpr::IsUnknown(_) => unsupported_sql_err!("IS UNKNOWN"),
            SQLExpr::IsNotUnknown(_) => {
                unsupported_sql_err!("IS NOT UNKNOWN")
            }
            SQLExpr::IsDistinctFrom(_, _) => {
                unsupported_sql_err!("IS DISTINCT FROM")
            }
            SQLExpr::IsNotDistinctFrom(_, _) => {
                unsupported_sql_err!("IS NOT DISTINCT FROM")
            }
            SQLExpr::InList {
                expr,
                list,
                negated,
            } => {
                let expr = self.plan_expr(expr)?;
                let list = list
                    .iter()
                    .map(|e| self.plan_expr(e))
                    .collect::<SQLPlannerResult<Vec<_>>>()?;

                let expr = expr.is_in(list);
                if *negated {
                    Ok(expr.not())
                } else {
                    Ok(expr)
                }
            }
            SQLExpr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let expr = self.plan_expr(expr)?;
                let mut child_planner = self.new_child();
                let subquery = child_planner.plan_query(subquery)?.build();
                let subquery = Subquery { plan: subquery };

                if *negated {
                    Ok(expr.in_subquery(subquery).not())
                } else {
                    Ok(expr.in_subquery(subquery))
                }
            }
            SQLExpr::InUnnest { .. } => unsupported_sql_err!("IN UNNEST"),
            SQLExpr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let expr = self.plan_expr(expr)?;
                let low = self.plan_expr(low)?;
                let high = self.plan_expr(high)?;
                let expr = expr.between(low, high);
                if *negated {
                    Ok(expr.not())
                } else {
                    Ok(expr)
                }
            }
            SQLExpr::Like {
                negated,
                expr,
                pattern,
                escape_char,
            } => {
                if escape_char.is_some() {
                    unsupported_sql_err!("LIKE with escape char")
                }
                let expr = self.plan_expr(expr)?;
                let pattern = self.plan_expr(pattern)?;
                let expr = like(expr, pattern);
                if *negated {
                    Ok(expr.not())
                } else {
                    Ok(expr)
                }
            }
            SQLExpr::ILike {
                negated,
                expr,
                pattern,
                escape_char,
            } => {
                if escape_char.is_some() {
                    unsupported_sql_err!("ILIKE with escape char")
                }
                let expr = self.plan_expr(expr)?;
                let pattern = self.plan_expr(pattern)?;
                let expr = ilike(expr, pattern);
                if *negated {
                    Ok(expr.not())
                } else {
                    Ok(expr)
                }
            }
            SQLExpr::SimilarTo { .. } => unsupported_sql_err!("SIMILAR TO"),
            SQLExpr::RLike { .. } => unsupported_sql_err!("RLIKE"),
            SQLExpr::AnyOp { .. } => unsupported_sql_err!("ANY"),
            SQLExpr::AllOp { .. } => unsupported_sql_err!("ALL"),
            SQLExpr::Convert { .. } => unsupported_sql_err!("CONVERT"),
            SQLExpr::Cast { .. } => unsupported_sql_err!("CAST"),
            SQLExpr::AtTimeZone { .. } => unsupported_sql_err!("AT TIME ZONE"),
            SQLExpr::Extract {
                field,
                syntax: _,
                expr,
            } => {
                use daft_functions::temporal::{self as dt};
                let expr = self.plan_expr(expr)?;

                match field {
                    DateTimeField::Year => Ok(dt::dt_year(expr)),
                    DateTimeField::Month => Ok(dt::dt_month(expr)),
                    DateTimeField::Day => Ok(dt::dt_day(expr)),
                    DateTimeField::DayOfWeek => Ok(dt::dt_day_of_week(expr)),
                    DateTimeField::Date => Ok(dt::dt_date(expr)),
                    DateTimeField::Hour => Ok(dt::dt_hour(expr)),
                    DateTimeField::Minute => Ok(dt::dt_minute(expr)),
                    DateTimeField::Second => Ok(dt::dt_second(expr)),
                    other => unsupported_sql_err!("EXTRACT ({other})"),
                }
            }
            SQLExpr::Ceil { expr, .. } => Ok(ceil(self.plan_expr(expr)?)),
            SQLExpr::Floor { expr, .. } => Ok(floor(self.plan_expr(expr)?)),
            SQLExpr::Position { .. } => unsupported_sql_err!("POSITION"),
            SQLExpr::Substring {
                expr,
                substring_from,
                substring_for,
                special: true, // We only support SUBSTRING(expr, start, length) syntax
            } => {
                let (Some(substring_from), Some(substring_for)) = (substring_from, substring_for)
                else {
                    unsupported_sql_err!("SUBSTRING")
                };

                let expr = self.plan_expr(expr)?;
                let start = self.plan_expr(substring_from)?;
                let length = self.plan_expr(substring_for)?;

                // SQL substring is one indexed
                let start = start.sub(lit(1));

                Ok(daft_functions::utf8::substr(expr, start, length))
            }
            SQLExpr::Substring { special: false, .. } => {
                unsupported_sql_err!("`SUBSTRING(expr [FROM start] [FOR len])` syntax")
            }
            SQLExpr::Trim { .. } => unsupported_sql_err!("TRIM"),
            SQLExpr::Overlay { .. } => unsupported_sql_err!("OVERLAY"),
            SQLExpr::Collate { .. } => unsupported_sql_err!("COLLATE"),
            SQLExpr::Nested(e) => self.plan_expr(e),
            SQLExpr::IntroducedString { .. } => unsupported_sql_err!("INTRODUCED STRING"),
            SQLExpr::TypedString { data_type, value } => match data_type {
                sqlparser::ast::DataType::Date => Ok(to_date(lit(value.as_str()), "%Y-%m-%d")),
                sqlparser::ast::DataType::Timestamp(None, TimezoneInfo::None)
                | sqlparser::ast::DataType::Datetime(None) => Ok(to_datetime(
                    lit(value.as_str()),
                    "%Y-%m-%d %H:%M:%S %z",
                    None,
                )),
                dtype => {
                    unsupported_sql_err!("TypedString with data type {:?}", dtype)
                }
            },
            SQLExpr::Function(func) => self.plan_function(func),
            SQLExpr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if operand.is_some() {
                    unsupported_sql_err!("CASE with operand not yet supported");
                }
                if results.len() != conditions.len() {
                    unsupported_sql_err!("CASE with different number of conditions and results");
                }

                let else_expr = match else_result {
                    Some(expr) => self.plan_expr(expr)?,
                    None => unsupported_sql_err!("CASE with no else result"),
                };

                // we need to traverse from back to front to build the if else chain
                // because we need to start with the else expression
                conditions.iter().zip(results.iter()).rev().try_fold(
                    else_expr,
                    |else_expr, (condition, result)| {
                        let cond = self.plan_expr(condition)?;
                        let res = self.plan_expr(result)?;
                        Ok(cond.if_else(res, else_expr))
                    },
                )
            }
            SQLExpr::Exists { subquery, negated } => {
                let mut child_planner = self.new_child();
                let subquery = child_planner.plan_query(subquery)?;
                let subquery = Subquery {
                    plan: subquery.build(),
                };
                if *negated {
                    Ok(Expr::Exists(subquery).arced().not())
                } else {
                    Ok(Expr::Exists(subquery).arced())
                }
            }
            SQLExpr::Subquery(subquery) => {
                let mut child_planner = self.new_child();
                let subquery = child_planner.plan_query(subquery)?;
                let subquery = Subquery {
                    plan: subquery.build(),
                };
                Ok(Expr::Subquery(subquery).arced())
            }
            SQLExpr::GroupingSets(_) => unsupported_sql_err!("GROUPING SETS"),
            SQLExpr::Cube(_) => unsupported_sql_err!("CUBE"),
            SQLExpr::Rollup(_) => unsupported_sql_err!("ROLLUP"),
            // Similar to rust and python conventions, tuples always have a fixed size,
            // so we convert them to a fixed size list.
            SQLExpr::Tuple(exprs) => {
                let values = exprs
                    .iter()
                    .map(|expr| {
                        let expr = self.plan_expr(expr)?;
                        // this should always unwrap
                        // there is no way to have multiple references to the same expr at this point
                        let expr = Arc::unwrap_or_clone(expr);
                        match expr {
                            Expr::Literal(lit) => Ok(lit),
                            _ => unsupported_sql_err!("Tuple with non-literal"),
                        }
                    })
                    .collect::<SQLPlannerResult<Vec<_>>>()?;

                let s = literals_to_series(&values)?;
                let s = FixedSizeListArray::new(
                    Field::new("tuple", s.data_type().clone())
                        .to_fixed_size_list_field(exprs.len())?,
                    s,
                    None,
                )
                .into_series();

                Ok(lit(s))
            }
            SQLExpr::Struct { .. } => unsupported_sql_err!("STRUCT"),
            SQLExpr::Named { .. } => unsupported_sql_err!("NAMED"),
            SQLExpr::Dictionary(dict) => {
                let entries = dict
                    .iter()
                    .map(|entry| {
                        let key = entry.key.value.clone();
                        let value = self.plan_expr(&entry.value)?;
                        let value = value.as_literal().ok_or_else(|| {
                            PlannerError::invalid_operation("Dictionary value is not a literal")
                        })?;
                        let struct_field = Field::new(key, value.get_type());
                        Ok((struct_field, value.clone()))
                    })
                    .collect::<SQLPlannerResult<_>>()?;

                Ok(Expr::Literal(LiteralValue::Struct(entries)).arced())
            }
            SQLExpr::Map(_) => unsupported_sql_err!("MAP"),
            SQLExpr::Subscript { expr, subscript } => self.plan_subscript(expr, subscript.as_ref()),
            SQLExpr::Array(array) => {
                if array.elem.is_empty() {
                    invalid_operation_err!("List constructor requires at least one item")
                }
                let items = array
                    .elem
                    .iter()
                    .map(|e| self.plan_expr(e))
                    .collect::<SQLPlannerResult<Vec<_>>>()?;
                Ok(Expr::List(items).into())
            }
            SQLExpr::Interval(interval) => {
                use regex::Regex;

                /// A private struct represents a single parsed interval unit and its value
                #[derive(Debug)]
                struct IntervalPart {
                    count: i64,
                    unit: DateTimeField,
                }

                // Local function to parse interval string to interval parts
                fn parse_interval_string(expr: &str) -> Result<Vec<IntervalPart>, PlannerError> {
                    let expr = expr.trim().trim_matches('\'');

                    let re = Regex::new(r"(-?\d+)\s*(year|years|month|months|day|days|hour|hours|minute|minutes|second|seconds|millisecond|milliseconds|microsecond|microseconds|nanosecond|nanoseconds|week|weeks)")
                        .map_err(|e|PlannerError::invalid_operation(format!("Invalid regex pattern: {}", e)))?;

                    let mut parts = Vec::new();

                    for cap in re.captures_iter(expr) {
                        let count: i64 = cap[1].parse().map_err(|e| {
                            PlannerError::invalid_operation(format!("Invalid interval count: {e}"))
                        })?;

                        let unit = match &cap[2].to_lowercase()[..] {
                            "year" | "years" => DateTimeField::Year,
                            "month" | "months" => DateTimeField::Month,
                            "week" | "weeks" => DateTimeField::Week(None),
                            "day" | "days" => DateTimeField::Day,
                            "hour" | "hours" => DateTimeField::Hour,
                            "minute" | "minutes" => DateTimeField::Minute,
                            "second" | "seconds" => DateTimeField::Second,
                            "millisecond" | "milliseconds" => DateTimeField::Millisecond,
                            "microsecond" | "microseconds" => DateTimeField::Microsecond,
                            "nanosecond" | "nanoseconds" => DateTimeField::Nanosecond,
                            _ => {
                                return Err(PlannerError::invalid_operation(format!(
                                    "Invalid interval unit: {}",
                                    &cap[2]
                                )))
                            }
                        };

                        parts.push(IntervalPart { count, unit });
                    }

                    if parts.is_empty() {
                        return Err(PlannerError::invalid_operation("Invalid interval format."));
                    }

                    Ok(parts)
                }

                // Local function to convert parts to interval values
                fn interval_parts_to_values(parts: Vec<IntervalPart>) -> (i64, i64, i64) {
                    let mut total_months = 0i64;
                    let mut total_days = 0i64;
                    let mut total_nanos = 0i64;

                    for part in parts {
                        match part.unit {
                            DateTimeField::Year => total_months += 12 * part.count,
                            DateTimeField::Month => total_months += part.count,
                            DateTimeField::Week(_) => total_days += 7 * part.count,
                            DateTimeField::Day => total_days += part.count,
                            DateTimeField::Hour => total_nanos += part.count * 3_600_000_000_000,
                            DateTimeField::Minute => total_nanos += part.count * 60_000_000_000,
                            DateTimeField::Second => total_nanos += part.count * 1_000_000_000,
                            DateTimeField::Millisecond | DateTimeField::Milliseconds => {
                                total_nanos += part.count * 1_000_000;
                            }
                            DateTimeField::Microsecond | DateTimeField::Microseconds => {
                                total_nanos += part.count * 1_000;
                            }
                            DateTimeField::Nanosecond | DateTimeField::Nanoseconds => {
                                total_nanos += part.count;
                            }
                            _ => {}
                        }
                    }

                    (total_months, total_days, total_nanos)
                }

                match interval {
                    // If leading_field is specified, treat it as the old style single-unit interval
                    // e.g., INTERVAL '12' YEAR
                    sqlparser::ast::Interval {
                        value,
                        leading_field: Some(time_unit),
                        ..
                    } => {
                        let expr = self.plan_expr(value)?;

                        let expr =
                            expr.as_literal()
                                .and_then(|lit| lit.as_str())
                                .ok_or_else(|| {
                                    PlannerError::invalid_operation(
                                        "Interval value must be a string",
                                    )
                                })?;

                        let count = expr.parse::<i64>().map_err(|e| {
                            PlannerError::unsupported_sql(format!("Invalid interval count: {e}"))
                        })?;

                        let (months, days, nanoseconds) = match time_unit {
                            DateTimeField::Year => (12 * count, 0, 0),
                            DateTimeField::Month => (count, 0, 0),
                            DateTimeField::Week(_) => (0, 7 * count, 0),
                            DateTimeField::Day => (0, count, 0),
                            DateTimeField::Hour => (0, 0, count * 3_600_000_000_000),
                            DateTimeField::Minute => (0, 0, count * 60_000_000_000),
                            DateTimeField::Second => (0, 0, count * 1_000_000_000),
                            DateTimeField::Microsecond | DateTimeField::Microseconds => (0, 0, count * 1_000),
                            DateTimeField::Millisecond | DateTimeField::Milliseconds => (0, 0, count * 1_000_000),
                            DateTimeField::Nanosecond | DateTimeField::Nanoseconds => (0, 0, count),
                            _ => return Err(PlannerError::invalid_operation(format!(
                                "Invalid interval unit: {time_unit}. Expected one of: year, month, week, day, hour, minute, second, millisecond, microsecond, nanosecond"
                            ))),
                        };

                        Ok(Arc::new(Expr::Literal(LiteralValue::Interval(
                            daft_core::datatypes::IntervalValue::new(
                                months as i32,
                                days as i32,
                                nanoseconds,
                            ),
                        ))))
                    }

                    // If no leading_field is specified, treat it as the new style multi-unit interval
                    // e.g., INTERVAL '12 years 3 months 7 days'
                    sqlparser::ast::Interval {
                        value,
                        leading_field: None,
                        ..
                    } => {
                        let expr = self.plan_expr(value)?;

                        let expr =
                            expr.as_literal()
                                .and_then(|lit| lit.as_str())
                                .ok_or_else(|| {
                                    PlannerError::invalid_operation(
                                        "Interval value must be a string",
                                    )
                                })?;

                        let parts = parse_interval_string(expr)
                            .map_err(|e| PlannerError::invalid_operation(e.to_string()))?;

                        let (months, days, nanoseconds) = interval_parts_to_values(parts);

                        Ok(Arc::new(Expr::Literal(LiteralValue::Interval(
                            daft_core::datatypes::IntervalValue::new(
                                months as i32,
                                days as i32,
                                nanoseconds,
                            ),
                        ))))
                    }
                }
            }
            SQLExpr::MatchAgainst { .. } => unsupported_sql_err!("MATCH AGAINST"),
            SQLExpr::Wildcard => unsupported_sql_err!("WILDCARD"),
            SQLExpr::QualifiedWildcard(_) => unsupported_sql_err!("QUALIFIED WILDCARD"),
            SQLExpr::OuterJoin(_) => unsupported_sql_err!("OUTER JOIN"),
            SQLExpr::Prior(_) => unsupported_sql_err!("PRIOR"),
            SQLExpr::Lambda(_) => unsupported_sql_err!("LAMBDA"),
            SQLExpr::JsonAccess { .. } | SQLExpr::MapAccess { .. } => {
                unreachable!("Not reachable in our dialect, should always be parsed as subscript")
            }
        }
    }

    fn sql_operator_to_operator(&self, op: &BinaryOperator) -> SQLPlannerResult<Operator> {
        match op {
            BinaryOperator::Plus => Ok(Operator::Plus),
            BinaryOperator::Minus => Ok(Operator::Minus),
            BinaryOperator::Multiply => Ok(Operator::Multiply),
            BinaryOperator::Divide => Ok(Operator::TrueDivide),
            BinaryOperator::Eq => Ok(Operator::Eq),
            BinaryOperator::Spaceship => Ok(Operator::EqNullSafe),
            BinaryOperator::Modulo => Ok(Operator::Modulus),
            BinaryOperator::Gt => Ok(Operator::Gt),
            BinaryOperator::Lt => Ok(Operator::Lt),
            BinaryOperator::GtEq => Ok(Operator::GtEq),
            BinaryOperator::LtEq => Ok(Operator::LtEq),
            BinaryOperator::NotEq => Ok(Operator::NotEq),
            BinaryOperator::And => Ok(Operator::And),
            BinaryOperator::Or => Ok(Operator::Or),
            BinaryOperator::DuckIntegerDivide => Ok(Operator::FloorDivide),
            other => unsupported_sql_err!("Unsupported operator: '{other}'"),
        }
    }

    /// Visit a SQL unary operator.
    ///
    /// e.g. +column or -column
    fn plan_unary_op(
        &self,
        op: &UnaryOperator,
        expr: &sqlparser::ast::Expr,
    ) -> SQLPlannerResult<ExprRef> {
        let expr = self.plan_expr(expr)?;
        Ok(match (op, expr.as_ref()) {
            // simplify the parse tree by special-casing common unary +/- ops
            (UnaryOperator::Plus, Expr::Literal(LiteralValue::Int64(n))) => lit(*n),
            (UnaryOperator::Plus, Expr::Literal(LiteralValue::Float64(n))) => lit(*n),
            (UnaryOperator::Minus, Expr::Literal(LiteralValue::Int64(n))) => lit(-n),
            (UnaryOperator::Minus, Expr::Literal(LiteralValue::Float64(n))) => lit(-n),
            // general case
            (UnaryOperator::Plus, _) => lit(0).add(expr),
            (UnaryOperator::Minus, _) => lit(0).sub(expr),
            (UnaryOperator::Not, _) => expr.not(),
            other => unsupported_sql_err!("unary operator {:?}", other),
        })
    }
    fn plan_subscript(
        &self,
        expr: &sqlparser::ast::Expr,
        subscript: &Subscript,
    ) -> SQLPlannerResult<ExprRef> {
        match subscript {
            Subscript::Index { index } => {
                let expr = self.plan_expr(expr)?;
                let index = self.plan_expr(index)?;
                let schema = self
                    .current_plan
                    .as_ref()
                    .ok_or_else(|| {
                        PlannerError::invalid_operation("subscript without a current relation")
                    })?
                    .schema();
                let expr_field = expr.to_field(schema.as_ref())?;
                match expr_field.dtype {
                    DataType::List(_) | DataType::FixedSizeList(_, _) => {
                        Ok(daft_functions::list::get(expr, index, null_lit()))
                    }
                    DataType::Struct(_) => {
                        if let Some(s) = index.as_literal().and_then(|l| l.as_str()) {
                            Ok(daft_dsl::functions::struct_::get(expr, s))
                        } else {
                            invalid_operation_err!("Index must be a string literal")
                        }
                    }
                    DataType::Map { .. } => Ok(daft_dsl::functions::map::get(expr, index)),
                    dtype => {
                        invalid_operation_err!("nested access on column with type: {}", dtype)
                    }
                }
            }
            Subscript::Slice {
                lower_bound,
                upper_bound,
                stride,
            } => {
                if stride.is_some() {
                    unsupported_sql_err!("stride cannot be provided when slicing an expression");
                }
                match (lower_bound, upper_bound) {
                    (Some(lower), Some(upper)) => {
                        let lower = self.plan_expr(lower)?;
                        let upper = self.plan_expr(upper)?;
                        let expr = self.plan_expr(expr)?;
                        Ok(daft_functions::list::slice(expr, lower, upper))
                    }
                    _ => {
                        unsupported_sql_err!("slice with only one bound not yet supported");
                    }
                }
            }
        }
    }
}

/// Checks if the SQL query is valid syntax and doesn't use unsupported features.
/// /// This function examines various clauses and options in the provided [sqlparser::ast::Query]
/// and returns an error if any unsupported features are encountered.
fn check_query_features(query: &sqlparser::ast::Query) -> SQLPlannerResult<()> {
    if !query.limit_by.is_empty() {
        unsupported_sql_err!("LIMIT BY");
    }
    if query.offset.is_some() {
        unsupported_sql_err!("OFFSET");
    }
    if query.fetch.is_some() {
        unsupported_sql_err!("FETCH");
    }
    if !query.locks.is_empty() {
        unsupported_sql_err!("LOCKS");
    }
    if let Some(for_clause) = &query.for_clause {
        unsupported_sql_err!("{for_clause}");
    }
    if query.settings.is_some() {
        unsupported_sql_err!("SETTINGS");
    }
    if let Some(format_clause) = &query.format_clause {
        unsupported_sql_err!("{format_clause}");
    }
    Ok(())
}

/// Checks if the features used in the SQL SELECT statement are supported.
///
/// This function examines various clauses and options in the provided [sqlparser::ast::Select]
/// and returns an error if any unsupported features are encountered.
///
/// # Arguments
///
/// * `selection` - A reference to the [sqlparser::ast::Select] to be checked.
///
/// # Returns
///
/// * `SQLPlannerResult<()>` - Ok(()) if all features are supported, or an error describing
///   the first unsupported feature encountered.
fn check_select_features(selection: &sqlparser::ast::Select) -> SQLPlannerResult<()> {
    if selection.top.is_some() {
        unsupported_sql_err!("TOP");
    }

    if selection.into.is_some() {
        unsupported_sql_err!("INTO");
    }
    if !selection.lateral_views.is_empty() {
        unsupported_sql_err!("LATERAL");
    }
    if selection.prewhere.is_some() {
        unsupported_sql_err!("PREWHERE");
    }
    if !selection.cluster_by.is_empty() {
        unsupported_sql_err!("CLUSTER BY");
    }
    if !selection.distribute_by.is_empty() {
        unsupported_sql_err!("DISTRIBUTE BY");
    }
    if !selection.sort_by.is_empty() {
        unsupported_sql_err!("SORT BY");
    }

    if !selection.named_window.is_empty() {
        unsupported_sql_err!("WINDOW");
    }
    if selection.qualify.is_some() {
        unsupported_sql_err!("QUALIFY");
    }
    if selection.connect_by.is_some() {
        unsupported_sql_err!("CONNECT BY");
    }
    Ok(())
}

fn check_wildcard_options(
    WildcardAdditionalOptions {
        opt_ilike,
        opt_except,
        opt_replace,
        opt_rename,
        opt_exclude: _,
    }: &WildcardAdditionalOptions,
) -> SQLPlannerResult<()> {
    if opt_ilike.is_some() {
        unsupported_sql_err!("ILIKE");
    }
    if opt_except.is_some() {
        unsupported_sql_err!("EXCEPT");
    }
    if opt_replace.is_some() {
        unsupported_sql_err!("REPLACE");
    }
    if opt_rename.is_some() {
        unsupported_sql_err!("RENAME");
    }

    Ok(())
}

pub fn sql_schema<S: AsRef<str>>(s: S) -> SQLPlannerResult<SchemaRef> {
    let planner = SQLPlanner::default();

    let tokens = Tokenizer::new(&GenericDialect, s.as_ref()).tokenize()?;

    let mut parser = Parser::new(&GenericDialect)
        .with_options(ParserOptions {
            trailing_commas: true,
            ..Default::default()
        })
        .with_tokens(tokens);

    let column_defs = parser.parse_comma_separated(Parser::parse_column_def)?;

    let fields: Result<Vec<_>, _> = column_defs
        .into_iter()
        .map(|c| planner.column_to_field(&c))
        .collect();

    let fields = fields?;

    let schema = Schema::new(fields)?;
    Ok(Arc::new(schema))
}

pub fn sql_expr<S: AsRef<str>>(s: S) -> SQLPlannerResult<ExprRef> {
    let mut planner = SQLPlanner::default();

    let tokens = Tokenizer::new(&GenericDialect {}, s.as_ref()).tokenize()?;

    let mut parser = Parser::new(&GenericDialect {})
        .with_options(ParserOptions {
            trailing_commas: true,
            ..Default::default()
        })
        .with_tokens(tokens);

    let expr = parser.parse_select_item()?;
    let exprs = planner.select_item_to_expr(&expr)?;
    if exprs.len() != 1 {
        invalid_operation_err!("expected a single expression, found {}", exprs.len())
    }
    Ok(exprs.into_iter().next().unwrap())
}

// ----------------
// Helper functions
// ----------------

/// Join identifiers on "." to form a string.
///
/// Generally, this should only be used for displaying
/// or when there is no better way to represent the compound identifier,
/// since this will include quotes and escapes.
fn compound_ident_to_str(idents: &[Ident]) -> String {
    idents
        .iter()
        .map(<_>::to_string)
        .collect::<Vec<_>>()
        .join(".")
}

/// Returns a normalized daft identifier from an sqlparser ObjectName
pub(crate) fn normalize(name: &ObjectName) -> Identifier {
    // TODO case-normalization of regular identifiers
    let mut names: Vec<String> = name.0.iter().map(|i| i.value.to_string()).collect();
    let name = names.pop().unwrap();
    let namespace = names;
    Identifier::new(namespace, name)
}

/// Returns true iff the ObjectName is a string literal (single-quoted identifier e.g. 'path/to/file.extension').
/// Example:
/// ```text
/// 'file.ext'           -> true
/// 'path/to/file.ext'   -> true
/// 'a'.'b'.'c'          -> false (multiple identifiers)
/// "path/to/file.ext"   -> false (double-quotes)
/// hello                -> false (not single-quoted)
/// ```
fn is_table_path(name: &ObjectName) -> bool {
    if name.0.len() != 1 {
        return false;
    }
    matches!(name.0[0].quote_style, Some('\''))
}

/// Add the relevant projection and alias plan nodes to reflect the TableAlias
fn apply_table_alias(
    mut plan: LogicalPlanBuilder,
    alias: &TableAlias,
) -> SQLPlannerResult<LogicalPlanBuilder> {
    if !alias.columns.is_empty() {
        let columns = plan.schema().names();
        if columns.len() != alias.columns.len() {
            invalid_operation_err!(
                "Column count mismatch: expected {} columns, found {}",
                alias.columns.len(),
                columns.len()
            );
        }

        let projection = columns
            .into_iter()
            .zip(&alias.columns)
            .map(|(name, ident)| unresolved_col(name).alias(ident.value.clone()))
            .collect::<Vec<_>>();

        plan = plan.select(projection)?;
    }

    plan = plan.alias(alias.name.value.clone());

    Ok(plan)
}

/// Helper to do create a singleton plan for SELECT without FROM.
#[cfg(feature = "python")]
fn singleton_plan() -> DaftResult<LogicalPlanBuilder> {
    use daft_logical_plan::PyLogicalPlanBuilder;
    use pyo3::{
        intern,
        prelude::*,
        types::{IntoPyDict, PyList},
    };
    Python::with_gil(|py| {
        // df = DataFrame._from_pydict({"":[""]})
        let df = py
            .import(intern!(py, "daft.dataframe.dataframe"))?
            .getattr(intern!(py, "DataFrame"))?
            .getattr(intern!(py, "_from_pydict"))?
            .call1(([("", PyList::new(py, [""]).unwrap())]
                .into_py_dict(py)
                .unwrap(),))?;
        // builder = df._builder._builder
        let builder: PyLogicalPlanBuilder = df
            .getattr(intern!(py, "_builder"))?
            .getattr(intern!(py, "_builder"))?
            .extract()?;
        // done.
        Ok(builder.builder)
    })
}

/// Helper to do create a singleton plan for SELECT without FROM.
#[cfg(not(feature = "python"))]
fn singleton_plan() -> DaftResult<LogicalPlanBuilder> {
    Err(common_error::DaftError::InternalError(
        "SELECT without FROM requires 'python' feature".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use daft_core::prelude::*;
    use sqlparser::ast::{Ident, ObjectName};

    use crate::{planner::is_table_path, sql_schema};

    #[test]
    fn test_sql_schema_creates_expected_schema() {
        let result =
            sql_schema("Year int, First_Name STRING, County STRING, Sex STRING, Count int")
                .unwrap();

        let expected = Schema::new(vec![
            Field::new("Year", DataType::Int32),
            Field::new("First_Name", DataType::Utf8),
            Field::new("County", DataType::Utf8),
            Field::new("Sex", DataType::Utf8),
            Field::new("Count", DataType::Int32),
        ])
        .unwrap();

        assert_eq!(&*result, &expected);
    }

    #[test]
    fn test_duplicate_column_names_in_schema() {
        // This test checks that sql_schema fails or handles duplicates gracefully.
        // The planner currently returns errors if schema construction fails, so we expect an Err here.
        let result = sql_schema("col1 INT, col1 STRING");

        assert_eq!(
            result.unwrap_err().to_string(),
            "Daft error: DaftError::ValueError Attempting to make a Schema with duplicate field names: col1"
        );
    }

    #[test]
    fn test_degenerate_empty_schema() {
        assert!(sql_schema("").is_err());
    }

    #[test]
    fn test_single_field_schema() {
        let result = sql_schema("col1 INT").unwrap();
        let expected = Schema::new(vec![Field::new("col1", DataType::Int32)]).unwrap();
        assert_eq!(&*result, &expected);
    }

    #[test]
    fn test_is_table_path() {
        // single-quoted path should return true
        assert!(is_table_path(&ObjectName(vec![Ident {
            value: "path/to/file.ext".to_string(),
            quote_style: Some('\'')
        }])));
        // multiple identifiers should return false
        assert!(!is_table_path(&ObjectName(vec![
            Ident::new("a"),
            Ident::new("b")
        ])));
        // double-quoted identifier should return false
        assert!(!is_table_path(&ObjectName(vec![Ident {
            value: "path/to/file.ext".to_string(),
            quote_style: Some('"')
        }])));
        // unquoted identifier should return false
        assert!(!is_table_path(&ObjectName(vec![Ident::new(
            "path/to/file.ext"
        )])));
    }
}
