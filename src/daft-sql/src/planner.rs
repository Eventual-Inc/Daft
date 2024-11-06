use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    col,
    functions::utf8::{ilike, like, to_date, to_datetime},
    has_agg, lit, literals_to_series, null_lit, Expr, ExprRef, LiteralValue, Operator,
};
use daft_functions::numeric::{ceil::ceil, floor::floor};
use daft_plan::{LogicalPlanBuilder, LogicalPlanRef};
use sqlparser::{
    ast::{
        ArrayElemTypeDef, BinaryOperator, CastKind, DateTimeField, Distinct, ExactNumberInfo,
        ExcludeSelectItem, GroupByExpr, Ident, Query, SelectItem, SetExpr, Statement, StructField,
        Subscript, TableAlias, TableWithJoins, TimezoneInfo, UnaryOperator, Value,
        WildcardAdditionalOptions, With,
    },
    dialect::GenericDialect,
    parser::{Parser, ParserOptions},
    tokenizer::Tokenizer,
};

use crate::{
    catalog::SQLCatalog, column_not_found_err, error::*, invalid_operation_err,
    table_not_found_err, unsupported_sql_err,
};

/// A named logical plan
/// This is used to keep track of the table name associated with a logical plan while planning a SQL query
#[derive(Debug, Clone)]
pub struct Relation {
    pub(crate) inner: LogicalPlanBuilder,
    pub(crate) name: String,
    pub(crate) alias: Option<TableAlias>,
}

impl Relation {
    pub fn new(inner: LogicalPlanBuilder, name: String) -> Self {
        Self {
            inner,
            name,
            alias: None,
        }
    }
    pub fn with_alias(self, alias: TableAlias) -> Self {
        Self {
            alias: Some(alias),
            ..self
        }
    }
    pub fn get_name(&self) -> String {
        self.alias
            .as_ref()
            .map(|alias| ident_to_str(&alias.name))
            .unwrap_or_else(|| self.name.clone())
    }
    pub(crate) fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

pub struct SQLPlanner {
    catalog: SQLCatalog,
    current_relation: Option<Relation>,
    table_map: HashMap<String, Relation>,
    cte_map: HashMap<String, Relation>,
}

impl Default for SQLPlanner {
    fn default() -> Self {
        Self {
            catalog: SQLCatalog::new(),
            current_relation: Default::default(),
            table_map: Default::default(),
            cte_map: Default::default(),
        }
    }
}

impl SQLPlanner {
    pub fn new(context: SQLCatalog) -> Self {
        Self {
            catalog: context,
            ..Default::default()
        }
    }

    /// SAFETY: it is up to the caller to ensure that the relation is set before calling this method.
    /// It's a programming error to call this method without setting the relation first.
    /// Some methods such as `plan_expr` do not require the relation to be set.
    fn relation_mut(&mut self) -> &mut Relation {
        self.current_relation.as_mut().expect("relation not set")
    }

    pub(crate) fn relation_opt(&self) -> Option<&Relation> {
        self.current_relation.as_ref()
    }

    /// Clears the current context used for planning a SQL query
    fn clear_context(&mut self) {
        self.current_relation = None;
        self.table_map.clear();
        self.cte_map.clear();
    }

    fn get_table_from_current_scope(&self, name: &str) -> Option<Relation> {
        let table = self.table_map.get(name).cloned();
        table
            .or_else(|| self.cte_map.get(name).cloned())
            .or_else(|| {
                self.catalog
                    .get_table(name)
                    .map(|table| Relation::new(table.into(), name.to_string()))
            })
    }

    fn register_cte(
        &mut self,
        mut rel: Relation,
        column_aliases: &[Ident],
    ) -> SQLPlannerResult<()> {
        if !column_aliases.is_empty() {
            let schema = rel.schema();
            let columns = schema.names();
            if columns.len() != column_aliases.len() {
                invalid_operation_err!(
                    "Column count mismatch: expected {} columns, found {}",
                    column_aliases.len(),
                    columns.len()
                );
            }

            let projection = columns
                .into_iter()
                .zip(column_aliases)
                .map(|(name, alias)| col(name).alias(ident_to_str(alias)))
                .collect::<Vec<_>>();

            rel.inner = rel.inner.select(projection)?;
        }
        self.cte_map.insert(rel.get_name(), rel);
        Ok(())
    }

    fn plan_ctes(&mut self, with: &With) -> SQLPlannerResult<()> {
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

            let name = ident_to_str(&cte.alias.name);
            let plan = self.plan_query(&cte.query)?;
            let rel = Relation::new(plan, name);

            self.register_cte(rel, cte.alias.columns.as_slice())?;
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

        let statements = parser.parse_statements()?;

        let plan = match statements.len() {
            1 => Ok(self.plan_statement(&statements[0])?),
            other => {
                unsupported_sql_err!("Only exactly one SQL statement allowed, found {}", other)
            }
        };
        self.clear_context();
        plan
    }

    fn plan_statement(&mut self, statement: &Statement) -> SQLPlannerResult<LogicalPlanRef> {
        match statement {
            Statement::Query(query) => Ok(self.plan_query(query)?.build()),
            other => unsupported_sql_err!("{}", other),
        }
    }

    fn plan_query(&mut self, query: &Query) -> SQLPlannerResult<LogicalPlanBuilder> {
        check_query_features(query)?;

        let selection = match query.body.as_ref() {
            SetExpr::Select(selection) => selection,
            SetExpr::Query(_) => unsupported_sql_err!("Subqueries are not supported"),
            SetExpr::SetOperation { .. } => {
                unsupported_sql_err!("Set operations are not supported")
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
        let rel = self.plan_from(&from)?;
        let schema = rel.schema();
        self.current_relation = Some(rel);

        // WHERE
        if let Some(selection) = &selection.selection {
            let filter = self.plan_expr(selection)?;
            let rel = self.relation_mut();
            rel.inner = rel.inner.filter(filter)?;
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
        let mut projections = Vec::with_capacity(selection.projection.len());
        let mut projection_fields = Vec::with_capacity(selection.projection.len());
        for expr in &selection.projection {
            let exprs = self.select_item_to_expr(expr, &schema)?;

            let fields = exprs
                .iter()
                .map(|expr| expr.to_field(&schema).map_err(PlannerError::from))
                .collect::<SQLPlannerResult<Vec<_>>>()?;

            projections.extend(exprs);

            projection_fields.extend(fields);
        }

        let projection_schema = Schema::new(projection_fields)?;
        let has_orderby = query.order_by.is_some();
        let has_aggs = projections.iter().any(has_agg);

        if has_aggs {
            self.plan_aggregate_query(
                &projections,
                &schema,
                has_orderby,
                groupby_exprs,
                query,
                &projection_schema,
            )?;
        } else {
            self.plan_non_agg_query(projections, schema, has_orderby, query, projection_schema)?;
        }

        match &selection.distinct {
            Some(Distinct::Distinct) => {
                let rel = self.relation_mut();
                rel.inner = rel.inner.distinct()?;
            }
            Some(Distinct::On(_)) => unsupported_sql_err!("DISTINCT ON"),
            None => {}
        }

        if let Some(limit) = &query.limit {
            let limit = self.plan_expr(limit)?;
            if let Expr::Literal(LiteralValue::Int64(limit)) = limit.as_ref() {
                let rel = self.relation_mut();
                rel.inner = rel.inner.limit(*limit, true)?; // TODO: Should this be eager or not?
            } else {
                invalid_operation_err!(
                    "LIMIT <n> must be a constant integer, instead got: {limit}"
                );
            }
        }

        Ok(self.current_relation.clone().unwrap().inner)
    }

    fn plan_non_agg_query(
        &mut self,
        projections: Vec<Arc<Expr>>,
        schema: Arc<Schema>,
        has_orderby: bool,
        query: &Query,
        projection_schema: Schema,
    ) -> Result<(), PlannerError> {
        // Final/selected cols
        // if there is an orderby, and it references a column that is not part of the final projection (such as an alias)
        // then we need to keep the original column in the projection, and remove it at the end
        // ex: `SELECT a as b, c FROM t ORDER BY a`
        // we need to keep a, and c in the first projection
        // but only c in the final projection
        // We only will apply 2 projections if there is an order by and the order by references a column that is not in the final projection
        let mut final_projection = Vec::with_capacity(projections.len());
        let mut orderby_projection = Vec::with_capacity(projections.len());
        for p in &projections {
            let fld = p.to_field(&schema);

            let fld = fld?;
            let name = fld.name.clone();

            // if there is an orderby, then the final projection will only contain the columns that are in the orderby
            final_projection.push(if has_orderby {
                col(name.as_ref())
            } else {
                // otherwise we just do a normal projection
                p.clone()
            });
        }

        if has_orderby {
            let order_by = query.order_by.as_ref().unwrap();

            if order_by.interpolate.is_some() {
                unsupported_sql_err!("ORDER BY [query] [INTERPOLATE]");
            };

            let (orderby_exprs, orderby_desc) =
                self.plan_order_by_exprs(order_by.exprs.as_slice())?;

            for expr in &orderby_exprs {
                if let Err(DaftError::FieldNotFound(_)) = expr.to_field(&projection_schema) {
                    // this is likely an alias
                    orderby_projection.push(expr.clone());
                }
            }
            // if the orderby references a column that is not in the final projection
            // then we need an additional projection
            let needs_projection = !orderby_projection.is_empty();

            let rel = self.relation_mut();
            if needs_projection {
                let pre_orderby_projections = projections
                    .iter()
                    .cloned()
                    .chain(orderby_projection)
                    .collect::<HashSet<_>>() // dedup
                    .into_iter()
                    .collect::<Vec<_>>();
                rel.inner = rel.inner.select(pre_orderby_projections)?;
            } else {
                rel.inner = rel.inner.select(projections)?;
            }

            rel.inner = rel.inner.sort(orderby_exprs, orderby_desc)?;

            if needs_projection {
                rel.inner = rel.inner.select(final_projection)?;
            }
        } else {
            let rel = self.relation_mut();
            rel.inner = rel.inner.select(projections)?;
        }

        Ok(())
    }

    fn plan_aggregate_query(
        &mut self,
        projections: &Vec<Arc<Expr>>,
        schema: &Arc<Schema>,
        has_orderby: bool,
        groupby_exprs: Vec<Arc<Expr>>,
        query: &Query,
        projection_schema: &Schema,
    ) -> Result<(), PlannerError> {
        let mut final_projection = Vec::with_capacity(projections.len());
        let mut orderby_projection = Vec::with_capacity(projections.len());
        let mut aggs = Vec::with_capacity(projections.len());
        let mut orderby_exprs = None;
        let mut orderby_desc = None;
        for p in projections {
            let fld = p.to_field(schema)?;

            let name = fld.name.clone();
            if has_agg(p) {
                // this is an aggregate, so it is resolved during `.agg`. So we just push the column name
                final_projection.push(col(name.as_ref()));
                // add it to the aggs list
                aggs.push(p.clone());
            } else {
                // otherwise we just do a normal projection
                final_projection.push(p.clone());
            }
        }
        let groupby_exprs = groupby_exprs
            .into_iter()
            .map(|e| {
                // instead of trying to do an additional projection for the groupby column, we just map it back to the original (unaliased) column
                // ex: SELECT a as b FROM t GROUP BY b
                // in this case, we need to resolve b to a
                if let Err(DaftError::FieldNotFound(_)) = e.to_field(schema) {
                    // this is likely an alias
                    unresolve_alias(e, &final_projection)
                } else {
                    Ok(e)
                }
            })
            .collect::<SQLPlannerResult<Vec<_>>>()?;

        if has_orderby {
            let order_by = query.order_by.as_ref().unwrap();

            if order_by.interpolate.is_some() {
                unsupported_sql_err!("ORDER BY [query] [INTERPOLATE]");
            };

            let (exprs, desc) = self.plan_order_by_exprs(order_by.exprs.as_slice())?;

            orderby_exprs = Some(exprs.clone());
            orderby_desc = Some(desc);

            for expr in &exprs {
                // if the orderby references a column that is not in the final projection
                // then we need an additional projection
                if let Err(DaftError::FieldNotFound(_)) = expr.to_field(projection_schema) {
                    orderby_projection.push(expr.clone());
                }
            }
        }

        let rel = self.relation_mut();
        rel.inner = rel.inner.aggregate(aggs, groupby_exprs)?;

        let needs_projection = !orderby_projection.is_empty();
        if needs_projection {
            let orderby_projection = rel
                .schema()
                .names()
                .iter()
                .map(|n| col(n.as_str()))
                .chain(orderby_projection)
                .collect::<HashSet<_>>() // dedup
                .into_iter()
                .collect::<Vec<_>>();

            rel.inner = rel.inner.select(orderby_projection)?;
        }

        // these are orderbys that are part of the final projection
        let mut orderbys_after_projection = Vec::new();
        let mut orderbys_after_projection_desc = Vec::new();

        // these are orderbys that are not part of the final projection
        let mut orderbys_before_projection = Vec::new();
        let mut orderbys_before_projection_desc = Vec::new();

        if let Some(orderby_exprs) = orderby_exprs {
            // this needs to be done after the aggregation and any projections
            // because the orderby may reference an alias, or an intermediate column that is not in the final projection
            let schema = rel.schema();
            for (i, expr) in orderby_exprs.iter().enumerate() {
                if let Err(DaftError::FieldNotFound(_)) = expr.to_field(&schema) {
                    orderbys_after_projection.push(expr.clone());
                    let desc = orderby_desc.clone().map(|o| o[i]).unwrap();
                    orderbys_after_projection_desc.push(desc);
                } else {
                    let desc = orderby_desc.clone().map(|o| o[i]).unwrap();

                    orderbys_before_projection.push(expr.clone());
                    orderbys_before_projection_desc.push(desc);
                }
            }
        }

        let has_orderby_before_projection = !orderbys_before_projection.is_empty();
        let has_orderby_after_projection = !orderbys_after_projection.is_empty();

        // PERF(cory): if there are order bys from both parts, can we combine them into a single sort instead of two?
        // or can we optimize them into a single sort?

        // order bys that are not in the final projection
        if has_orderby_before_projection {
            rel.inner = rel
                .inner
                .sort(orderbys_before_projection, orderbys_before_projection_desc)?;
        }

        rel.inner = rel.inner.select(final_projection)?;

        // order bys that are in the final projection
        if has_orderby_after_projection {
            rel.inner = rel
                .inner
                .sort(orderbys_after_projection, orderbys_after_projection_desc)?;
        }
        Ok(())
    }

    fn plan_order_by_exprs(
        &self,
        expr: &[sqlparser::ast::OrderByExpr],
    ) -> SQLPlannerResult<(Vec<ExprRef>, Vec<bool>)> {
        if expr.is_empty() {
            unsupported_sql_err!("ORDER BY []");
        }
        let mut exprs = Vec::with_capacity(expr.len());
        let mut desc = Vec::with_capacity(expr.len());
        for order_by_expr in expr {
            if order_by_expr.nulls_first.is_some() {
                unsupported_sql_err!("NULLS FIRST");
            }
            if order_by_expr.with_fill.is_some() {
                unsupported_sql_err!("WITH FILL");
            }
            let expr = self.plan_expr(&order_by_expr.expr)?;
            desc.push(!order_by_expr.asc.unwrap_or(true));

            exprs.push(expr);
        }
        Ok((exprs, desc))
    }

    fn plan_from(&mut self, from: &[TableWithJoins]) -> SQLPlannerResult<Relation> {
        if from.len() > 1 {
            // todo!("cross join")
            let mut from_iter = from.iter();

            let first = from_iter.next().unwrap();
            let mut rel = self.plan_relation(&first.relation)?;
            self.table_map.insert(rel.get_name(), rel.clone());
            for tbl in from_iter {
                let right = self.plan_relation(&tbl.relation)?;
                self.table_map.insert(right.get_name(), right.clone());
                let right_join_prefix = Some(format!("{}.", right.get_name()));

                rel.inner = rel.inner.join(
                    right.inner,
                    vec![],
                    vec![],
                    JoinType::Inner,
                    None,
                    None,
                    right_join_prefix.as_deref(),
                )?;
            }
            return Ok(rel);
        }

        let from = from.iter().next().unwrap();

        fn collect_compound_identifiers(
            left: &[Ident],
            right: &[Ident],
            left_rel: &Relation,
            right_rel: &Relation,
        ) -> SQLPlannerResult<(Vec<ExprRef>, Vec<ExprRef>)> {
            if left.len() == 2 && right.len() == 2 {
                let (tbl_a, col_a) = (&left[0].value, &left[1].value);
                let (tbl_b, col_b) = (&right[0].value, &right[1].value);

                // switch left/right operands if the caller has them in reverse
                if &left_rel.get_name() == tbl_b || &right_rel.get_name() == tbl_a {
                    Ok((vec![col(col_b.as_ref())], vec![col(col_a.as_ref())]))
                } else {
                    Ok((vec![col(col_a.as_ref())], vec![col(col_b.as_ref())]))
                }
            } else {
                unsupported_sql_err!("collect_compound_identifiers: Expected left.len() == 2 && right.len() == 2, but found left.len() == {:?}, right.len() == {:?}", left.len(), right.len());
            }
        }

        fn process_join_on(
            expression: &sqlparser::ast::Expr,
            left_rel: &Relation,
            right_rel: &Relation,
        ) -> SQLPlannerResult<(Vec<ExprRef>, Vec<ExprRef>, Vec<bool>)> {
            if let sqlparser::ast::Expr::BinaryOp { left, op, right } = expression {
                match *op {
                    BinaryOperator::Eq | BinaryOperator::Spaceship => {
                        if let (
                            sqlparser::ast::Expr::CompoundIdentifier(left),
                            sqlparser::ast::Expr::CompoundIdentifier(right),
                        ) = (left.as_ref(), right.as_ref())
                        {
                            let null_equals_null = *op == BinaryOperator::Spaceship;
                            collect_compound_identifiers(left, right, left_rel, right_rel)
                                .map(|(left, right)| (left, right, vec![null_equals_null]))
                        } else {
                            unsupported_sql_err!("JOIN clauses support '='/'<=>' constraints on identifiers; found lhs={:?}, rhs={:?}", left, right);
                        }
                    }
                    BinaryOperator::And => {
                        let (mut left_i, mut right_i, mut null_equals_nulls_i) =
                            process_join_on(left, left_rel, right_rel)?;
                        let (mut left_j, mut right_j, mut null_equals_nulls_j) =
                            process_join_on(left, left_rel, right_rel)?;
                        left_i.append(&mut left_j);
                        right_i.append(&mut right_j);
                        null_equals_nulls_i.append(&mut null_equals_nulls_j);
                        Ok((left_i, right_i, null_equals_nulls_i))
                    }
                    _ => {
                        unsupported_sql_err!("JOIN clauses support '=' constraints combined with 'AND'; found op = '{:?}'", op);
                    }
                }
            } else if let sqlparser::ast::Expr::Nested(expr) = expression {
                process_join_on(expr, left_rel, right_rel)
            } else {
                unsupported_sql_err!("JOIN clauses support '=' constraints combined with 'AND'; found expression = {:?}", expression);
            }
        }

        let relation = from.relation.clone();
        let mut left_rel = self.plan_relation(&relation)?;
        self.table_map.insert(left_rel.get_name(), left_rel.clone());

        for join in &from.joins {
            use sqlparser::ast::{
                JoinConstraint,
                JoinOperator::{
                    AsOf, CrossApply, CrossJoin, FullOuter, Inner, LeftAnti, LeftOuter, LeftSemi,
                    OuterApply, RightAnti, RightOuter, RightSemi,
                },
            };
            let right_rel = self.plan_relation(&join.relation)?;
            self.table_map
                .insert(right_rel.get_name(), right_rel.clone());
            let right_rel_name = right_rel.get_name();
            let right_join_prefix = Some(format!("{right_rel_name}."));

            match &join.join_operator {
                Inner(JoinConstraint::On(expr)) => {
                    let (left_on, right_on, null_equals_nulls) =
                        process_join_on(expr, &left_rel, &right_rel)?;

                    left_rel.inner = left_rel.inner.join_with_null_safe_equal(
                        right_rel.inner,
                        left_on,
                        right_on,
                        Some(null_equals_nulls),
                        JoinType::Inner,
                        None,
                        None,
                        right_join_prefix.as_deref(),
                    )?;
                }
                Inner(JoinConstraint::Using(idents)) => {
                    let on = idents
                        .iter()
                        .map(|i| col(i.value.clone()))
                        .collect::<Vec<_>>();

                    left_rel.inner = left_rel.inner.join(
                        right_rel.inner,
                        on.clone(),
                        on,
                        JoinType::Inner,
                        None,
                        None,
                        right_join_prefix.as_deref(),
                    )?;
                }
                LeftOuter(JoinConstraint::On(expr)) => {
                    let (left_on, right_on, null_equals_nulls) =
                        process_join_on(expr, &left_rel, &right_rel)?;

                    left_rel.inner = left_rel.inner.join_with_null_safe_equal(
                        right_rel.inner,
                        left_on,
                        right_on,
                        Some(null_equals_nulls),
                        JoinType::Left,
                        None,
                        None,
                        right_join_prefix.as_deref(),
                    )?;
                }
                RightOuter(JoinConstraint::On(expr)) => {
                    let (left_on, right_on, null_equals_nulls) =
                        process_join_on(expr, &left_rel, &right_rel)?;

                    left_rel.inner = left_rel.inner.join_with_null_safe_equal(
                        right_rel.inner,
                        left_on,
                        right_on,
                        Some(null_equals_nulls),
                        JoinType::Right,
                        None,
                        None,
                        right_join_prefix.as_deref(),
                    )?;
                }

                FullOuter(JoinConstraint::On(expr)) => {
                    let (left_on, right_on, null_equals_nulls) =
                        process_join_on(expr, &left_rel, &right_rel)?;

                    left_rel.inner = left_rel.inner.join_with_null_safe_equal(
                        right_rel.inner,
                        left_on,
                        right_on,
                        Some(null_equals_nulls),
                        JoinType::Outer,
                        None,
                        None,
                        right_join_prefix.as_deref(),
                    )?;
                }
                CrossJoin => unsupported_sql_err!("CROSS JOIN"),
                LeftSemi(_) => unsupported_sql_err!("LEFT SEMI JOIN"),
                RightSemi(_) => unsupported_sql_err!("RIGHT SEMI JOIN"),
                LeftAnti(_) => unsupported_sql_err!("LEFT ANTI JOIN"),
                RightAnti(_) => unsupported_sql_err!("RIGHT ANTI JOIN"),
                CrossApply => unsupported_sql_err!("CROSS APPLY"),
                OuterApply => unsupported_sql_err!("OUTER APPLY"),
                AsOf { .. } => unsupported_sql_err!("AS OF"),
                join_type => unsupported_sql_err!("join type: {join_type:?}"),
            };
        }

        Ok(left_rel)
    }

    fn plan_relation(&mut self, rel: &sqlparser::ast::TableFactor) -> SQLPlannerResult<Relation> {
        let (rel, alias) = match rel {
            sqlparser::ast::TableFactor::Table {
                name,
                args: Some(args),
                alias,
                ..
            } => {
                let tbl_fn = name.0.first().unwrap().value.as_str();
                (self.plan_table_function(tbl_fn, args)?, alias.clone())
            }
            sqlparser::ast::TableFactor::Table {
                name,
                args: None,
                alias,
                ..
            } => {
                let table_name = name.to_string();
                let Some(rel) = self.get_table_from_current_scope(&table_name) else {
                    table_not_found_err!(table_name)
                };
                (rel, alias.clone())
            }
            sqlparser::ast::TableFactor::Derived {
                lateral,
                subquery,
                alias: Some(alias),
            } => {
                if *lateral {
                    unsupported_sql_err!("LATERAL");
                }
                let subquery = self.plan_query(subquery)?;
                let rel_name = ident_to_str(&alias.name);
                let rel = Relation::new(subquery, rel_name);

                (rel, Some(alias.clone()))
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
            _ => unsupported_sql_err!("Unsupported table factor"),
        };
        if let Some(alias) = alias {
            Ok(rel.with_alias(alias))
        } else {
            Ok(rel)
        }
    }

    fn plan_compound_identifier(&self, idents: &[Ident]) -> SQLPlannerResult<Vec<ExprRef>> {
        let ident_str = idents_to_str(idents);
        let mut idents = idents.iter();

        let root = idents.next().unwrap();
        let root = ident_to_str(root);
        let current_relation = match self.get_table_from_current_scope(&root) {
            Some(rel) => rel,
            None => {
                return Err(PlannerError::TableNotFound {
                    message: "Expected table".to_string(),
                })
            }
        };

        if root == current_relation.get_name() {
            // This happens when it's called from a qualified wildcard (tbl.*)
            if idents.len() == 0 {
                return Ok(current_relation
                    .inner
                    .schema()
                    .fields
                    .keys()
                    .map(|f| col(f.clone()))
                    .collect());
            }

            // If duplicate columns are present in the schema, it adds the table name as a prefix. (df.column_name)
            // So we first check if the prefixed column name is present in the schema.
            let current_schema = self.relation_opt().unwrap().inner.schema();

            let f = current_schema.get_field(&ident_str).ok();
            if let Some(field) = f {
                Ok(vec![col(field.name.clone())])
            // If it's not, we also need to check if the column name is present in the current schema.
            // This is to handle aliased tables. (df1 as a join df2 as b where b.column_name)
            } else if let Some(next_ident) = idents.next() {
                let column_name = ident_to_str(next_ident);

                let f = current_schema.get_field(&column_name).ok();
                if let Some(field) = f {
                    Ok(vec![col(field.name.clone())])
                } else {
                    column_not_found_err!(&column_name, &current_relation.get_name());
                }
            } else {
                column_not_found_err!(&ident_str, &current_relation.get_name());
            }
        } else {
            table_not_found_err!(root);
        }
    }

    fn select_item_to_expr(
        &self,
        item: &SelectItem,
        schema: &Schema,
    ) -> SQLPlannerResult<Vec<ExprRef>> {
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
                Ok(vec![expr.alias(alias)])
            }
            SelectItem::UnnamedExpr(expr) => self.plan_expr(expr).map(|e| vec![e]),

            SelectItem::Wildcard(wildcard_opts) => {
                check_wildcard_options(wildcard_opts)?;

                if let Some(exclude) = &wildcard_opts.opt_exclude {
                    let current_relation = match &self.current_relation {
                        Some(rel) => rel,
                        None => {
                            return Err(PlannerError::TableNotFound {
                                message: "No table found to exclude columns from".to_string(),
                            })
                        }
                    };
                    let schema = current_relation.inner.schema();
                    wildcard_exclude(schema, exclude)
                        .map(|schema| {
                            schema
                                .names()
                                .iter()
                                .map(|n| col(n.as_ref()))
                                .collect::<Vec<_>>()
                        })
                        .map_err(std::convert::Into::into)
                } else if schema.is_empty() {
                    Ok(vec![col("*")])
                } else {
                    Ok(schema
                        .names()
                        .iter()
                        .map(|n| col(n.as_ref()))
                        .collect::<Vec<_>>())
                }
            }
            SelectItem::QualifiedWildcard(object_name, wildcard_opts) => {
                check_wildcard_options(wildcard_opts)?;
                let table_name = idents_to_str(&object_name.0);
                let Some(rel) = self.relation_opt() else {
                    table_not_found_err!(table_name);
                };
                let Some(table_rel) = self.get_table_from_current_scope(&table_name) else {
                    table_not_found_err!(table_name);
                };
                let right_schema = table_rel.inner.schema();
                let schema = rel.inner.schema();
                let keys = schema.fields.keys();

                let right_schema = if let Some(exclude) = &wildcard_opts.opt_exclude {
                    Arc::new(wildcard_exclude(right_schema, exclude)?)
                } else {
                    right_schema
                };
                let columns = right_schema
                    .fields
                    .keys()
                    .map(|field| {
                        if keys
                            .clone()
                            .any(|s| s.starts_with(&table_name) && s.ends_with(field))
                        {
                            col(format!("{}.{}", table_name, field)).alias(field.as_ref())
                        } else {
                            col(field.clone())
                        }
                    })
                    .collect::<Vec<_>>();
                Ok(columns)
            }
        }
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

    fn plan_lit(&self, expr: &sqlparser::ast::Expr) -> SQLPlannerResult<LiteralValue> {
        if let sqlparser::ast::Expr::Value(v) = expr {
            self.value_to_lit(v)
        } else {
            invalid_operation_err!("Only string, number, boolean and null literals are supported. Instead found: `{expr}`");
        }
    }
    pub(crate) fn plan_expr(&self, expr: &sqlparser::ast::Expr) -> SQLPlannerResult<ExprRef> {
        use sqlparser::ast::Expr as SQLExpr;
        match expr {
            SQLExpr::Identifier(ident) => Ok(col(ident_to_str(ident))),
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
                let dtype = self.sql_dtype_to_dtype(data_type)?;
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
            SQLExpr::CompoundIdentifier(idents) => self
                .plan_compound_identifier(idents.as_slice())
                .map(|e| e[0].clone()),

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
                    .map(|e| self.plan_lit(e))
                    .collect::<SQLPlannerResult<Vec<_>>>()?;
                // We should really have a better way to use `is_in` instead of all of this extra wrapping of the values
                let series = literals_to_series(&list)?;
                let series_lit = LiteralValue::Series(series);
                let series_expr = Expr::Literal(series_lit);
                let series_expr_arc = Arc::new(series_expr);
                let expr = expr.is_in(series_expr_arc);
                if *negated {
                    Ok(expr.not())
                } else {
                    Ok(expr)
                }
            }
            SQLExpr::InSubquery { .. } => {
                unsupported_sql_err!("IN subquery")
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
            SQLExpr::Extract { .. } => unsupported_sql_err!("EXTRACT"),
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

                Ok(daft_dsl::functions::utf8::substr(expr, start, length))
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
            SQLExpr::Exists { .. } => unsupported_sql_err!("EXISTS"),
            SQLExpr::Subquery(_) => unsupported_sql_err!("SUBQUERY"),
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
            SQLExpr::Array(_) => unsupported_sql_err!("ARRAY"),
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
    fn sql_dtype_to_dtype(&self, dtype: &sqlparser::ast::DataType) -> SQLPlannerResult<DataType> {
        use sqlparser::ast::DataType as SQLDataType;

        Ok(match dtype {
            // ---------------------------------
            // array/list
            // ---------------------------------
            SQLDataType::Array(
                ArrayElemTypeDef::AngleBracket(inner_type)
                | ArrayElemTypeDef::SquareBracket(inner_type, None),
            ) => DataType::List(Box::new(self.sql_dtype_to_dtype(inner_type)?)),
            SQLDataType::Array(ArrayElemTypeDef::SquareBracket(inner_type, Some(size))) => {
                DataType::FixedSizeList(
                    Box::new(self.sql_dtype_to_dtype(inner_type)?),
                    *size as usize,
                )
            }

            // ---------------------------------
            // binary
            // ---------------------------------
            SQLDataType::Bytea
            | SQLDataType::Bytes(_)
            | SQLDataType::Binary(_)
            | SQLDataType::Blob(_)
            | SQLDataType::Varbinary(_) => DataType::Binary,

            // ---------------------------------
            // boolean
            // ---------------------------------
            SQLDataType::Boolean | SQLDataType::Bool => DataType::Boolean,
            // ---------------------------------
            // signed integer
            // ---------------------------------
            SQLDataType::Int(_) | SQLDataType::Integer(_) => DataType::Int32,
            SQLDataType::Int2(_) | SQLDataType::SmallInt(_) => DataType::Int16,
            SQLDataType::Int4(_) | SQLDataType::MediumInt(_) => DataType::Int32,
            SQLDataType::Int8(_) | SQLDataType::BigInt(_) => DataType::Int64,
            SQLDataType::TinyInt(_) => DataType::Int8,
            // ---------------------------------
            // unsigned integer
            // ---------------------------------
            SQLDataType::UnsignedInt(_) | SQLDataType::UnsignedInteger(_) => DataType::UInt32,
            SQLDataType::UnsignedInt2(_) | SQLDataType::UnsignedSmallInt(_) => DataType::UInt16,
            SQLDataType::UnsignedInt4(_) | SQLDataType::UnsignedMediumInt(_) => DataType::UInt32,
            SQLDataType::UnsignedInt8(_) | SQLDataType::UnsignedBigInt(_) => DataType::UInt64,
            SQLDataType::UnsignedTinyInt(_) => DataType::UInt8,
            // ---------------------------------
            // float
            // ---------------------------------
            SQLDataType::Double | SQLDataType::DoublePrecision | SQLDataType::Float8 => {
                DataType::Float64
            }
            SQLDataType::Float(n_bytes) => match n_bytes {
                Some(n) if (1u64..=24u64).contains(n) => DataType::Float32,
                Some(n) if (25u64..=53u64).contains(n) => DataType::Float64,
                Some(n) => {
                    unsupported_sql_err!(
                        "unsupported `float` size (expected a value between 1 and 53, found {})",
                        n
                    )
                }
                None => DataType::Float64,
            },
            SQLDataType::Float4 | SQLDataType::Real => DataType::Float32,

            // ---------------------------------
            // decimal
            // ---------------------------------
            SQLDataType::Dec(info) | SQLDataType::Decimal(info) | SQLDataType::Numeric(info) => {
                match *info {
                    ExactNumberInfo::PrecisionAndScale(p, s) => {
                        DataType::Decimal128(p as usize, s as usize)
                    }
                    ExactNumberInfo::Precision(p) => DataType::Decimal128(p as usize, 0),
                    ExactNumberInfo::None => DataType::Decimal128(38, 9),
                }
            }
            // ---------------------------------
            // temporal
            // ---------------------------------
            SQLDataType::Date => DataType::Date,
            SQLDataType::Interval => DataType::Duration(TimeUnit::Microseconds),
            SQLDataType::Time(precision, tz) => match tz {
                TimezoneInfo::None => DataType::Time(self.timeunit_from_precision(precision)?),
                _ => unsupported_sql_err!("`time` with timezone is; found tz={}", tz),
            },
            SQLDataType::Datetime(_) => unsupported_sql_err!("`datetime` is not supported"),
            SQLDataType::Timestamp(prec, tz) => match tz {
                TimezoneInfo::None => {
                    DataType::Timestamp(self.timeunit_from_precision(prec)?, None)
                }
                _ => unsupported_sql_err!("`timestamp` with timezone"),
            },
            // ---------------------------------
            // string
            // ---------------------------------
            SQLDataType::Char(_)
            | SQLDataType::CharVarying(_)
            | SQLDataType::Character(_)
            | SQLDataType::CharacterVarying(_)
            | SQLDataType::Clob(_)
            | SQLDataType::String(_)
            | SQLDataType::Text
            | SQLDataType::Uuid
            | SQLDataType::Varchar(_) => DataType::Utf8,
            // ---------------------------------
            // struct
            // ---------------------------------
            SQLDataType::Struct(fields, _) => {
                let fields = fields
                    .iter()
                    .enumerate()
                    .map(
                        |(
                            idx,
                            StructField {
                                field_name,
                                field_type,
                            },
                        )| {
                            let dtype = self.sql_dtype_to_dtype(field_type)?;
                            let name = match field_name {
                                Some(name) => name.to_string(),
                                None => format!("col_{idx}"),
                            };

                            Ok(Field::new(name, dtype))
                        },
                    )
                    .collect::<SQLPlannerResult<Vec<_>>>()?;
                DataType::Struct(fields)
            }
            other => unsupported_sql_err!("data type: {:?}", other),
        })
    }

    fn timeunit_from_precision(&self, prec: &Option<u64>) -> SQLPlannerResult<TimeUnit> {
        Ok(match prec {
            None => TimeUnit::Microseconds,
            Some(n) if (1u64..=3u64).contains(n) => TimeUnit::Milliseconds,
            Some(n) if (4u64..=6u64).contains(n) => TimeUnit::Microseconds,
            Some(n) if (7u64..=9u64).contains(n) => TimeUnit::Nanoseconds,
            Some(n) => {
                unsupported_sql_err!(
                    "invalid temporal type precision (expected 1-9, found {})",
                    n
                )
            }
        })
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
                    .current_relation
                    .as_ref()
                    .ok_or_else(|| {
                        PlannerError::invalid_operation("subscript without a current relation")
                    })
                    .map(Relation::schema)?;
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
    if selection.having.is_some() {
        unsupported_sql_err!("HAVING");
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
pub fn sql_expr<S: AsRef<str>>(s: S) -> SQLPlannerResult<ExprRef> {
    let planner = SQLPlanner::default();

    let tokens = Tokenizer::new(&GenericDialect {}, s.as_ref()).tokenize()?;

    let mut parser = Parser::new(&GenericDialect {})
        .with_options(ParserOptions {
            trailing_commas: true,
            ..Default::default()
        })
        .with_tokens(tokens);

    let expr = parser.parse_select_item()?;
    let empty_schema = Schema::empty();
    let exprs = planner.select_item_to_expr(&expr, &empty_schema)?;
    if exprs.len() != 1 {
        invalid_operation_err!("expected a single expression, found {}", exprs.len())
    }
    Ok(exprs.into_iter().next().unwrap())
}

fn ident_to_str(ident: &Ident) -> String {
    if ident.quote_style == Some('"') {
        ident.value.to_string()
    } else {
        ident.to_string()
    }
}
fn idents_to_str(idents: &[Ident]) -> String {
    idents
        .iter()
        .map(ident_to_str)
        .collect::<Vec<_>>()
        .join(".")
}

/// unresolves an alias in a projection
/// Example:
/// ```sql
/// SELECT a as b, c FROM t group by b
/// ```
/// in this case if you tried to unresolve the expr `b` using the projections [`a as b`, `c`] you would get `a`
///
/// Since sql allows you to use the alias in the group by or the order by clause, we need to unresolve the alias to the original expression
/// ex:
/// All of the following are valid sql queries
/// `select a as b, c from t group by b`
/// `select a as b, c from t group by a`
/// `select a as b, c from t group by a order by a`
/// `select a as b, c from t group by a order by b`
/// `select a as b, c from t group by b order by a`
/// `select a as b, c from t group by b order by b`
///
/// In all of the above cases, the group by and order by clauses are resolved to the original expression `a`
///
/// This is needed for resolving group by and order by clauses
fn unresolve_alias(expr: ExprRef, projection: &[ExprRef]) -> SQLPlannerResult<ExprRef> {
    projection
        .iter()
        .find_map(|p| {
            if let Expr::Alias(e, alias) = &p.as_ref() {
                if expr.name() == alias.as_ref() {
                    Some(e.clone())
                } else {
                    None
                }
            } else {
                None
            }
        })
        .ok_or_else(|| PlannerError::column_not_found(expr.name(), "projection"))
}
