use std::sync::Arc;

use common_error::DaftResult;
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
        ArrayElemTypeDef, BinaryOperator, CastKind, ExactNumberInfo, GroupByExpr, Ident, Query,
        SelectItem, Statement, StructField, Subscript, TableWithJoins, TimezoneInfo, UnaryOperator,
        Value, WildcardAdditionalOptions,
    },
    dialect::GenericDialect,
    parser::{Parser, ParserOptions},
    tokenizer::Tokenizer,
};

use crate::{
    catalog::SQLCatalog,
    column_not_found_err,
    error::{PlannerError, SQLPlannerResult},
    invalid_operation_err, table_not_found_err, unsupported_sql_err,
};
/// A named logical plan
/// This is used to keep track of the table name associated with a logical plan while planning a SQL query
#[derive(Debug, Clone)]
pub struct Relation {
    pub(crate) inner: LogicalPlanBuilder,
    pub(crate) name: String,
}

impl Relation {
    pub fn new(inner: LogicalPlanBuilder, name: String) -> Self {
        Self { inner, name }
    }
    pub(crate) fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

pub struct SQLPlanner {
    catalog: SQLCatalog,
    current_relation: Option<Relation>,
}

impl Default for SQLPlanner {
    fn default() -> Self {
        Self {
            catalog: SQLCatalog::new(),
            current_relation: None,
        }
    }
}

impl SQLPlanner {
    pub fn new(context: SQLCatalog) -> Self {
        Self {
            catalog: context,
            current_relation: None,
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

    pub fn plan_sql(&mut self, sql: &str) -> SQLPlannerResult<LogicalPlanRef> {
        let tokens = Tokenizer::new(&GenericDialect {}, sql).tokenize()?;

        let mut parser = Parser::new(&GenericDialect {})
            .with_options(ParserOptions {
                trailing_commas: true,
                ..Default::default()
            })
            .with_tokens(tokens);

        let statements = parser.parse_statements()?;

        match statements.len() {
            1 => Ok(self.plan_statement(&statements[0])?),
            other => {
                unsupported_sql_err!("Only exactly one SQL statement allowed, found {}", other)
            }
        }
    }

    fn plan_statement(&mut self, statement: &Statement) -> SQLPlannerResult<LogicalPlanRef> {
        match statement {
            Statement::Query(query) => Ok(self.plan_query(query)?.build()),
            other => unsupported_sql_err!("{}", other),
        }
    }

    fn plan_query(&mut self, query: &Query) -> SQLPlannerResult<LogicalPlanBuilder> {
        check_query_features(query)?;

        let selection = query.body.as_select().ok_or_else(|| {
            PlannerError::invalid_operation(format!(
                "Only SELECT queries are supported, got: '{}'",
                query.body
            ))
        })?;

        check_select_features(selection)?;

        // FROM/JOIN
        let from = selection.clone().from;
        if from.len() != 1 {
            unsupported_sql_err!("Only exactly one table is supported");
        }

        self.current_relation = Some(self.plan_from(&from[0])?);

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

        // split the selection into the groupby expressions and the rest
        let (groupby_selection, to_select) = selection
            .projection
            .iter()
            .map(|expr| self.select_item_to_expr(expr))
            .collect::<SQLPlannerResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .partition::<Vec<_>, _>(|expr| {
                groupby_exprs
                    .iter()
                    .any(|e| expr.input_mapping() == e.input_mapping())
            });

        if !groupby_exprs.is_empty() {
            let rel = self.relation_mut();
            rel.inner = rel.inner.aggregate(to_select, groupby_exprs.clone())?;
        } else if !to_select.is_empty() {
            let rel = self.relation_mut();
            let has_aggs = to_select.iter().any(has_agg);
            if has_aggs {
                rel.inner = rel.inner.aggregate(to_select, vec![])?;
            } else {
                rel.inner = rel.inner.select(to_select)?;
            }
        }

        if let Some(order_by) = &query.order_by {
            if order_by.interpolate.is_some() {
                unsupported_sql_err!("ORDER BY [query] [INTERPOLATE]");
            }
            // TODO: if ordering by a column not in the projection, this will fail.
            let (exprs, descending) = self.plan_order_by_exprs(order_by.exprs.as_slice())?;
            let rel = self.relation_mut();
            rel.inner = rel.inner.sort(exprs, descending)?;
        }

        // Properly apply or remove the groupby columns from the selection
        // This needs to be done after the orderby
        // otherwise, the orderby will not be able to reference the grouping columns
        //
        // ex: SELECT sum(a) as sum_a, max(a) as max_a, b as c FROM table GROUP BY b
        //
        // The groupby columns are [b]
        // the evaluation of sum(a) and max(a) are already handled by the earlier aggregate,
        // so our projection is [sum_a, max_a, (b as c)]
        // leaving us to handle (b as c)
        //
        // we filter for the columns in the schema that are not in the groupby keys,
        // [sum_a, max_a, b] -> [sum_a, max_a]
        //
        // Then we add the groupby columns back in with the correct expressions
        // this gives us the final projection: [sum_a, max_a, (b as c)]
        if !groupby_exprs.is_empty() {
            let rel = self.relation_mut();
            let schema = rel.inner.schema();

            let groupby_keys = groupby_exprs
                .iter()
                .map(|e| Ok(e.to_field(&schema)?.name))
                .collect::<DaftResult<Vec<_>>>()?;

            let selection_colums = schema
                .exclude(groupby_keys.as_ref())?
                .names()
                .iter()
                .map(|n| col(n.as_str()))
                .chain(groupby_selection)
                .collect();

            rel.inner = rel.inner.select(selection_colums)?;
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

    fn plan_order_by_exprs(
        &self,
        expr: &[sqlparser::ast::OrderByExpr],
    ) -> SQLPlannerResult<(Vec<ExprRef>, Vec<bool>)> {
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

    fn plan_from(&self, from: &TableWithJoins) -> SQLPlannerResult<Relation> {
        fn collect_compound_identifiers(
            left: &[Ident],
            right: &[Ident],
            left_name: &str,
            right_name: &str,
        ) -> SQLPlannerResult<(Vec<ExprRef>, Vec<ExprRef>)> {
            if left.len() == 2 && right.len() == 2 {
                let (tbl_a, col_a) = (&left[0].value, &left[1].value);
                let (tbl_b, col_b) = (&right[0].value, &right[1].value);

                // switch left/right operands if the caller has them in reverse
                if left_name == tbl_b || right_name == tbl_a {
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
            left_name: &str,
            right_name: &str,
        ) -> SQLPlannerResult<(Vec<ExprRef>, Vec<ExprRef>)> {
            if let sqlparser::ast::Expr::BinaryOp { left, op, right } = expression {
                match *op {
                    BinaryOperator::Eq => {
                        if let (
                            sqlparser::ast::Expr::CompoundIdentifier(left),
                            sqlparser::ast::Expr::CompoundIdentifier(right),
                        ) = (left.as_ref(), right.as_ref())
                        {
                            collect_compound_identifiers(left, right, left_name, right_name)
                        } else {
                            unsupported_sql_err!("JOIN clauses support '=' constraints on identifiers; found lhs={:?}, rhs={:?}", left, right);
                        }
                    }
                    BinaryOperator::And => {
                        let (mut left_i, mut right_i) =
                            process_join_on(left, left_name, right_name)?;
                        let (mut left_j, mut right_j) =
                            process_join_on(right, left_name, right_name)?;
                        left_i.append(&mut left_j);
                        right_i.append(&mut right_j);
                        Ok((left_i, right_i))
                    }
                    _ => {
                        unsupported_sql_err!("JOIN clauses support '=' constraints combined with 'AND'; found op = '{:?}'", op);
                    }
                }
            } else if let sqlparser::ast::Expr::Nested(expr) = expression {
                process_join_on(expr, left_name, right_name)
            } else {
                unsupported_sql_err!("JOIN clauses support '=' constraints combined with 'AND'; found expression = {:?}", expression);
            }
        }

        let relation = from.relation.clone();
        let mut left_rel = self.plan_relation(&relation)?;

        for join in &from.joins {
            use sqlparser::ast::{
                JoinConstraint,
                JoinOperator::{
                    AsOf, CrossApply, CrossJoin, FullOuter, Inner, LeftAnti, LeftOuter, LeftSemi,
                    OuterApply, RightAnti, RightOuter, RightSemi,
                },
            };
            let Relation {
                inner: right_plan,
                name: right_name,
            } = self.plan_relation(&join.relation)?;

            match &join.join_operator {
                Inner(JoinConstraint::On(expr)) => {
                    let (left_on, right_on) = process_join_on(expr, &left_rel.name, &right_name)?;

                    left_rel.inner = left_rel.inner.join(
                        right_plan,
                        left_on,
                        right_on,
                        JoinType::Inner,
                        None,
                    )?;
                }
                Inner(JoinConstraint::Using(idents)) => {
                    let on = idents
                        .iter()
                        .map(|i| col(i.value.clone()))
                        .collect::<Vec<_>>();

                    left_rel.inner =
                        left_rel
                            .inner
                            .join(right_plan, on.clone(), on, JoinType::Inner, None)?;
                }
                LeftOuter(JoinConstraint::On(expr)) => {
                    let (left_on, right_on) = process_join_on(expr, &left_rel.name, &right_name)?;

                    left_rel.inner =
                        left_rel
                            .inner
                            .join(right_plan, left_on, right_on, JoinType::Left, None)?;
                }
                RightOuter(JoinConstraint::On(expr)) => {
                    let (left_on, right_on) = process_join_on(expr, &left_rel.name, &right_name)?;

                    left_rel.inner = left_rel.inner.join(
                        right_plan,
                        left_on,
                        right_on,
                        JoinType::Right,
                        None,
                    )?;
                }

                FullOuter(JoinConstraint::On(expr)) => {
                    let (left_on, right_on) = process_join_on(expr, &left_rel.name, &right_name)?;

                    left_rel.inner = left_rel.inner.join(
                        right_plan,
                        left_on,
                        right_on,
                        JoinType::Outer,
                        None,
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

    fn plan_relation(&self, rel: &sqlparser::ast::TableFactor) -> SQLPlannerResult<Relation> {
        match rel {
            sqlparser::ast::TableFactor::Table {
                name,
                args: Some(args),
                alias,
                ..
            } => {
                let tbl_fn = name.0.first().unwrap().value.as_str();

                self.plan_table_function(tbl_fn, args, alias)
            }
            sqlparser::ast::TableFactor::Table {
                name, args: None, ..
            } => {
                let table_name = name.to_string();
                let plan = self
                    .catalog
                    .get_table(&table_name)
                    .ok_or_else(|| PlannerError::table_not_found(table_name.clone()))?;
                let plan_builder = LogicalPlanBuilder::new(plan, None);
                Ok(Relation::new(plan_builder, table_name))
            }
            _ => todo!(),
        }
    }

    fn plan_compound_identifier(&self, idents: &[Ident]) -> SQLPlannerResult<Vec<ExprRef>> {
        let mut idents = idents.iter();

        let root = idents.next().unwrap();
        let root = ident_to_str(root);
        let current_relation = match &self.current_relation {
            Some(rel) => rel,
            None => {
                return Err(PlannerError::TableNotFound {
                    message: "Expected table".to_string(),
                })
            }
        };
        if root == current_relation.name {
            let column = idents.next().unwrap();
            let column_name = ident_to_str(column);
            let current_schema = current_relation.inner.schema();
            let f = current_schema.get_field(&column_name).ok();
            if let Some(field) = f {
                Ok(vec![col(field.name.clone())])
            } else {
                column_not_found_err!(&column_name, &current_relation.name);
            }
        } else {
            table_not_found_err!(root);
        }
    }

    fn select_item_to_expr(&self, item: &SelectItem) -> SQLPlannerResult<Vec<ExprRef>> {
        match item {
            SelectItem::ExprWithAlias { expr, alias } => {
                let expr = self.plan_expr(expr)?;
                let alias = alias.value.to_string();
                Ok(vec![expr.alias(alias)])
            }
            SelectItem::UnnamedExpr(expr) => self.plan_expr(expr).map(|e| vec![e]),
            SelectItem::Wildcard(WildcardAdditionalOptions {
                opt_ilike,
                opt_exclude,
                opt_except,
                opt_replace,
                opt_rename,
            }) => {
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

                if let Some(exclude) = opt_exclude {
                    let current_relation = match &self.current_relation {
                        Some(rel) => rel,
                        None => {
                            return Err(PlannerError::TableNotFound {
                                message: "No table found to exclude columns from".to_string(),
                            })
                        }
                    };

                    use sqlparser::ast::ExcludeSelectItem::{Multiple, Single};
                    match exclude {
                        Single(item) => current_relation
                            .inner
                            .schema()
                            .exclude(&[&item.to_string()]),

                        Multiple(items) => {
                            let items = items
                                .iter()
                                .map(std::string::ToString::to_string)
                                .collect::<Vec<String>>();

                            current_relation.inner.schema().exclude(items.as_slice())
                        }
                    }
                    .map(|schema| {
                        schema
                            .names()
                            .iter()
                            .map(|n| col(n.as_ref()))
                            .collect::<Vec<_>>()
                    })
                    .map_err(std::convert::Into::into)
                } else {
                    Ok(vec![col("*")])
                }
            }
            _ => todo!(),
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
                    "Only string, number, boolean and null literals are supported",
                ))
            }
        })
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
            SQLExpr::InList { .. } => unsupported_sql_err!("IN LIST"),
            SQLExpr::InSubquery { .. } => {
                unsupported_sql_err!("IN subquery")
            }
            SQLExpr::InUnnest { .. } => unsupported_sql_err!("IN UNNEST"),
            SQLExpr::Between { .. } => unsupported_sql_err!("BETWEEN"),
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
            SQLExpr::Interval(_) => unsupported_sql_err!("INTERVAL"),
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
    if let Some(with) = &query.with {
        unsupported_sql_err!("WITH: {with}")
    }
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
    if selection.distinct.is_some() {
        unsupported_sql_err!("DISTINCT");
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
    let exprs = planner.select_item_to_expr(&expr)?;
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
