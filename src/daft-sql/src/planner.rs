use crate::{
    catalog::SQLCatalog,
    column_not_found_err,
    error::{PlannerError, SQLPlannerResult},
    invalid_operation_err, table_not_found_err, unsupported_sql_err,
};
use daft_core::{
    datatypes::{Field, TimeUnit},
    DataType, JoinType,
};
use daft_dsl::{
    col,
    functions::numeric::{ceil, floor},
    lit, null_lit, Expr, ExprRef, LiteralValue, Operator,
};
use daft_plan::{LogicalPlanBuilder, LogicalPlanRef};

use sqlparser::{
    ast::{
        ArrayElemTypeDef, BinaryOperator, CastKind, ExactNumberInfo, GroupByExpr, Ident, Query,
        SelectItem, StructField, Subscript, TableWithJoins, TimezoneInfo, UnaryOperator, Value,
        WildcardAdditionalOptions,
    },
    dialect::GenericDialect,
    parser::{Parser, ParserOptions},
    tokenizer::Tokenizer,
};
/// A named logical plan
/// This is used to keep track of the table name associated with a logical plan while planning a SQL query
pub(crate) struct Relation {
    inner: LogicalPlanBuilder,
    name: String,
}

impl Relation {
    pub fn new(inner: LogicalPlanBuilder, name: String) -> Self {
        Relation { inner, name }
    }
}

pub struct SQLPlanner {
    catalog: SQLCatalog,
}

impl Default for SQLPlanner {
    fn default() -> Self {
        SQLPlanner {
            catalog: SQLCatalog::new(),
        }
    }
}

impl SQLPlanner {
    pub fn new(context: SQLCatalog) -> Self {
        SQLPlanner { catalog: context }
    }

    pub fn plan_sql(&self, sql: &str) -> SQLPlannerResult<LogicalPlanRef> {
        let tokens = Tokenizer::new(&GenericDialect {}, sql).tokenize()?;

        let mut parser = Parser::new(&GenericDialect {})
            .with_options(ParserOptions {
                trailing_commas: true,
                ..Default::default()
            })
            .with_tokens(tokens);

        let statements = parser.parse_statements()?;

        match statements.as_slice() {
            [sqlparser::ast::Statement::Query(query)] => Ok(self.plan_query(query)?.build()),
            other => unsupported_sql_err!("{}", other[0]),
        }
    }

    fn plan_query(&self, query: &Query) -> SQLPlannerResult<LogicalPlanBuilder> {
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

        let selection = query.body.as_select().ok_or_else(|| {
            PlannerError::invalid_operation(format!(
                "Only SELECT queries are supported, got: '{}'",
                query.body
            ))
        })?;

        let mut rel = self.plan_select(selection)?;

        if let Some(order_by) = &query.order_by {
            if order_by.interpolate.is_some() {
                unsupported_sql_err!("ORDER BY [query] [INTERPOLATE]");
            }
            // TODO: if ordering by a column not in the projection, this will fail.
            let (exprs, descending) = self.plan_order_by_exprs(order_by.exprs.as_slice(), &rel)?;
            rel.inner = rel.inner.sort(exprs, descending)?;
        }

        if let Some(limit) = &query.limit {
            let limit = self.plan_expr(limit, &rel)?;
            if let Expr::Literal(LiteralValue::Int64(limit)) = limit.as_ref() {
                rel.inner = rel.inner.limit(*limit, true)?; // TODO: Should this be eager or not?
            } else {
                invalid_operation_err!(
                    "LIMIT <n> must be a constant integer, instead got: {limit}"
                );
            }
        }
        Ok(rel.inner)
    }

    fn plan_order_by_exprs(
        &self,
        expr: &[sqlparser::ast::OrderByExpr],
        rel: &Relation,
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
            let expr = self.plan_expr(&order_by_expr.expr, rel)?;
            desc.push(!order_by_expr.asc.unwrap_or(true));

            exprs.push(expr);
        }
        Ok((exprs, desc))
    }

    fn plan_select(&self, selection: &sqlparser::ast::Select) -> SQLPlannerResult<Relation> {
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

        // FROM/JOIN
        let from = selection.clone().from;
        if from.len() != 1 {
            unsupported_sql_err!("Only exactly one table is supported");
        }
        let mut relation = self.plan_from(&from[0])?;

        // WHERE
        if let Some(selection) = &selection.selection {
            let filter = self.plan_expr(selection, &relation)?;
            relation.inner = relation.inner.filter(filter)?;
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
                    .map(|expr| self.plan_expr(expr, &relation))
                    .collect::<SQLPlannerResult<Vec<_>>>()?;
            }
        }

        let to_select = selection
            .projection
            .iter()
            .map(|expr| self.select_item_to_expr(expr, &relation))
            .collect::<SQLPlannerResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        if !groupby_exprs.is_empty() {
            relation.inner = relation.inner.aggregate(to_select, groupby_exprs)?;
        } else if !to_select.is_empty() {
            relation.inner = relation.inner.select(to_select)?;
        }

        Ok(relation)
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
            use sqlparser::ast::{JoinConstraint, JoinOperator::*};
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
            sqlparser::ast::TableFactor::Table { name, .. } => {
                let table_name = name.to_string();
                let plan = self
                    .catalog
                    .get_table(&table_name)
                    .ok_or_else(|| PlannerError::table_not_found(table_name.clone()))?;
                let plan_builder = LogicalPlanBuilder::new(plan);
                Ok(Relation::new(plan_builder, table_name))
            }
            _ => todo!(),
        }
    }

    fn plan_compound_identifier(
        &self,
        idents: &[Ident],
        current_relation: &Relation,
    ) -> SQLPlannerResult<Vec<ExprRef>> {
        let mut idents = idents.iter();

        let root = idents.next().unwrap();
        let root = ident_to_str(root);

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

    fn select_item_to_expr(
        &self,
        item: &SelectItem,
        current_relation: &Relation,
    ) -> SQLPlannerResult<Vec<ExprRef>> {
        match item {
            SelectItem::ExprWithAlias { expr, alias } => {
                let expr = self.plan_expr(expr, current_relation)?;
                let alias = alias.value.to_string();
                Ok(vec![expr.alias(alias)])
            }
            SelectItem::UnnamedExpr(expr) => {
                self.plan_expr(expr, current_relation).map(|e| vec![e])
            }
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
                    use sqlparser::ast::ExcludeSelectItem::*;
                    return match exclude {
                        Single(item) => current_relation
                            .inner
                            .schema()
                            .exclude(&[&item.to_string()]),
                        Multiple(items) => {
                            let items =
                                items.iter().map(|i| i.to_string()).collect::<Vec<String>>();

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
                    .map_err(|e| e.into());
                }

                Ok(vec![])
            }
            _ => todo!(),
        }
    }

    pub(crate) fn plan_expr(
        &self,
        expr: &sqlparser::ast::Expr,
        current_relation: &Relation,
    ) -> SQLPlannerResult<ExprRef> {
        use sqlparser::ast::Expr as SQLExpr;
        match expr {
            SQLExpr::Identifier(ident) => Ok(col(ident_to_str(ident))),
            SQLExpr::Value(Value::SingleQuotedString(s)) => Ok(lit(s.as_str())),
            SQLExpr::Value(Value::Number(n, _)) => {
                let n = n.parse::<i64>().expect("Failed to parse number");
                Ok(lit(n))
            }
            SQLExpr::Value(Value::Boolean(b)) => Ok(lit(*b)),
            SQLExpr::Value(Value::Null) => Ok(null_lit()),
            SQLExpr::Value(other) => {
                unsupported_sql_err!("literal value '{other}' not yet supported")
            }
            SQLExpr::BinaryOp { left, op, right } => {
                let left = self.plan_expr(left, current_relation)?;
                let right = self.plan_expr(right, current_relation)?;
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
                let expr = self.plan_expr(expr, current_relation)?;
                Ok(expr.cast(&dtype))
            }
            SQLExpr::IsFalse(expr) => Ok(self.plan_expr(expr, current_relation)?.eq(lit(false))),
            SQLExpr::IsNotFalse(_) => {
                Ok(self.plan_expr(expr, current_relation)?.eq(lit(false)).not())
            }
            SQLExpr::IsTrue(expr) => Ok(self.plan_expr(expr, current_relation)?.eq(lit(true))),
            SQLExpr::IsNotTrue(expr) => {
                Ok(self.plan_expr(expr, current_relation)?.eq(lit(true)).not())
            }
            SQLExpr::IsNull(expr) => Ok(self.plan_expr(expr, current_relation)?.is_null()),
            SQLExpr::IsNotNull(expr) => Ok(self.plan_expr(expr, current_relation)?.is_null().not()),
            SQLExpr::UnaryOp { op, expr } => self.plan_unary_op(op, expr, current_relation),
            SQLExpr::CompoundIdentifier(idents) => self
                .plan_compound_identifier(idents.as_slice(), current_relation)
                .map(|e| e[0].clone()),

            SQLExpr::JsonAccess { .. } => {
                unsupported_sql_err!("json access")
            }
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
            SQLExpr::Like { .. } => unsupported_sql_err!("LIKE"),
            SQLExpr::ILike { .. } => unsupported_sql_err!("ILIKE"),
            SQLExpr::SimilarTo { .. } => unsupported_sql_err!("SIMILAR TO"),
            SQLExpr::RLike { .. } => unsupported_sql_err!("RLIKE"),
            SQLExpr::AnyOp { .. } => unsupported_sql_err!("ANY"),
            SQLExpr::AllOp { .. } => unsupported_sql_err!("ALL"),
            SQLExpr::Convert { .. } => unsupported_sql_err!("CONVERT"),
            SQLExpr::Cast { .. } => unsupported_sql_err!("CAST"),
            SQLExpr::AtTimeZone { .. } => unsupported_sql_err!("AT TIME ZONE"),
            SQLExpr::Extract { .. } => unsupported_sql_err!("EXTRACT"),
            SQLExpr::Ceil { expr, .. } => Ok(ceil(self.plan_expr(expr, current_relation)?)),
            SQLExpr::Floor { expr, .. } => Ok(floor(self.plan_expr(expr, current_relation)?)),
            SQLExpr::Position { .. } => unsupported_sql_err!("POSITION"),
            SQLExpr::Substring { .. } => unsupported_sql_err!("SUBSTRING"),
            SQLExpr::Trim { .. } => unsupported_sql_err!("TRIM"),
            SQLExpr::Overlay { .. } => unsupported_sql_err!("OVERLAY"),
            SQLExpr::Collate { .. } => unsupported_sql_err!("COLLATE"),
            SQLExpr::Nested(_) => unsupported_sql_err!("NESTED"),
            SQLExpr::IntroducedString { .. } => unsupported_sql_err!("INTRODUCED STRING"),

            SQLExpr::TypedString { .. } => unsupported_sql_err!("TYPED STRING"),
            SQLExpr::MapAccess { .. } => unsupported_sql_err!("MAP ACCESS"),
            SQLExpr::Function(func) => self.plan_function(func, current_relation),
            SQLExpr::Case { .. } => unsupported_sql_err!("CASE"),
            SQLExpr::Exists { .. } => unsupported_sql_err!("EXISTS"),
            SQLExpr::Subquery(_) => unsupported_sql_err!("SUBQUERY"),
            SQLExpr::GroupingSets(_) => unsupported_sql_err!("GROUPING SETS"),
            SQLExpr::Cube(_) => unsupported_sql_err!("CUBE"),
            SQLExpr::Rollup(_) => unsupported_sql_err!("ROLLUP"),
            SQLExpr::Tuple(_) => unsupported_sql_err!("TUPLE"),
            SQLExpr::Struct { .. } => unsupported_sql_err!("STRUCT"),
            SQLExpr::Named { .. } => unsupported_sql_err!("NAMED"),
            SQLExpr::Dictionary(_) => unsupported_sql_err!("DICTIONARY"),
            SQLExpr::Map(_) => unsupported_sql_err!("MAP"),
            SQLExpr::Subscript { expr, subscript } => match subscript.as_ref() {
                Subscript::Index { index } => {
                    let index = self.plan_expr(index, current_relation)?;
                    let expr = self.plan_expr(expr, current_relation)?;
                    Ok(daft_dsl::functions::list::get(expr, index, null_lit()))
                }
                Subscript::Slice {
                    lower_bound,
                    upper_bound,
                    stride,
                } => {
                    if stride.is_some() {
                        unsupported_sql_err!("stride");
                    }
                    match (lower_bound, upper_bound) {
                        (Some(lower), Some(upper)) => {
                            let lower = self.plan_expr(lower, current_relation)?;
                            let upper = self.plan_expr(upper, current_relation)?;
                            let expr = self.plan_expr(expr, current_relation)?;
                            Ok(daft_dsl::functions::list::slice(expr, lower, upper))
                        }
                        _ => {
                            unsupported_sql_err!("slice with only one bound not yet supported");
                        }
                    }
                }
            },
            SQLExpr::Array(_) => unsupported_sql_err!("ARRAY"),
            SQLExpr::Interval(_) => unsupported_sql_err!("INTERVAL"),
            SQLExpr::MatchAgainst { .. } => unsupported_sql_err!("MATCH AGAINST"),
            SQLExpr::Wildcard => unsupported_sql_err!("WILDCARD"),
            SQLExpr::QualifiedWildcard(_) => unsupported_sql_err!("QUALIFIED WILDCARD"),
            SQLExpr::OuterJoin(_) => unsupported_sql_err!("OUTER JOIN"),
            SQLExpr::Prior(_) => unsupported_sql_err!("PRIOR"),
            SQLExpr::Lambda(_) => unsupported_sql_err!("LAMBDA"),
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
            SQLDataType::Array(ArrayElemTypeDef::AngleBracket(inner_type))
            | SQLDataType::Array(ArrayElemTypeDef::SquareBracket(inner_type, None)) => {
                DataType::List(Box::new(self.sql_dtype_to_dtype(inner_type)?))
            }
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
            SQLDataType::Struct(fields) => {
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
                                None => format!("col_{}", idx),
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
        current_relation: &Relation,
    ) -> SQLPlannerResult<ExprRef> {
        let expr = self.plan_expr(expr, current_relation)?;
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
}

fn ident_to_str(ident: &Ident) -> String {
    if let Some('"') = ident.quote_style {
        ident.value.to_string()
    } else {
        ident.to_string()
    }
}
