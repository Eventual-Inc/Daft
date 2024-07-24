use std::collections::HashMap;

use common_error::{DaftError, DaftResult};
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
        ArrayElemTypeDef, BinaryOperator, CastKind, ExactNumberInfo, Function, FunctionArg,
        FunctionArgExpr, GroupByExpr, Ident, Query, SelectItem, StructField, Subscript,
        TableWithJoins, TimezoneInfo, UnaryOperator, Value, WildcardAdditionalOptions,
    },
    dialect::GenericDialect,
    parser::{Parser, ParserOptions},
    tokenizer::Tokenizer,
};

macro_rules! sql_bail {
    ($($arg:tt)*) => {
        return Err(DaftError::ValueError(format!($($arg)*)))
    }
}

#[derive(Debug, Default)]
pub struct SQLContext {
    tables: HashMap<String, LogicalPlanRef>,
}

/// A named logical plan
struct Relation {
    inner: LogicalPlanBuilder,
    name: String,
}

impl Relation {
    pub fn new(inner: LogicalPlanBuilder, name: String) -> Self {
        Relation { inner, name }
    }
}

impl SQLContext {
    pub fn new() -> Self {
        Default::default()
    }
    pub fn get_table(&self, name: &str) -> DaftResult<LogicalPlanRef> {
        self.tables
            .get(name)
            .cloned()
            .ok_or(DaftError::ComputeError(format!("Table {} not found", name)))
    }
    pub fn register_table(&mut self, name: &str, table: LogicalPlanRef) {
        self.tables.insert(name.to_string(), table);
    }
}

pub struct SQLPlanner {
    context: SQLContext,
}

impl Default for SQLPlanner {
    fn default() -> Self {
        SQLPlanner {
            context: SQLContext::new(),
        }
    }
}

impl SQLPlanner {
    pub fn new(context: SQLContext) -> Self {
        SQLPlanner { context }
    }

    pub fn plan_sql(&self, sql: &str) -> DaftResult<LogicalPlanRef> {
        let tokens = Tokenizer::new(&GenericDialect {}, sql)
            .tokenize()
            .map_err(|e| DaftError::ValueError(format!("failed to tokenize sql, reason: {}", e)))?;

        let mut parser = Parser::new(&GenericDialect {})
            .with_options(ParserOptions {
                trailing_commas: true,
                ..Default::default()
            })
            .with_tokens(tokens);

        let statements = parser
            .parse_statements()
            .map_err(|e| DaftError::ValueError(format!("failed to parse sql, reason: {}", e)))?;

        match statements.as_slice() {
            [sqlparser::ast::Statement::Query(query)] => Ok(self.plan_query(query)?.build()),
            other => sql_bail!("Unsupported SQL: '{}'", other[0]),
        }
    }

    fn plan_query(&self, query: &Query) -> DaftResult<LogicalPlanBuilder> {
        let selection = query.body.as_select().expect("Expected SELECT statement");
        self.plan_select(selection)
    }

    fn plan_select(&self, selection: &sqlparser::ast::Select) -> DaftResult<LogicalPlanBuilder> {
        if selection.top.is_some() {
            sql_bail!("TOP not (yet) supported");
        }
        if selection.distinct.is_some() {
            sql_bail!("DISTINCT not (yet) supported");
        }
        if selection.into.is_some() {
            sql_bail!("INTO not (yet) supported");
        }
        if !selection.lateral_views.is_empty() {
            sql_bail!("LATERAL not (yet) supported");
        }
        if selection.prewhere.is_some() {
            sql_bail!("PREWHERE not supported");
        }
        if !selection.cluster_by.is_empty() {
            sql_bail!("CLUSTER BY not (yet) supported");
        }
        if !selection.distribute_by.is_empty() {
            sql_bail!("DISTRIBUTE BY not (yet) supported");
        }
        if !selection.sort_by.is_empty() {
            sql_bail!("SORT BY not (yet) supported");
        }
        if selection.having.is_some() {
            sql_bail!("HAVING not (yet) supported");
        }
        if !selection.named_window.is_empty() {
            sql_bail!("WINDOW not (yet) supported");
        }
        if selection.qualify.is_some() {
            sql_bail!("QUALIFY not (yet) supported");
        }
        if selection.connect_by.is_some() {
            sql_bail!("CONNECT BY not (yet) supported");
        }

        match &selection.group_by {
            GroupByExpr::All(s) => {
                if !s.is_empty() {
                    sql_bail!("GROUP BY not supported");
                }
            }
            GroupByExpr::Expressions(expressions, _) => {
                if !expressions.is_empty() {
                    sql_bail!("GROUP BY not supported");
                }
            }
        }

        let from = selection.clone().from;
        if from.len() != 1 {
            return Err(DaftError::ComputeError(
                "Only exactly one table is supported".to_string(),
            ));
        }
        let mut builder = self.plan_from(&from[0])?;

        if let Some(selection) = &selection.selection {
            let filter = self.plan_expr(selection, &builder)?;
            builder.inner = builder.inner.filter(filter)?;
        }

        let to_select = selection
            .projection
            .iter()
            .map(|expr| self.select_item_to_expr(expr, &builder))
            .collect::<DaftResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        if !to_select.is_empty() {
            builder.inner = builder.inner.select(to_select)?;
        }

        Ok(builder.inner)
    }

    fn plan_from(&self, from: &TableWithJoins) -> DaftResult<Relation> {
        fn collect_compound_identifiers(
            left: &[Ident],
            right: &[Ident],
            left_name: &str,
            right_name: &str,
        ) -> DaftResult<(Vec<ExprRef>, Vec<ExprRef>)> {
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
                sql_bail!("collect_compound_identifiers: Expected left.len() == 2 && right.len() == 2, but found left.len() == {:?}, right.len() == {:?}", left.len(), right.len());
            }
        }
        fn process_join_on(
            expression: &sqlparser::ast::Expr,
            left_name: &str,
            right_name: &str,
        ) -> DaftResult<(Vec<ExprRef>, Vec<ExprRef>)> {
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
                            sql_bail!("JOIN clauses support '=' constraints on identifiers; found lhs={:?}, rhs={:?}", left, right);
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
                        sql_bail!("JOIN clauses support '=' constraints combined with 'AND'; found op = '{:?}'", op);
                    }
                }
            } else if let sqlparser::ast::Expr::Nested(expr) = expression {
                process_join_on(expr, left_name, right_name)
            } else {
                sql_bail!("JOIN clauses support '=' constraints combined with 'AND'; found expression = {:?}", expression);
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
                CrossJoin => sql_bail!("CROSS JOIN not supported"),
                LeftSemi(_) => sql_bail!("LEFT SEMI JOIN not supported"),
                RightSemi(_) => {
                    sql_bail!("RIGHT SEMI JOIN not supported")
                }
                LeftAnti(_) => sql_bail!("LEFT ANTI JOIN not supported"),
                RightAnti(_) => {
                    sql_bail!("RIGHT ANTI JOIN not supported")
                }
                CrossApply => sql_bail!("CROSS APPLY not supported"),
                OuterApply => sql_bail!("OUTER APPLY not supported"),
                AsOf { .. } => sql_bail!("AS OF not supported"),
                _ => {
                    sql_bail!("Unsupported join type");
                }
            };
        }

        Ok(left_rel)
    }

    fn plan_relation(&self, rel: &sqlparser::ast::TableFactor) -> DaftResult<Relation> {
        match rel {
            sqlparser::ast::TableFactor::Table { name, .. } => {
                let table_name = name.to_string();
                let plan = self.context.get_table(&table_name)?;
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
    ) -> DaftResult<Vec<ExprRef>> {
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
                sql_bail!(
                    "Column '{}' not found in relation '{}'",
                    column_name,
                    current_relation.name
                );
            }
        } else {
            sql_bail!("table: '{}' not found", root);
        }
    }

    fn select_item_to_expr(
        &self,
        item: &SelectItem,
        current_relation: &Relation,
    ) -> DaftResult<Vec<ExprRef>> {
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
                    sql_bail!("ILIKE not yet supported");
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
                    });
                }
                if opt_exclude.is_some() {
                    sql_bail!("EXCLUDE not yet supported");
                }
                if opt_except.is_some() {
                    sql_bail!("EXCEPT not yet supported");
                }
                if opt_replace.is_some() {
                    sql_bail!("REPLACE not yet supported");
                }
                if opt_rename.is_some() {
                    sql_bail!("RENAME not yet supported");
                }
                Ok(vec![])
            }
            _ => todo!(),
        }
    }

    fn plan_expr(
        &self,
        expr: &sqlparser::ast::Expr,
        current_relation: &Relation,
    ) -> DaftResult<ExprRef> {
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
            SQLExpr::Value(other) => todo!("literal value {other} not yet supported"),
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

            SQLExpr::JsonAccess { .. } => todo!("json access not yet supported"),
            SQLExpr::CompositeAccess { .. } => todo!("composite access not yet supported"),
            SQLExpr::IsUnknown(_) => todo!("IS UNKNOWN not yet supported"),
            SQLExpr::IsNotUnknown(_) => todo!("IS NOT UNKNOWN not yet supported"),
            SQLExpr::IsDistinctFrom(_, _) => todo!("IS DISTINCT FROM not yet supported"),
            SQLExpr::IsNotDistinctFrom(_, _) => todo!("IS NOT DISTINCT FROM not yet supported"),
            SQLExpr::InList { .. } => todo!("IN list not yet supported"),
            SQLExpr::InSubquery { .. } => todo!("IN subquery not yet supported"),
            SQLExpr::InUnnest { .. } => todo!("IN unnest not yet supported"),
            SQLExpr::Between { .. } => todo!("BETWEEN not yet supported"),
            SQLExpr::Like { .. } => todo!("LIKE not yet supported"),
            SQLExpr::ILike { .. } => todo!("ILIKE not yet supported"),
            SQLExpr::SimilarTo { .. } => todo!("SIMILAR TO not yet supported"),
            SQLExpr::RLike { .. } => todo!("RLIKE not yet supported"),
            SQLExpr::AnyOp { .. } => todo!("ANY not yet supported"),
            SQLExpr::AllOp { .. } => todo!("ALL not yet supported"),
            SQLExpr::Convert { .. } => todo!("CONVERT not yet supported"),
            SQLExpr::Cast { .. } => todo!("CAST not yet supported"),
            SQLExpr::AtTimeZone { .. } => todo!("AT TIME ZONE not yet supported"),
            SQLExpr::Extract { .. } => todo!("EXTRACT not yet supported"),
            SQLExpr::Ceil { expr, .. } => Ok(ceil(self.plan_expr(expr, current_relation)?)),
            SQLExpr::Floor { expr, .. } => Ok(floor(self.plan_expr(expr, current_relation)?)),
            SQLExpr::Position { .. } => todo!("POSITION not yet supported"),
            SQLExpr::Substring { .. } => todo!("SUBSTRING not yet supported"),
            SQLExpr::Trim { .. } => todo!("TRIM not yet supported"),
            SQLExpr::Overlay { .. } => todo!("OVERLAY not yet supported"),
            SQLExpr::Collate { .. } => todo!("COLLATE not yet supported"),
            SQLExpr::Nested(_) => todo!("NESTED not yet supported"),
            SQLExpr::IntroducedString { .. } => todo!("INTRODUCED STRING not yet supported"),
            SQLExpr::TypedString { .. } => todo!("TYPED STRING not yet supported"),
            SQLExpr::MapAccess { .. } => todo!("MAP ACCESS not yet supported"),
            SQLExpr::Function(func) => self.plan_function(func, current_relation),
            SQLExpr::Case { .. } => todo!("CASE not yet supported"),
            SQLExpr::Exists { .. } => todo!("EXISTS not yet supported"),
            SQLExpr::Subquery(_) => todo!("SUBQUERY not yet supported"),
            SQLExpr::GroupingSets(_) => todo!("GROUPING SETS not yet supported"),
            SQLExpr::Cube(_) => todo!("CUBE not yet supported"),
            SQLExpr::Rollup(_) => todo!("ROLLUP not yet supported"),
            SQLExpr::Tuple(_) => todo!("TUPLE not yet supported"),
            SQLExpr::Struct { .. } => todo!("STRUCT not yet supported"),
            SQLExpr::Named { .. } => todo!("NAMED not yet supported"),
            SQLExpr::Dictionary(_) => todo!("DICTIONARY not yet supported"),
            SQLExpr::Map(_) => todo!("MAP not yet supported"),
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
                        sql_bail!("stride not yet supported");
                    }
                    match (lower_bound, upper_bound) {
                        (Some(lower), Some(upper)) => {
                            let lower = self.plan_expr(lower, current_relation)?;
                            let upper = self.plan_expr(upper, current_relation)?;
                            let expr = self.plan_expr(expr, current_relation)?;
                            Ok(daft_dsl::functions::list::slice(expr, lower, upper))
                        }
                        _ => {
                            sql_bail!("slice with only one bound not yet supported");
                        }
                    }
                }
            },
            SQLExpr::Array(_) => todo!("ARRAY not yet supported"),
            SQLExpr::Interval(_) => todo!("INTERVAL not yet supported"),
            SQLExpr::MatchAgainst { .. } => todo!("MATCH AGAINST not yet supported"),
            SQLExpr::Wildcard => todo!("WILDCARD not yet supported"),
            SQLExpr::QualifiedWildcard(_) => todo!("QUALIFIED WILDCARD not yet supported"),
            SQLExpr::OuterJoin(_) => todo!("OUTER JOIN not yet supported"),
            SQLExpr::Prior(_) => todo!("PRIOR not yet supported"),
            SQLExpr::Lambda(_) => todo!("LAMBDA not yet supported"),
        }
    }
    fn plan_function(&self, func: &Function, current_rel: &Relation) -> DaftResult<ExprRef> {
        if func.null_treatment.is_some() {
            sql_bail!("null treatment not yet supported");
        }
        if func.filter.is_some() {
            sql_bail!("filter not yet supported");
        }
        if func.over.is_some() {
            sql_bail!("over not yet supported");
        }
        if !func.within_group.is_empty() {
            sql_bail!("within group not yet supported");
        }
        let args = match &func.args {
            sqlparser::ast::FunctionArguments::None => vec![],
            sqlparser::ast::FunctionArguments::Subquery(_) => {
                sql_bail!("subquery not yet supported")
            }
            sqlparser::ast::FunctionArguments::List(arg_list) => {
                if arg_list.duplicate_treatment.is_some() {
                    sql_bail!("duplicate treatment not yet supported");
                }
                if !arg_list.clauses.is_empty() {
                    sql_bail!("clauses not yet supported");
                }
                arg_list
                    .args
                    .iter()
                    .map(|arg| self.plan_function_arg(arg, current_rel))
                    .collect::<DaftResult<Vec<_>>>()?
            }
        };
        let func_name = func.name.to_string();
        match func_name.as_str() {
            // --------------------
            // Numeric Functions
            // --------------------
            "abs" => {
                if args.len() != 1 {
                    sql_bail!("abs takes exactly one argument");
                }
                Ok(daft_dsl::functions::numeric::abs(args[0].clone()))
            }
            "sign" => {
                if args.len() != 1 {
                    sql_bail!("sign takes exactly one argument");
                }
                Ok(daft_dsl::functions::numeric::sign(args[0].clone()))
            }
            "round" => {
                if args.len() != 2 {
                    sql_bail!("round takes exactly one argument");
                }
                sql_bail!("round not yet supported");
            }
            other => {
                sql_bail!("unknown function: '{other}'");
            }
        }

        // todo!()
    }
    fn plan_function_arg(
        &self,
        function_arg: &FunctionArg,
        current_rel: &Relation,
    ) -> DaftResult<ExprRef> {
        match function_arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => self.plan_expr(expr, current_rel),
            _ => todo!("named function args not yet supported"),
        }
    }

    fn sql_operator_to_operator(&self, op: &BinaryOperator) -> DaftResult<Operator> {
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
            other => sql_bail!("Unsupported operator: '{other}'"),
        }
    }
    fn sql_dtype_to_dtype(&self, dtype: &sqlparser::ast::DataType) -> DaftResult<DataType> {
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
                    sql_bail!(
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
                _ => sql_bail!("`time` with timezone is not supported; found tz={}", tz),
            },
            SQLDataType::Datetime(_) => sql_bail!("`datetime` is not supported"),
            SQLDataType::Timestamp(prec, tz) => match tz {
                TimezoneInfo::None => {
                    DataType::Timestamp(self.timeunit_from_precision(prec)?, None)
                }
                _ => sql_bail!("`timestamp` with timezone is not (yet) supported"),
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
                    .collect::<DaftResult<Vec<_>>>()?;
                DataType::Struct(fields)
            }
            other => sql_bail!("unsupported data type: {:?}", other),
        })
    }

    fn timeunit_from_precision(&self, prec: &Option<u64>) -> DaftResult<TimeUnit> {
        Ok(match prec {
            None => TimeUnit::Microseconds,
            Some(n) if (1u64..=3u64).contains(n) => TimeUnit::Milliseconds,
            Some(n) if (4u64..=6u64).contains(n) => TimeUnit::Microseconds,
            Some(n) if (7u64..=9u64).contains(n) => TimeUnit::Nanoseconds,
            Some(n) => {
                sql_bail!(
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
    ) -> DaftResult<ExprRef> {
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
            other => sql_bail!("unary operator {:?} is not supported", other),
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use daft_core::schema::Schema;
    use daft_plan::{
        logical_plan::Source, source_info::PlaceHolderInfo, ClusteringSpec, LogicalPlan, SourceInfo,
    };
    use rstest::rstest;

    fn tbl_1() -> LogicalPlanRef {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("test", DataType::Utf8),
                Field::new("utf8", DataType::Utf8),
                Field::new("i32", DataType::Int32),
                Field::new("i64", DataType::Int64),
                Field::new("f32", DataType::Float32),
                Field::new("f64", DataType::Float64),
                Field::new("bool", DataType::Boolean),
                Field::new("date", DataType::Date),
                Field::new("time", DataType::Time(TimeUnit::Microseconds)),
                Field::new("list_utf8", DataType::new_list(DataType::Utf8)),
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
    fn tbl_2() -> LogicalPlanRef {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("text", DataType::Utf8),
                Field::new("id", DataType::Int32),
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

    fn tbl_3() -> LogicalPlanRef {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("first_name", DataType::Utf8),
                Field::new("last_name", DataType::Utf8),
                Field::new("id", DataType::Int32),
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

    fn setup() -> SQLPlanner {
        let mut ctx = SQLContext::new();

        ctx.register_table("tbl1", tbl_1());
        ctx.register_table("tbl2", tbl_2());
        ctx.register_table("tbl3", tbl_3());

        SQLPlanner::new(ctx)
    }

    #[rstest]
    #[case("select * from tbl1")]
    #[case("select * exclude utf8 from tbl1")]
    #[case("select * exclude (utf8, i32) from tbl1")]
    #[case("select utf8 from tbl1")]
    #[case("select i32 from tbl1")]
    #[case("select i64 from tbl1")]
    #[case("select f32 from tbl1")]
    #[case("select f64 from tbl1")]
    #[case("select bool from tbl1")]
    #[case("select date from tbl1")]
    #[case("select bool::text from tbl1")]
    #[case("select cast(bool as text) from tbl1")]
    #[case("select list_utf8 as a, utf8 as b, utf8 as c from tbl1")]
    #[case("select list_utf8::text[] from tbl1")]
    #[case("select list_utf8[0] from tbl1")]
    #[case("select list_utf8[0:2] from tbl1")]
    #[case("select * from tbl2 join tbl3 on tbl2.id = tbl3.id")]
    #[case("select tbl2.text from tbl2")]
    #[case("select tbl2.text from tbl2 join tbl3 using (id)")]
    #[case(
        r#"
    select
        abs(i32) as abs,
        ceil(i32) as ceil,
        floor(i32) as floor,
        sign(i32) as sign
    from tbl1"#
    )]
    fn test_compiles(#[case] query: &str) -> DaftResult<()> {
        let planner = setup();

        let plan = planner.plan_sql(query);
        assert!(plan.is_ok(), "query: {}\nerror: {:?}", query, plan);

        Ok(())
    }

    #[test]
    fn test_parse_sql() {
        let planner = setup();
        let sql = "select test as a from tbl1";
        let plan = planner.plan_sql(sql).unwrap();

        let expected = LogicalPlanBuilder::new(tbl_1())
            .select(vec![col("test").alias("a")])
            .unwrap()
            .build();
        assert_eq!(plan, expected);
    }

    #[test]
    fn test_where_clause() -> DaftResult<()> {
        let planner = setup();
        let sql = "select test as a from tbl1 where test = 'a'";
        let plan = planner.plan_sql(sql)?;

        let expected = LogicalPlanBuilder::new(tbl_1())
            .filter(col("test").eq(lit("a")))?
            .select(vec![col("test").alias("a")])?
            .build();

        assert_eq!(plan, expected);
        Ok(())
    }

    #[test]
    fn test_cast() -> DaftResult<()> {
        let planner = setup();
        let builder = LogicalPlanBuilder::new(tbl_1());
        let cases = vec![
            (
                "select bool::text from tbl1",
                vec![col("bool").cast(&DataType::Utf8)],
            ),
            (
                "select utf8::bytes from tbl1",
                vec![col("utf8").cast(&DataType::Binary)],
            ),
            (
                r#"select CAST("bool" as text) from tbl1"#,
                vec![col("bool").cast(&DataType::Utf8)],
            ),
        ];
        for (sql, expected) in cases {
            let actual = planner.plan_sql(sql)?;
            let expected = builder.clone().select(expected)?.build();
            assert_eq!(
                actual,
                expected,
                "query: {}\n expected:{}",
                sql,
                expected.repr_ascii(false)
            );
        }

        Ok(())
    }

    #[test]
    fn test_join() -> DaftResult<()> {
        let planner = setup();
        let sql = "select * from tbl2 join tbl3 on tbl2.id = tbl3.id";
        let plan = planner.plan_sql(sql)?;
        let expected = LogicalPlanBuilder::new(tbl_2())
            .join(
                tbl_3(),
                vec![col("id")],
                vec![col("id")],
                daft_core::JoinType::Inner,
                None,
            )?
            .build();
        assert_eq!(plan, expected);
        Ok(())
    }
}
