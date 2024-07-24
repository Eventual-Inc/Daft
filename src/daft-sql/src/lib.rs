use std::collections::HashMap;

use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::{Field, TimeUnit},
    schema::Schema,
    DataType,
};
use daft_dsl::{
    col,
    functions::numeric::{ceil, floor},
    lit, null_lit, Expr, ExprRef, LiteralValue, Operator,
};
use daft_plan::{LogicalPlanBuilder, LogicalPlanRef};
use sqlparser::{
    ast::{
        ArrayElemTypeDef, CastKind, ExactNumberInfo, GroupByExpr, Query, SelectItem, StructField,
        TableWithJoins, TimezoneInfo, UnaryOperator, Value, WildcardAdditionalOptions,
    },
    dialect::GenericDialect,
};

macro_rules! sql_error {
    ($($arg:tt)*) => {
        Err(DaftError::ValueError(format!($($arg)*)))
    }
}

pub struct SQLContext {
    tables: HashMap<String, LogicalPlanRef>,
}
impl Default for SQLContext {
    fn default() -> Self {
        SQLContext {
            tables: HashMap::new(),
        }
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
        let ast = sqlparser::parser::Parser::parse_sql(&GenericDialect {}, &sql).map_err(|e| {
            DaftError::ValueError(format!("failed to parse sql, reason: {}", e.to_string()))
        })?;

        match ast.as_slice() {
            [sqlparser::ast::Statement::Query(query)] => Ok(self.plan_query(query)?.build()),
            other => sql_error!("Unsupported SQL: '{}'", other[0]),
        }
    }

    fn plan_query(&self, query: &Query) -> DaftResult<LogicalPlanBuilder> {
        let selection = query.body.as_select().expect("Expected SELECT statement");
        self.plan_select(selection)
    }

    fn plan_select(&self, selection: &sqlparser::ast::Select) -> DaftResult<LogicalPlanBuilder> {
        if selection.top.is_some() {
            return sql_error!("TOP not (yet) supported");
        }
        if selection.distinct.is_some() {
            return sql_error!("DISTINCT not (yet) supported");
        }
        if selection.into.is_some() {
            return sql_error!("INTO not (yet) supported");
        }
        if !selection.lateral_views.is_empty() {
            return sql_error!("LATERAL not (yet) supported");
        }
        if selection.prewhere.is_some() {
            return sql_error!("PREWHERE not supported");
        }
        if !selection.cluster_by.is_empty() {
            return sql_error!("CLUSTER BY not (yet) supported");
        }
        if !selection.distribute_by.is_empty() {
            return sql_error!("DISTRIBUTE BY not (yet) supported");
        }
        if !selection.sort_by.is_empty() {
            return sql_error!("SORT BY not (yet) supported");
        }
        if selection.having.is_some() {
            return sql_error!("HAVING not (yet) supported");
        }
        if !selection.named_window.is_empty() {
            return sql_error!("WINDOW not (yet) supported");
        }
        if selection.qualify.is_some() {
            return sql_error!("QUALIFY not (yet) supported");
        }
        if selection.connect_by.is_some() {
            return sql_error!("CONNECT BY not (yet) supported");
        }

        match &selection.group_by {
            GroupByExpr::All(s) => {
                if !s.is_empty() {
                    return sql_error!("GROUP BY not supported");
                }
            }
            GroupByExpr::Expressions(expressions, _) => {
                if !expressions.is_empty() {
                    return sql_error!("GROUP BY not supported");
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
        let current_schema = builder.schema();
        if let Some(selection) = &selection.selection {
            let filter = self.plan_expr(selection)?;
            builder = builder.filter(filter)?;
        }

        if selection.projection.len() > 0 {
            let to_select = selection
                .projection
                .iter()
                .map(|expr| self.select_item_to_expr(expr, &current_schema))
                .collect::<DaftResult<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();

            if !to_select.is_empty() {
                builder = builder.select(to_select)?;
            }
        }

        Ok(builder)
    }

    fn plan_from(&self, from: &TableWithJoins) -> DaftResult<LogicalPlanBuilder> {
        let table_factor = from.relation.clone();
        match table_factor {
            ref tbl @ sqlparser::ast::TableFactor::Table { .. } => self.plan_table(tbl),
            other => todo!("Unsupported table factor: {other}"),
        }
    }

    fn plan_table(&self, table: &sqlparser::ast::TableFactor) -> DaftResult<LogicalPlanBuilder> {
        let sqlparser::ast::TableFactor::Table { name, .. } = table else {
            unreachable!("this should never happen, we already checked for TableFactor::Table")
        };
        let table_name = name.to_string();
        let plan = self.context.get_table(&table_name)?;
        let plan_builder = LogicalPlanBuilder::new(plan);
        Ok(plan_builder)
    }

    fn select_item_to_expr(
        &self,
        item: &SelectItem,
        current_schema: &Schema,
    ) -> DaftResult<Vec<ExprRef>> {
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
                    return sql_error!("ILIKE not yet supported");
                }
                if let Some(exclude) = opt_exclude {
                    use sqlparser::ast::ExcludeSelectItem::*;
                    return match exclude {
                        Single(item) => current_schema.exclude(&[&item.to_string()]),
                        Multiple(items) => {
                            let items =
                                items.iter().map(|i| i.to_string()).collect::<Vec<String>>();

                            current_schema.exclude(items.as_slice())
                        }
                    }
                    .and_then(|schema| {
                        let exprs = schema
                            .names()
                            .iter()
                            .map(|n| col(n.as_ref()))
                            .collect::<Vec<_>>();
                        Ok(exprs)
                    });
                }
                if opt_exclude.is_some() {
                    return sql_error!("EXCLUDE not yet supported");
                }
                if opt_except.is_some() {
                    return sql_error!("EXCEPT not yet supported");
                }
                if opt_replace.is_some() {
                    return sql_error!("REPLACE not yet supported");
                }
                if opt_rename.is_some() {
                    return sql_error!("RENAME not yet supported");
                }
                Ok(vec![])
            }
            _ => todo!(),
        }
    }

    fn plan_expr(&self, expr: &sqlparser::ast::Expr) -> DaftResult<ExprRef> {
        use sqlparser::ast::Expr as SQLExpr;
        match expr {
            SQLExpr::Identifier(ident) => {
                if let Some('"') = ident.quote_style {
                    Ok(col(ident.value.to_string()))
                } else {
                    Ok(col(ident.to_string()))
                }
            }
            SQLExpr::Value(Value::SingleQuotedString(s)) => Ok(lit(s.as_str())),
            SQLExpr::Value(Value::Number(n, _)) => {
                let n = n.parse::<i64>().expect("Failed to parse number");
                Ok(lit(n))
            }
            SQLExpr::Value(Value::Boolean(b)) => Ok(lit(*b)),
            SQLExpr::Value(Value::Null) => Ok(null_lit()),
            SQLExpr::Value(other) => todo!("literal value {other} not yet supported"),
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
            SQLExpr::CompoundIdentifier(_) => todo!("compound identifier not yet supported"),
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
            SQLExpr::Ceil { expr, .. } => Ok(ceil(self.plan_expr(expr)?)),
            SQLExpr::Floor { expr, .. } => Ok(floor(self.plan_expr(expr)?)),
            SQLExpr::Position { .. } => todo!("POSITION not yet supported"),
            SQLExpr::Substring { .. } => todo!("SUBSTRING not yet supported"),
            SQLExpr::Trim { .. } => todo!("TRIM not yet supported"),
            SQLExpr::Overlay { .. } => todo!("OVERLAY not yet supported"),
            SQLExpr::Collate { .. } => todo!("COLLATE not yet supported"),
            SQLExpr::Nested(_) => todo!("NESTED not yet supported"),
            SQLExpr::IntroducedString { .. } => todo!("INTRODUCED STRING not yet supported"),
            SQLExpr::TypedString { .. } => todo!("TYPED STRING not yet supported"),
            SQLExpr::MapAccess { .. } => todo!("MAP ACCESS not yet supported"),
            SQLExpr::Function(_) => todo!("FUNCTION not yet supported"),
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
            SQLExpr::Subscript { .. } => todo!("SUBSCRIPT not yet supported"),
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

    fn sql_operator_to_operator(
        &self,
        op: &sqlparser::ast::BinaryOperator,
    ) -> DaftResult<Operator> {
        match op {
            sqlparser::ast::BinaryOperator::Plus => Ok(Operator::Plus),
            sqlparser::ast::BinaryOperator::Minus => Ok(Operator::Minus),
            sqlparser::ast::BinaryOperator::Multiply => Ok(Operator::Multiply),
            sqlparser::ast::BinaryOperator::Divide => Ok(Operator::TrueDivide),
            sqlparser::ast::BinaryOperator::Eq => Ok(Operator::Eq),
            sqlparser::ast::BinaryOperator::Modulo => Ok(Operator::Modulus),
            sqlparser::ast::BinaryOperator::Gt => Ok(Operator::Gt),
            sqlparser::ast::BinaryOperator::Lt => Ok(Operator::Lt),
            sqlparser::ast::BinaryOperator::GtEq => Ok(Operator::GtEq),
            sqlparser::ast::BinaryOperator::LtEq => Ok(Operator::LtEq),
            sqlparser::ast::BinaryOperator::NotEq => Ok(Operator::NotEq),
            sqlparser::ast::BinaryOperator::And => Ok(Operator::And),
            sqlparser::ast::BinaryOperator::Or => Ok(Operator::Or),
            other => return sql_error!("Unsupported operator: '{other}'"),
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
                    return sql_error!(
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
                _ => return sql_error!("`time` with timezone is not supported; found tz={}", tz),
            },
            SQLDataType::Datetime(_) => return sql_error!("`datetime` is not supported"),
            SQLDataType::Timestamp(prec, tz) => match tz {
                TimezoneInfo::None => {
                    DataType::Timestamp(self.timeunit_from_precision(prec)?, None)
                }
                _ => return sql_error!("`timestamp` with timezone is not (yet) supported"),
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
            other => return sql_error!("unsupported data type: {:?}", other),
        })
    }

    fn timeunit_from_precision(&self, prec: &Option<u64>) -> DaftResult<TimeUnit> {
        Ok(match prec {
            None => TimeUnit::Microseconds,
            Some(n) if (1u64..=3u64).contains(n) => TimeUnit::Milliseconds,
            Some(n) if (4u64..=6u64).contains(n) => TimeUnit::Microseconds,
            Some(n) if (7u64..=9u64).contains(n) => TimeUnit::Nanoseconds,
            Some(n) => {
                return sql_error!(
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
    ) -> DaftResult<ExprRef> {
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
            other => return sql_error!("unary operator {:?} is not supported", other),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use daft_plan::{
        logical_plan::Source, source_info::PlaceHolderInfo, ClusteringSpec, LogicalPlan, SourceInfo,
    };
    fn make_source() -> LogicalPlanRef {
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

    fn setup() -> SQLPlanner {
        let mut ctx = SQLContext::new();

        ctx.register_table("tbl", make_source());
        SQLPlanner::new(ctx)
    }

    #[test]
    fn test_parse_sql() {
        let planner = setup();
        let sql = "select test as a from tbl";
        let plan = planner.plan_sql(sql).unwrap();

        let expected = LogicalPlanBuilder::new(make_source())
            .select(vec![col("test").alias("a")])
            .unwrap()
            .build();
        assert_eq!(plan, expected);
    }

    #[test]
    fn test_where_clause() -> DaftResult<()> {
        let planner = setup();
        let sql = "select test as a from tbl where test = 'a'";
        let plan = planner.plan_sql(sql)?;

        let expected = LogicalPlanBuilder::new(make_source())
            .filter(col("test").eq(lit("a")))?
            .select(vec![col("test").alias("a")])?
            .build();

        assert_eq!(plan, expected);
        Ok(())
    }

    #[test]
    fn test_compiles() -> DaftResult<()> {
        let planner = setup();

        let queries = vec![
            "select * from tbl",
            "select * exclude utf8 from tbl",
            "select * exclude (utf8, i32) from tbl",
            "select utf8 from tbl",
            "select i32 from tbl",
            "select i64 from tbl",
            "select f32 from tbl",
            "select f64 from tbl",
            "select bool from tbl",
            "select date from tbl",
            "select list_utf8 as a, utf8 as b, utf8 as c from tbl",
            "select list_utf8::text[] from tbl",
        ];
        for query in queries {
            let plan = planner.plan_sql(query);
            assert!(plan.is_ok(), "query: {}\nerror: {:?}", query, plan);
        }
        Ok(())
    }

    #[test]
    fn test_cast() -> DaftResult<()> {
        let planner = setup();
        let builder = LogicalPlanBuilder::new(make_source());
        let cases = vec![
            (
                "select bool::text from tbl",
                vec![col("bool").cast(&DataType::Utf8)],
            ),
            (
                "select utf8::bytes from tbl",
                vec![col("utf8").cast(&DataType::Binary)],
            ),
            (
                r#"select CAST("bool" as text) from tbl"#,
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
}
