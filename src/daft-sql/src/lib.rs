use std::{collections::HashMap, sync::Arc};

use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::{Field, TimeUnit},
    schema::Schema,
    DataType,
};
use daft_dsl::{col, lit, null_lit, Expr, ExprRef, LiteralValue, Operator};
use daft_plan::{logical_plan::Source, LogicalPlan, LogicalPlanBuilder, LogicalPlanRef};
use sqlparser::{
    ast::{
        ArrayElemTypeDef, ExactNumberInfo, Ident, ObjectName, Query, SelectItem, StructField,
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
        let ast = sqlparser::parser::Parser::parse_sql(&GenericDialect {}, &sql)
            .expect("Failed to parse SQL");
        match ast.as_slice() {
            [sqlparser::ast::Statement::Query(query)] => Ok(self.plan_query(query)?.build()),
            _ => todo!(),
        }
    }

    fn plan_query(&self, query: &Query) -> DaftResult<LogicalPlanBuilder> {
        let selection = query.body.as_select().expect("Expected SELECT statement");
        self.plan_select(selection)
    }

    fn plan_select(&self, selection: &sqlparser::ast::Select) -> DaftResult<LogicalPlanBuilder> {
        let from = selection.clone().from;
        if from.len() != 1 {
            return Err(DaftError::ComputeError(
                "Only one table is supported".to_string(),
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
            _ => todo!(),
        }
    }

    fn plan_table(&self, table: &sqlparser::ast::TableFactor) -> DaftResult<LogicalPlanBuilder> {
        let sqlparser::ast::TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
        } = table
        else {
            unreachable!()
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
            SQLExpr::Identifier(ident) => Ok(col(ident.to_string())),
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
            SQLExpr::CompoundIdentifier(_) => todo!("compound identifier not yet supported"),
            SQLExpr::JsonAccess { value, path } => todo!(),
            SQLExpr::CompositeAccess { expr, key } => todo!(),
            SQLExpr::IsFalse(expr) => Ok(self.plan_expr(expr)?.eq(lit(false))),
            SQLExpr::IsNotFalse(_) => Ok(self.plan_expr(expr)?.eq(lit(false)).not()),
            SQLExpr::IsTrue(expr) => Ok(self.plan_expr(expr)?.eq(lit(true))),
            SQLExpr::IsNotTrue(expr) => Ok(self.plan_expr(expr)?.eq(lit(true)).not()),
            SQLExpr::IsNull(expr) => Ok(self.plan_expr(expr)?.is_null()),
            SQLExpr::IsNotNull(expr) => Ok(self.plan_expr(expr)?.is_null().not()),
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
            SQLExpr::UnaryOp { op, expr } => self.plan_unary_op(op, expr),
            SQLExpr::Convert { .. } => todo!("CONVERT not yet supported"),
            SQLExpr::Cast {
                kind,
                expr,
                data_type,
                format,
            } => todo!(),
            SQLExpr::AtTimeZone {
                timestamp,
                time_zone,
            } => todo!(),
            SQLExpr::Extract { field, expr } => todo!(),
            SQLExpr::Ceil { expr, field } => todo!(),
            SQLExpr::Floor { expr, field } => todo!(),
            SQLExpr::Position { expr, r#in } => todo!(),
            SQLExpr::Substring {
                expr,
                substring_from,
                substring_for,
                special,
            } => todo!(),
            SQLExpr::Trim {
                expr,
                trim_where,
                trim_what,
                trim_characters,
            } => todo!(),
            SQLExpr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => todo!(),
            SQLExpr::Collate { expr, collation } => todo!(),
            SQLExpr::Nested(_) => todo!(),
            SQLExpr::IntroducedString { introducer, value } => todo!(),
            SQLExpr::TypedString { data_type, value } => todo!(),
            SQLExpr::MapAccess { column, keys } => todo!(),
            SQLExpr::Function(_) => todo!(),
            SQLExpr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => todo!(),
            SQLExpr::Exists { subquery, negated } => todo!(),
            SQLExpr::Subquery(_) => todo!(),
            SQLExpr::GroupingSets(_) => todo!(),
            SQLExpr::Cube(_) => todo!(),
            SQLExpr::Rollup(_) => todo!(),
            SQLExpr::Tuple(_) => todo!(),
            SQLExpr::Struct { values, fields } => todo!(),
            SQLExpr::Named { expr, name } => todo!(),
            SQLExpr::Dictionary(_) => todo!(),
            SQLExpr::Map(_) => todo!(),
            SQLExpr::Subscript { expr, subscript } => todo!(),
            SQLExpr::Array(_) => todo!(),
            SQLExpr::Interval(_) => todo!(),
            SQLExpr::MatchAgainst {
                columns,
                match_value,
                opt_search_modifier,
            } => todo!(),
            SQLExpr::Wildcard => todo!(),
            SQLExpr::QualifiedWildcard(_) => todo!(),
            SQLExpr::OuterJoin(_) => todo!(),
            SQLExpr::Prior(_) => todo!(),
            SQLExpr::Lambda(_) => todo!(),
            // other => todo!("expr: {other:?} is not supported yet"),
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
            sqlparser::ast::BinaryOperator::StringConcat => todo!(),
            sqlparser::ast::BinaryOperator::Gt => Ok(Operator::Gt),
            sqlparser::ast::BinaryOperator::Lt => Ok(Operator::Lt),
            sqlparser::ast::BinaryOperator::GtEq => Ok(Operator::GtEq),
            sqlparser::ast::BinaryOperator::LtEq => Ok(Operator::LtEq),
            sqlparser::ast::BinaryOperator::Spaceship => todo!(),
            sqlparser::ast::BinaryOperator::NotEq => Ok(Operator::NotEq),
            sqlparser::ast::BinaryOperator::And => Ok(Operator::And),
            sqlparser::ast::BinaryOperator::Or => Ok(Operator::Or),
            sqlparser::ast::BinaryOperator::Xor => todo!(),
            sqlparser::ast::BinaryOperator::BitwiseOr => todo!(),
            sqlparser::ast::BinaryOperator::BitwiseAnd => todo!(),
            sqlparser::ast::BinaryOperator::BitwiseXor => todo!(),
            sqlparser::ast::BinaryOperator::DuckIntegerDivide => todo!(),
            sqlparser::ast::BinaryOperator::MyIntegerDivide => todo!(),
            sqlparser::ast::BinaryOperator::Custom(_) => todo!(),
            sqlparser::ast::BinaryOperator::PGBitwiseXor => todo!(),
            sqlparser::ast::BinaryOperator::PGBitwiseShiftLeft => todo!(),
            sqlparser::ast::BinaryOperator::PGBitwiseShiftRight => todo!(),
            sqlparser::ast::BinaryOperator::PGExp => todo!(),
            sqlparser::ast::BinaryOperator::PGOverlap => todo!(),
            sqlparser::ast::BinaryOperator::PGRegexMatch => todo!(),
            sqlparser::ast::BinaryOperator::PGRegexIMatch => todo!(),
            sqlparser::ast::BinaryOperator::PGRegexNotMatch => todo!(),
            sqlparser::ast::BinaryOperator::PGRegexNotIMatch => todo!(),
            sqlparser::ast::BinaryOperator::PGLikeMatch => todo!(),
            sqlparser::ast::BinaryOperator::PGILikeMatch => todo!(),
            sqlparser::ast::BinaryOperator::PGNotLikeMatch => todo!(),
            sqlparser::ast::BinaryOperator::PGNotILikeMatch => todo!(),
            sqlparser::ast::BinaryOperator::PGStartsWith => todo!(),
            sqlparser::ast::BinaryOperator::Arrow => todo!(),
            sqlparser::ast::BinaryOperator::LongArrow => todo!(),
            sqlparser::ast::BinaryOperator::HashArrow => todo!(),
            sqlparser::ast::BinaryOperator::HashLongArrow => todo!(),
            sqlparser::ast::BinaryOperator::AtAt => todo!(),
            sqlparser::ast::BinaryOperator::AtArrow => todo!(),
            sqlparser::ast::BinaryOperator::ArrowAt => todo!(),
            sqlparser::ast::BinaryOperator::HashMinus => todo!(),
            sqlparser::ast::BinaryOperator::AtQuestion => todo!(),
            sqlparser::ast::BinaryOperator::Question => todo!(),
            sqlparser::ast::BinaryOperator::QuestionAnd => todo!(),
            sqlparser::ast::BinaryOperator::QuestionPipe => todo!(),
            sqlparser::ast::BinaryOperator::PGCustomBinaryOperator(_) => todo!(),
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
            // unsigned integer: the following do not map to PostgreSQL types/syntax, but
            // are enabled for wider compatibility (eg: "CAST(col AS BIGINT UNSIGNED)").
            // ---------------------------------
            SQLDataType::UnsignedInt(_) | SQLDataType::UnsignedInteger(_) => DataType::UInt32,
            SQLDataType::UnsignedInt2(_) | SQLDataType::UnsignedSmallInt(_) => DataType::UInt16,
            SQLDataType::UnsignedInt4(_) | SQLDataType::UnsignedMediumInt(_) => DataType::UInt32,
            SQLDataType::UnsignedInt8(_) | SQLDataType::UnsignedBigInt(_) => DataType::UInt64,
            SQLDataType::UnsignedTinyInt(_) => DataType::UInt8, // see also: "custom" types below
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
    use super::*;
    use daft_core::{datatypes::Field, schema::Schema, DataType};
    use daft_plan::{source_info::PlaceHolderInfo, ClusteringSpec, SourceInfo};

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

    #[test]
    fn test_parse_sql() {
        let mut ctx = SQLContext::new();

        ctx.register_table("tbl", make_source());
        let planner = SQLPlanner::new(ctx);
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
        let mut ctx = SQLContext::new();

        ctx.register_table("tbl", make_source());
        let planner = SQLPlanner::new(ctx);
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
        let mut ctx = SQLContext::new();
        ctx.register_table("tbl", make_source());

        let planner = SQLPlanner::new(ctx);
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
        ];
        for query in queries {
            let plan = planner.plan_sql(query);
            assert!(plan.is_ok(), "query: {}\nerror: {:?}", query, plan);
        }
        Ok(())
    }
}
