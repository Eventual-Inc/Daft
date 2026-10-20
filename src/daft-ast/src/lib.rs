//! The AST the SQL parser produces, and the action functions its grammar rules call to build it.

mod display;

use std::borrow::Cow;

//------------------------------
// Statements
//------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Query(Query),
    Describe(Describe),
    ShowTables(ShowTables),
    Use(Use),
    CreateTable(CreateTable),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Describe {
    Query(Box<Query>),
    Table(TableName),
}

/// `SHOW TABLES [FROM catalog[.namespace]]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowTables {
    pub from: Option<ObjectName>,
}

/// `USE catalog[.schema]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Use {
    pub catalog: Ident,
    pub schema: Option<Ident>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTable {
    pub if_not_exists: bool,
    pub name: ObjectName,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: Ident,
    pub ty: DataType,
}

//------------------------------
// Queries
//------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub with: Vec<Cte>,
    pub body: SetExpr,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<Limit>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetExpr {
    Select(Box<Select>),
    SetOp {
        operator: SetOperator,
        left: Box<Self>,
        right: Box<Self>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetOperator {
    pub op: SetOp,
    pub quantifier: Option<SetQuantifier>,
    pub by_name: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOp {
    Union,
    Intersect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetQuantifier {
    Distinct,
    All,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Cte {
    pub name: Ident,
    pub columns: Vec<Ident>,
    pub query: Query,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limit {
    pub value: LimitValue,
    pub offset: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitValue {
    All,
    Count(u64),
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub order: Option<SortOrder>,
    pub nulls: Option<NullsOrder>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullsOrder {
    First,
    Last,
}

//------------------------------
// SELECT
//------------------------------

/// A SELECT block; `FROM t SELECT a` and `SELECT a FROM t` build the same one.
#[derive(Debug, Clone, PartialEq)]
pub struct Select {
    pub distinct: Option<DistinctClause>,
    pub items: Vec<SelectItem>,
    pub from: Vec<TableRef>,
    pub filter: Option<Expr>,
    pub group_by: Option<GroupBy>,
    pub having: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DistinctClause {
    All,
    Distinct,
    On(Vec<Expr>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectItem {
    pub expr: Expr,
    pub alias: Option<Ident>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum GroupBy {
    Exprs(Vec<Expr>),
    Rollup(Vec<Expr>),
}

//------------------------------
// FROM and joins
//------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct TableRef {
    pub factor: TableFactor,
    pub joins: Vec<Join>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableFactor {
    Table {
        name: TableName,
        alias: Option<TableAlias>,
        sample: Option<Sample>,
    },
    Subquery {
        query: Box<Query>,
        alias: Option<TableAlias>,
    },
    Function {
        name: Ident,
        args: Vec<FunctionArg>,
        alias: Option<TableAlias>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableName {
    Object(ObjectName),
    /// A quoted file path such as `'data/t.parquet'`.
    Path(String),
}

/// A dotted name: `name`, `schema.name`, or `catalog.schema….name`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectName(pub Vec<Ident>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableAlias {
    pub name: Ident,
    pub columns: Vec<Ident>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    /// `None` is a bare `JOIN`.
    pub kind: Option<JoinType>,
    pub table: TableRef,
    pub constraint: JoinConstraint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinConstraint {
    On(Expr),
    Using(Vec<Ident>),
}

/// `TABLESAMPLE`; the parenthesized and bare count forms build the same one.
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub method: Option<SampleMethod>,
    pub count: SampleCount,
    pub seed: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SampleMethod {
    Bernoulli,
    System,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SampleCount {
    pub value: Lit,
    pub unit: SampleUnit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SampleUnit {
    Percent,
    Rows,
}

//------------------------------
// Expressions
//------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A column, qualified by a table alias and/or followed by struct fields.
    Column(Vec<Ident>),
    Lit(Lit),
    Star {
        qualifier: Option<Ident>,
        exclude: Vec<Ident>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Self>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Self>,
        right: Box<Self>,
    },
    IsNull {
        expr: Box<Self>,
        negated: bool,
    },
    In {
        expr: Box<Self>,
        negated: bool,
        rhs: InExpression,
    },
    Between {
        expr: Box<Self>,
        negated: bool,
        low: Box<Self>,
        high: Box<Self>,
    },
    Like {
        expr: Box<Self>,
        negated: bool,
        case_insensitive: bool,
        pattern: Box<Self>,
    },
    Exists(Box<Query>),
    Subquery(Box<Query>),
    Case {
        whens: Vec<(Self, Self)>,
        else_: Box<Self>,
    },
    /// `CAST`, `TRY_CAST`, or `::`, which is a `CAST`.
    Cast {
        expr: Box<Self>,
        ty: DataType,
        try_: bool,
    },
    /// `DATE '…'`.
    Date(String),
    Interval {
        value: String,
        unit: Option<DatePart>,
    },
    Function(Function),
    Coalesce(Vec<Self>),
    Extract {
        part: DatePart,
        expr: Box<Self>,
    },
    /// `SUBSTRING`; the `FROM`/`FOR` and comma forms build the same one.
    Substring {
        expr: Box<Self>,
        from: Option<Box<Self>>,
        for_: Option<Box<Self>>,
    },
    Subscript {
        expr: Box<Self>,
        index: Box<Self>,
    },
    List(Vec<Self>),
    Struct(Vec<(Ident, Self)>),
}

impl Expr {
    pub fn unary(op: UnaryOp, expr: Self) -> Self {
        Self::Unary {
            op,
            expr: Box::new(expr),
        }
    }

    pub fn binary(op: BinaryOp, left: Self, right: Self) -> Self {
        Self::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum InExpression {
    List(Vec<Expr>),
    Select(Box<Query>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Function {
    pub name: Ident,
    pub quantifier: Option<SetQuantifier>,
    pub args: Vec<FunctionArg>,
    pub over: Option<Box<Window>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FunctionArg {
    Named(Ident, Expr),
    Positional(Expr),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Window {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub frame: Option<Frame>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Frame {
    pub units: FrameUnits,
    pub extent: FrameExtent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameUnits {
    Rows,
    Range,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FrameExtent {
    Single(FrameBound),
    Between(FrameBound, FrameBound),
}

#[derive(Debug, Clone, PartialEq)]
pub enum FrameBound {
    Unbounded(FrameDirection),
    CurrentRow,
    /// A number or interval literal.
    Offset(Expr, FrameDirection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameDirection {
    Preceding,
    Following,
}

/// A unit for `INTERVAL` or a part for `EXTRACT`; the grammar picks which apply where.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatePart {
    Year,
    Quarter,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Plus,
    Not,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    IntDiv,
    Mod,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}

//------------------------------
// Types
//------------------------------

/// A type name; spellings both engines treat as one type share a variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    UInt16,
    UInt32,
    UInt64,
    Real,
    Double,
    Float(u64),
    Decimal(Vec<u64>),
    Varchar(Option<Vec<u64>>),
    Binary,
    Date,
    Time,
    Timestamp(Option<Vec<u64>>),
    Interval,
    /// Element type and optional fixed size.
    Array(Box<Self>, Option<u64>),
}

//------------------------------
// Names and literals
//------------------------------

/// A name as written; `quoted` keeps `"X y"` and quoted keywords printable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ident {
    pub value: String,
    pub quoted: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Lit {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
}

//------------------------------
// Parser Actions: Statements
//------------------------------

/// Builds `DESCRIBE <query>`.
#[inline]
pub fn describe_select(query: Query) -> Statement {
    Statement::Describe(Describe::Query(Box::new(query)))
}

/// Builds `SHOW TABLES FROM <name>`.
#[inline]
pub fn show_tables(from: ObjectName) -> Statement {
    Statement::ShowTables(ShowTables { from: Some(from) })
}

/// Builds a bare `SHOW TABLES`.
#[inline]
pub fn show_by_name() -> Statement {
    Statement::ShowTables(ShowTables { from: None })
}

/// Builds `USE catalog.schema`.
#[inline]
pub fn use_target_catalog_schema(catalog: Ident, schema: Ident) -> Use {
    Use {
        catalog,
        schema: Some(schema),
    }
}

/// Builds `USE catalog`.
#[inline]
pub fn catalog_name_as_use_target(catalog: Ident) -> Use {
    Use {
        catalog,
        schema: None,
    }
}

/// Builds `CREATE TABLE` from its column list.
#[inline]
pub fn create_table_stmt(
    if_not_exists: bool,
    name: ObjectName,
    columns: Vec<ColumnDef>,
) -> CreateTable {
    CreateTable {
        if_not_exists,
        name,
        columns,
    }
}

/// Builds one `CREATE TABLE` column.
#[inline]
pub fn column_definition(name: Ident, ty: DataType) -> ColumnDef {
    ColumnDef { name, ty }
}

//------------------------------
// Parser Actions: Queries
//------------------------------

/// Builds a query from its WITH clause, set-operation chain, and ORDER BY/LIMIT.
#[inline]
pub fn select_statement_internal(
    with: Option<Vec<Cte>>,
    body: SetExpr,
    (order_by, limit): (Option<Vec<OrderByExpr>>, Option<Limit>),
) -> Query {
    Query {
        with: with.unwrap_or_default(),
        body,
        order_by: order_by.unwrap_or_default(),
        limit,
    }
}

/// Folds a UNION chain left to right.
#[inline]
pub fn select_set_op_chain(first: SetExpr, tail: Vec<(SetOperator, SetExpr)>) -> SetExpr {
    fold_set_ops(first, tail)
}

/// Folds an INTERSECT chain left to right.
#[inline]
pub fn intersect_chain(first: SetExpr, tail: Vec<(SetOperator, SetExpr)>) -> SetExpr {
    fold_set_ops(first, tail)
}

fn fold_set_ops(first: SetExpr, tail: Vec<(SetOperator, SetExpr)>) -> SetExpr {
    tail.into_iter()
        .fold(first, |left, (operator, right)| SetExpr::SetOp {
            operator,
            left: Box::new(left),
            right: Box::new(right),
        })
}

/// Builds a `UNION [DISTINCT | ALL] [BY NAME]` operator.
#[inline]
pub fn setop_clause(op: SetOp, quantifier: Option<SetQuantifier>, by_name: bool) -> SetOperator {
    SetOperator {
        op,
        quantifier,
        by_name,
    }
}

/// Builds an `INTERSECT [DISTINCT | ALL]` operator.
#[inline]
pub fn set_intersect_clause(quantifier: Option<SetQuantifier>) -> SetOperator {
    SetOperator {
        op: SetOp::Intersect,
        quantifier,
        by_name: false,
    }
}

/// Builds one operand of a set operation.
#[inline]
pub fn select_atom(select: Select) -> SetExpr {
    SetExpr::Select(Box::new(select))
}

/// Builds one `name [(columns)] AS (query)` CTE.
#[inline]
pub fn with_statement(name: Ident, columns: Option<Vec<Ident>>, query: Query) -> Cte {
    Cte {
        name,
        columns: columns.unwrap_or_default(),
        query,
    }
}

/// Builds `LIMIT n [OFFSET m]`.
#[inline]
pub fn limit_offset_clause(value: LimitValue, offset: Option<u64>) -> Limit {
    Limit { value, offset }
}

/// Builds `OFFSET m LIMIT n`.
#[inline]
pub fn offset_limit_clause(offset: u64, value: LimitValue) -> Limit {
    Limit {
        value,
        offset: Some(offset),
    }
}

/// Builds one ORDER BY key.
#[inline]
pub fn order_by_expression(
    expr: Expr,
    order: Option<SortOrder>,
    nulls: Option<NullsOrder>,
) -> OrderByExpr {
    OrderByExpr { expr, order, nulls }
}

//------------------------------
// Parser Actions: SELECT
//------------------------------

/// Attaches WHERE, GROUP BY and HAVING to a select block.
#[inline]
pub fn simple_select(
    select: Select,
    filter: Option<Expr>,
    group_by: Option<GroupBy>,
    having: Option<Expr>,
) -> Select {
    Select {
        filter,
        group_by,
        having,
        ..select
    }
}

/// Attaches a trailing FROM clause to a SELECT clause.
#[inline]
pub fn select_from_clause(select: Select, from: Option<Vec<TableRef>>) -> Select {
    Select {
        from: from.unwrap_or_default(),
        ..select
    }
}

/// Attaches a leading FROM clause to a SELECT clause.
#[inline]
pub fn from_select_clause(from: Vec<TableRef>, select: Select) -> Select {
    Select { from, ..select }
}

/// Builds a select block holding only its SELECT clause.
#[inline]
pub fn select_clause(distinct: Option<DistinctClause>, items: Vec<SelectItem>) -> Select {
    Select {
        distinct,
        items,
        from: vec![],
        filter: None,
        group_by: None,
        having: None,
    }
}

/// Builds `DISTINCT [ON (exprs)]`.
#[inline]
pub fn distinct_on(targets: Option<Vec<Expr>>) -> DistinctClause {
    targets.map_or(DistinctClause::Distinct, DistinctClause::On)
}

/// Builds `expr AS label`.
#[inline]
pub fn expression_as_collabel(expr: Expr, alias: Ident) -> SelectItem {
    SelectItem {
        expr,
        alias: Some(alias),
    }
}

/// Builds `expr [alias]`.
#[inline]
pub fn expression_opt_identifier(expr: Expr, alias: Option<Ident>) -> SelectItem {
    SelectItem { expr, alias }
}

/// Builds `[qualifier.]* [EXCLUDE …]`.
#[inline]
pub fn star_expression(qualifier: Option<Ident>, exclude: Option<Vec<Ident>>) -> Expr {
    Expr::Star {
        qualifier,
        exclude: exclude.unwrap_or_default(),
    }
}

/// Builds the one-name list of an unparenthesized `EXCLUDE name`.
#[inline]
pub fn exclude_name_single(name: Ident) -> Vec<Ident> {
    vec![name]
}

//------------------------------
// Parser Actions: FROM and joins
//------------------------------

/// Builds a comma-separated FROM list.
#[inline]
pub fn from_clause(first: TableRef, rest: Vec<TableRef>) -> Vec<TableRef> {
    std::iter::once(first).chain(rest).collect()
}

/// Builds a table reference and the joins that follow it.
#[inline]
pub fn table_ref(factor: TableFactor, joins: Vec<Join>) -> TableRef {
    TableRef { factor, joins }
}

/// Builds `(query) [alias]`.
#[inline]
pub fn table_subquery(query: Query, alias: Option<TableAlias>) -> TableFactor {
    TableFactor::Subquery {
        query: Box::new(query),
        alias,
    }
}

/// Builds a named table or file with its alias and sample.
#[inline]
pub fn base_table_ref(
    name: TableName,
    alias: Option<TableAlias>,
    sample: Option<Sample>,
) -> TableFactor {
    TableFactor::Table {
        name,
        alias,
        sample,
    }
}

/// Builds `schema.table`.
#[inline]
pub fn schema_reserved_table(schema: Ident, table: Ident) -> ObjectName {
    ObjectName(vec![schema, table])
}

/// Builds `catalog.schema….table`.
#[inline]
pub fn catalog_reserved_schema_table(
    catalog: Ident,
    schemas: Vec<Ident>,
    table: Ident,
) -> ObjectName {
    dotted(catalog, schemas, table)
}

/// Builds a table function call with its alias.
#[inline]
pub fn table_function_lateral_opt(
    name: Ident,
    args: Option<Vec<FunctionArg>>,
    alias: Option<TableAlias>,
) -> TableFactor {
    TableFactor::Function {
        name,
        args: args.unwrap_or_default(),
        alias,
    }
}

/// Builds `AS name [(columns)]`.
#[inline]
pub fn table_alias_as(name: Ident, columns: Option<Vec<Ident>>) -> TableAlias {
    TableAlias {
        name,
        columns: columns.unwrap_or_default(),
    }
}

/// Builds `name [(columns)]`.
#[inline]
pub fn table_alias_without_as(name: Ident, columns: Option<Vec<Ident>>) -> TableAlias {
    TableAlias {
        name,
        columns: columns.unwrap_or_default(),
    }
}

/// Builds `[type] JOIN table ON|USING …`.
#[inline]
pub fn regular_join_clause(
    kind: Option<JoinType>,
    table: TableRef,
    constraint: JoinConstraint,
) -> Join {
    Join {
        kind,
        table,
        constraint,
    }
}

/// Builds `TABLESAMPLE [method] (count) [REPEATABLE (seed)]`.
#[inline]
pub fn sample_entry_function(
    method: Option<SampleMethod>,
    count: SampleCount,
    seed: Option<u64>,
) -> Sample {
    Sample {
        method,
        count,
        seed,
    }
}

/// Builds `TABLESAMPLE count`.
#[inline]
pub fn sample_entry_count(count: SampleCount) -> Sample {
    Sample {
        method: None,
        count,
        seed: None,
    }
}

/// Builds a sample size and its unit.
#[inline]
pub fn sample_count(value: Lit, unit: SampleUnit) -> SampleCount {
    SampleCount { value, unit }
}

//------------------------------
// Parser Actions: Expressions
//------------------------------

/// Builds `expr[index]`.
#[inline]
pub fn slice_expression(expr: Expr, index: Expr) -> Expr {
    Expr::Subscript {
        expr: Box::new(expr),
        index: Box::new(index),
    }
}

/// Builds `expr::type` as a CAST.
#[inline]
pub fn cast_operator(expr: Expr, ty: DataType) -> Expr {
    Expr::Cast {
        expr: Box::new(expr),
        ty,
        try_: false,
    }
}

/// Builds a prefix `-` or `+`.
#[inline]
pub fn prefix_expression(op: UnaryOp, expr: Expr) -> Expr {
    Expr::unary(op, expr)
}

/// Builds `*`, `/`, `//` or `%`.
#[inline]
pub fn multiplicative_expression(left: Expr, op: BinaryOp, right: Expr) -> Expr {
    Expr::binary(op, left, right)
}

/// Builds `+` or `-`.
#[inline]
pub fn additive_expression(left: Expr, op: BinaryOp, right: Expr) -> Expr {
    Expr::binary(op, left, right)
}

/// Builds a comparison.
#[inline]
pub fn comparison_expression(left: Expr, op: BinaryOp, right: Expr) -> Expr {
    Expr::binary(op, left, right)
}

/// Builds `expr [NOT] IN (…)`.
#[inline]
pub fn in_predicate(expr: Expr, negated: bool, rhs: InExpression) -> Expr {
    Expr::In {
        expr: Box::new(expr),
        negated,
        rhs,
    }
}

/// Builds the subquery side of `IN`.
#[inline]
pub fn in_select_statement(query: Query) -> InExpression {
    InExpression::Select(Box::new(query))
}

/// Builds `expr [NOT] BETWEEN low AND high`.
#[inline]
pub fn between_clause(expr: Expr, negated: bool, low: Expr, high: Expr) -> Expr {
    Expr::Between {
        expr: Box::new(expr),
        negated,
        low: Box::new(low),
        high: Box::new(high),
    }
}

/// Builds `expr [NOT] LIKE|ILIKE pattern`.
#[inline]
pub fn like_clause(expr: Expr, negated: bool, case_insensitive: bool, pattern: Expr) -> Expr {
    Expr::Like {
        expr: Box::new(expr),
        negated,
        case_insensitive,
        pattern: Box::new(pattern),
    }
}

/// Builds the postfix `expr NOT NULL`.
#[inline]
pub fn not_null(expr: Expr) -> Expr {
    is_literal(expr, true)
}

/// Builds `expr IS [NOT] NULL`.
#[inline]
pub fn is_literal(expr: Expr, negated: bool) -> Expr {
    Expr::IsNull {
        expr: Box::new(expr),
        negated,
    }
}

/// Builds `NOT expr`.
#[inline]
pub fn logical_not_expression(expr: Expr) -> Expr {
    Expr::unary(UnaryOp::Not, expr)
}

/// Builds `left AND right`.
#[inline]
pub fn logical_and_expression(left: Expr, right: Expr) -> Expr {
    Expr::binary(BinaryOp::And, left, right)
}

/// Builds `left OR right`.
#[inline]
pub fn logical_or_expression(left: Expr, right: Expr) -> Expr {
    Expr::binary(BinaryOp::Or, left, right)
}

//------------------------------
// Parser Actions: Primary expressions
//------------------------------

/// Builds `EXISTS (query)`.
#[inline]
pub fn subquery_exists(query: Query) -> Expr {
    Expr::Exists(Box::new(query))
}

/// Builds a scalar `(query)`.
#[inline]
pub fn subquery_expression(query: Query) -> Expr {
    Expr::Subquery(Box::new(query))
}

/// Builds `CASE WHEN … THEN … ELSE … END`.
#[inline]
pub fn case_expression(whens: Vec<(Expr, Expr)>, else_: Expr) -> Expr {
    Expr::Case {
        whens,
        else_: Box::new(else_),
    }
}

/// Builds `CAST(expr AS type)` or `TRY_CAST(expr AS type)`.
#[inline]
pub fn cast_expression(try_: bool, (expr, ty): (Expr, DataType)) -> Expr {
    Expr::Cast {
        expr: Box::new(expr),
        ty,
        try_,
    }
}

/// Builds `INTERVAL '…' [unit]`.
#[inline]
pub fn interval_literal(value: String, unit: Option<DatePart>) -> Expr {
    Expr::Interval { value, unit }
}

/// Builds a function call with its optional window.
#[inline]
pub fn function_expression(
    name: Ident,
    (quantifier, args): (Option<SetQuantifier>, Option<Vec<FunctionArg>>),
    over: Option<Window>,
) -> Expr {
    Expr::Function(Function {
        name,
        quantifier,
        args: args.unwrap_or_default(),
        over: over.map(Box::new),
    })
}

/// Builds the contents of `OVER (…)`.
#[inline]
pub fn window_frame_contents(
    partition_by: Option<Vec<Expr>>,
    order_by: Option<Vec<OrderByExpr>>,
    frame: Option<Frame>,
) -> Window {
    Window {
        partition_by: partition_by.unwrap_or_default(),
        order_by: order_by.unwrap_or_default(),
        frame,
    }
}

/// Builds `ROWS|RANGE extent`.
#[inline]
pub fn frame_clause(units: FrameUnits, extent: FrameExtent) -> Frame {
    Frame { units, extent }
}

/// Builds `EXTRACT(part FROM expr)`.
#[inline]
pub fn extract_expression((part, expr): (DatePart, Expr)) -> Expr {
    Expr::Extract {
        part,
        expr: Box::new(expr),
    }
}

/// Builds `SUBSTRING(expr FROM … FOR …)`.
#[inline]
pub fn substring_parameters(expr: Expr, (from, for_): (Option<Expr>, Option<Expr>)) -> Expr {
    substring(expr, from, for_)
}

/// Builds `SUBSTRING(expr, from [, for])`.
#[inline]
pub fn substring_expression_list(expr: Expr, from: Expr, for_: Option<Expr>) -> Expr {
    substring(expr, Some(from), for_)
}

/// Builds the `FROM … [FOR …]` arguments of SUBSTRING.
#[inline]
pub fn substring_from_optional_for(from: Expr, for_: Option<Expr>) -> (Option<Expr>, Option<Expr>) {
    (Some(from), for_)
}

/// Builds the `FOR …` argument of SUBSTRING.
#[inline]
pub fn substring_for(for_: Expr) -> (Option<Expr>, Option<Expr>) {
    (None, Some(for_))
}

fn substring(expr: Expr, from: Option<Expr>, for_: Option<Expr>) -> Expr {
    Expr::Substring {
        expr: Box::new(expr),
        from: from.map(Box::new),
        for_: for_.map(Box::new),
    }
}

/// Builds the dotted parts of a column reference.
#[inline]
pub fn nested_column_name(qualifiers: Vec<Ident>, name: Ident) -> Vec<Ident> {
    let mut parts = qualifiers;
    parts.push(name);
    parts
}

//------------------------------
// Parser Actions: Types
//------------------------------

/// Wraps a base type in its `[]` / `[n]` array bounds, innermost first.
#[inline]
pub fn type_(base: DataType, bounds: Vec<Option<u64>>) -> DataType {
    bounds
        .into_iter()
        .fold(base, |ty, size| DataType::Array(Box::new(ty), size))
}

//------------------------------
// Parser Actions: Names
//------------------------------

/// Builds an unquoted name.
#[inline]
pub fn plain_identifier(name: &str) -> Ident {
    Ident {
        value: name.to_string(),
        quoted: false,
    }
}

/// Builds a `"…"` name.
#[inline]
pub fn quoted_identifier(raw: &str) -> Ident {
    Ident {
        value: unquote(raw),
        quoted: true,
    }
}

/// Builds a name written as a string literal.
#[inline]
pub fn string_literal_identifier(value: String) -> Ident {
    Ident {
        value,
        quoted: true,
    }
}

/// Builds a one-part qualified name.
#[inline]
pub fn qualified_name(name: Ident) -> ObjectName {
    ObjectName(vec![name])
}

/// Builds `schema.name`.
#[inline]
pub fn schema_reserved_identifier_or_string_literal(schema: Ident, name: Ident) -> ObjectName {
    ObjectName(vec![schema, name])
}

/// Builds `catalog.schema….name`.
#[inline]
pub fn catalog_reserved_schema_identifier(
    catalog: Ident,
    schemas: Vec<Ident>,
    name: Ident,
) -> ObjectName {
    dotted(catalog, schemas, name)
}

/// Builds a one-part table name.
#[inline]
pub fn table_name(name: Ident) -> TableName {
    TableName::Object(ObjectName(vec![name]))
}

fn dotted(first: Ident, middle: Vec<Ident>, last: Ident) -> ObjectName {
    ObjectName(
        std::iter::once(first)
            .chain(middle)
            .chain(std::iter::once(last))
            .collect(),
    )
}

//------------------------------
// Parser Actions: Literals
//------------------------------

/// Joins a `'…'` literal and its continuations into one value.
#[inline]
pub fn string_literal(first: &str, rest: Vec<&str>) -> String {
    std::iter::once(first).chain(rest).map(unquote).collect()
}

/// Parses a number; fractions, exponents, and integers past BIGINT are DOUBLE.
#[inline]
pub fn number_literal(text: &str) -> Option<Lit> {
    let digits = strip_separators(text);
    digits
        .parse()
        .map(Lit::Int)
        .or_else(|_| digits.parse().map(Lit::Float))
        .ok()
}

/// Parses a number the spec restricts to a non-negative integer.
#[inline]
pub fn integer_literal(text: &str) -> Option<u64> {
    strip_separators(text).parse().ok()
}

/// `_` separates digits.
fn strip_separators(text: &str) -> Cow<'_, str> {
    if text.contains('_') {
        Cow::Owned(text.replace('_', ""))
    } else {
        Cow::Borrowed(text)
    }
}

/// The value of a `'…'` or `"…"` token: quotes stripped and doubled quotes halved.
fn unquote(raw: &str) -> String {
    let quote = &raw[..1];
    raw[1..raw.len() - 1].replace(&quote.repeat(2), quote)
}
