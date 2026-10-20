//! Prints SQL that re-parses to the same tree; every compound expression is parenthesized so the
//! tree's shape is visible.

use std::fmt::{self, Display, Formatter, Write};

use crate::{
    BinaryOp, ColumnDef, CreateTable, Cte, DataType, DatePart, Describe, DistinctClause, Expr,
    Frame, FrameBound, FrameDirection, FrameExtent, FrameUnits, Function, FunctionArg, GroupBy,
    Ident, InExpression, Join, JoinConstraint, JoinType, Limit, LimitValue, Lit, NullsOrder,
    ObjectName, OrderByExpr, Query, Sample, SampleMethod, SampleUnit, Select, SelectItem, SetExpr,
    SetOp, SetOperator, SetQuantifier, ShowTables, SortOrder, Statement, TableAlias, TableFactor,
    TableName, TableRef, UnaryOp, Use, Window,
};

/// Items joined by a separator.
struct Sep<'a, T>(&'a [T], &'static str);

impl<T: Display> Display for Sep<'_, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for (i, item) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str(self.1)?;
            }
            write!(f, "{item}")?;
        }
        Ok(())
    }
}

/// Wraps `s` in `delim`, doubling any `delim` inside.
fn quote(f: &mut Formatter<'_>, s: &str, delim: char) -> fmt::Result {
    f.write_char(delim)?;
    for c in s.chars() {
        if c == delim {
            f.write_char(delim)?;
        }
        f.write_char(c)?;
    }
    f.write_char(delim)
}

impl Display for Statement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Query(query) => write!(f, "{query}"),
            Self::Describe(Describe::Query(query)) => write!(f, "DESCRIBE {query}"),
            Self::Describe(Describe::Table(name)) => write!(f, "DESCRIBE {name}"),
            Self::ShowTables(ShowTables { from: None }) => f.write_str("SHOW TABLES"),
            Self::ShowTables(ShowTables { from: Some(from) }) => {
                write!(f, "SHOW TABLES FROM {from}")
            }
            Self::Use(Use { catalog, schema }) => {
                write!(f, "USE {catalog}")?;
                if let Some(schema) = schema {
                    write!(f, ".{schema}")?;
                }
                Ok(())
            }
            Self::CreateTable(create) => write!(f, "{create}"),
        }
    }
}

impl Display for CreateTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("CREATE TABLE ")?;
        if self.if_not_exists {
            f.write_str("IF NOT EXISTS ")?;
        }
        write!(f, "{} ({})", self.name, Sep(&self.columns, ", "))
    }
}

impl Display for ColumnDef {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.name, self.ty)
    }
}

impl Display for Query {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if !self.with.is_empty() {
            write!(f, "WITH {} ", Sep(&self.with, ", "))?;
        }
        write!(f, "{}", self.body)?;
        if !self.order_by.is_empty() {
            write!(f, " ORDER BY {}", Sep(&self.order_by, ", "))?;
        }
        if let Some(limit) = &self.limit {
            write!(f, " {limit}")?;
        }
        Ok(())
    }
}

impl Display for Cte {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)?;
        if !self.columns.is_empty() {
            write!(f, "({})", Sep(&self.columns, ", "))?;
        }
        write!(f, " AS ({})", self.query)
    }
}

impl Display for SetExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Select(select) => write!(f, "{select}"),
            Self::SetOp {
                operator,
                left,
                right,
            } => write!(f, "{left} {operator} {right}"),
        }
    }
}

impl Display for SetOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self.op {
            SetOp::Union => "UNION",
            SetOp::Intersect => "INTERSECT",
        })?;
        if let Some(quantifier) = self.quantifier {
            write!(f, " {quantifier}")?;
        }
        if self.by_name {
            f.write_str(" BY NAME")?;
        }
        Ok(())
    }
}

impl Display for SetQuantifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Distinct => "DISTINCT",
            Self::All => "ALL",
        })
    }
}

impl Display for Limit {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.value {
            LimitValue::All => f.write_str("LIMIT ALL")?,
            LimitValue::Count(n) => write!(f, "LIMIT {n}")?,
        }
        if let Some(offset) = self.offset {
            write!(f, " OFFSET {offset}")?;
        }
        Ok(())
    }
}

impl Display for OrderByExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)?;
        match self.order {
            Some(SortOrder::Asc) => f.write_str(" ASC")?,
            Some(SortOrder::Desc) => f.write_str(" DESC")?,
            None => {}
        }
        match self.nulls {
            Some(NullsOrder::First) => f.write_str(" NULLS FIRST")?,
            Some(NullsOrder::Last) => f.write_str(" NULLS LAST")?,
            None => {}
        }
        Ok(())
    }
}

impl Display for Select {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("SELECT ")?;
        match &self.distinct {
            Some(DistinctClause::All) => f.write_str("ALL ")?,
            Some(DistinctClause::Distinct) => f.write_str("DISTINCT ")?,
            Some(DistinctClause::On(exprs)) => write!(f, "DISTINCT ON ({}) ", Sep(exprs, ", "))?,
            None => {}
        }
        write!(f, "{}", Sep(&self.items, ", "))?;
        if !self.from.is_empty() {
            write!(f, " FROM {}", Sep(&self.from, ", "))?;
        }
        if let Some(filter) = &self.filter {
            write!(f, " WHERE {filter}")?;
        }
        match &self.group_by {
            Some(GroupBy::Exprs(exprs)) => write!(f, " GROUP BY {}", Sep(exprs, ", "))?,
            Some(GroupBy::Rollup(exprs)) => write!(f, " GROUP BY ROLLUP ({})", Sep(exprs, ", "))?,
            None => {}
        }
        if let Some(having) = &self.having {
            write!(f, " HAVING {having}")?;
        }
        Ok(())
    }
}

impl Display for SelectItem {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)?;
        if let Some(alias) = &self.alias {
            write!(f, " AS {alias}")?;
        }
        Ok(())
    }
}

impl Display for TableRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.factor)?;
        for join in &self.joins {
            write!(f, " {join}")?;
        }
        Ok(())
    }
}

impl Display for TableFactor {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let (alias, sample) = match self {
            Self::Table {
                name,
                alias,
                sample,
            } => {
                write!(f, "{name}")?;
                (alias, sample.as_ref())
            }
            Self::Subquery { query, alias } => {
                write!(f, "({query})")?;
                (alias, None)
            }
            Self::Function { name, args, alias } => {
                write!(f, "{name}({})", Sep(args, ", "))?;
                (alias, None)
            }
        };
        if let Some(alias) = alias {
            write!(f, " {alias}")?;
        }
        if let Some(sample) = sample {
            write!(f, " {sample}")?;
        }
        Ok(())
    }
}

impl Display for TableName {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Object(name) => write!(f, "{name}"),
            Self::Path(path) => quote(f, path, '\''),
        }
    }
}

impl Display for ObjectName {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", Sep(&self.0, "."))
    }
}

impl Display for TableAlias {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "AS {}", self.name)?;
        if !self.columns.is_empty() {
            write!(f, "({})", Sep(&self.columns, ", "))?;
        }
        Ok(())
    }
}

impl Display for Join {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.kind {
            Some(JoinType::Inner) => f.write_str("INNER ")?,
            Some(JoinType::Left) => f.write_str("LEFT ")?,
            Some(JoinType::Right) => f.write_str("RIGHT ")?,
            Some(JoinType::Full) => f.write_str("FULL ")?,
            None => {}
        }
        write!(f, "JOIN {} ", self.table)?;
        match &self.constraint {
            JoinConstraint::On(expr) => write!(f, "ON {expr}"),
            JoinConstraint::Using(columns) => write!(f, "USING ({})", Sep(columns, ", ")),
        }
    }
}

impl Display for Sample {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("TABLESAMPLE ")?;
        match self.method {
            Some(SampleMethod::Bernoulli) => f.write_str("BERNOULLI ")?,
            Some(SampleMethod::System) => f.write_str("SYSTEM ")?,
            None => {}
        }
        let unit = match self.count.unit {
            SampleUnit::Percent => "PERCENT",
            SampleUnit::Rows => "ROWS",
        };
        write!(f, "({} {unit})", self.count.value)?;
        if let Some(seed) = self.seed {
            write!(f, " REPEATABLE ({seed})")?;
        }
        Ok(())
    }
}

impl Display for Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let not = |negated: bool| if negated { "NOT " } else { "" };
        match self {
            Self::Column(parts) => write!(f, "{}", Sep(parts, ".")),
            Self::Lit(lit) => write!(f, "{lit}"),
            Self::Star { qualifier, exclude } => {
                if let Some(qualifier) = qualifier {
                    write!(f, "{qualifier}.")?;
                }
                f.write_char('*')?;
                if !exclude.is_empty() {
                    write!(f, " EXCLUDE ({})", Sep(exclude, ", "))?;
                }
                Ok(())
            }
            Self::Unary { op, expr } => {
                let op = match op {
                    UnaryOp::Neg => "-",
                    UnaryOp::Plus => "+",
                    UnaryOp::Not => "NOT ",
                };
                write!(f, "({op}{expr})")
            }
            Self::Binary { op, left, right } => write!(f, "({left} {op} {right})"),
            Self::IsNull { expr, negated } => write!(f, "({expr} IS {}NULL)", not(*negated)),
            Self::In { expr, negated, rhs } => {
                write!(f, "({expr} {}IN ", not(*negated))?;
                match rhs {
                    InExpression::List(list) => write!(f, "({}))", Sep(list, ", ")),
                    InExpression::Select(query) => write!(f, "({query}))"),
                }
            }
            Self::Between {
                expr,
                negated,
                low,
                high,
            } => write!(f, "({expr} {}BETWEEN {low} AND {high})", not(*negated)),
            Self::Like {
                expr,
                negated,
                case_insensitive,
                pattern,
            } => {
                let like = if *case_insensitive { "ILIKE" } else { "LIKE" };
                write!(f, "({expr} {}{like} {pattern})", not(*negated))
            }
            Self::Exists(query) => write!(f, "EXISTS ({query})"),
            Self::Subquery(query) => write!(f, "({query})"),
            Self::Case { whens, else_ } => {
                f.write_str("CASE")?;
                for (when, then) in whens {
                    write!(f, " WHEN {when} THEN {then}")?;
                }
                write!(f, " ELSE {else_} END")
            }
            Self::Cast { expr, ty, try_ } => {
                let cast = if *try_ { "TRY_CAST" } else { "CAST" };
                write!(f, "{cast}({expr} AS {ty})")
            }
            Self::Date(value) => {
                f.write_str("DATE ")?;
                quote(f, value, '\'')
            }
            Self::Interval { value, unit } => {
                f.write_str("INTERVAL ")?;
                quote(f, value, '\'')?;
                if let Some(unit) = unit {
                    write!(f, " {unit}")?;
                }
                Ok(())
            }
            Self::Function(function) => write!(f, "{function}"),
            Self::Coalesce(args) => write!(f, "COALESCE({})", Sep(args, ", ")),
            Self::Extract { part, expr } => write!(f, "EXTRACT({part} FROM {expr})"),
            Self::Substring { expr, from, for_ } => {
                write!(f, "SUBSTRING({expr}")?;
                if let Some(from) = from {
                    write!(f, " FROM {from}")?;
                }
                if let Some(for_) = for_ {
                    write!(f, " FOR {for_}")?;
                }
                f.write_char(')')
            }
            Self::Subscript { expr, index } => write!(f, "({expr}[{index}])"),
            Self::List(items) => write!(f, "[{}]", Sep(items, ", ")),
            Self::Struct(fields) => {
                f.write_char('{')?;
                for (i, (name, value)) in fields.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{name}: {value}")?;
                }
                f.write_char('}')
            }
        }
    }
}

impl Display for Function {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.name)?;
        if let Some(quantifier) = self.quantifier {
            write!(f, "{quantifier} ")?;
        }
        write!(f, "{})", Sep(&self.args, ", "))?;
        if let Some(over) = &self.over {
            write!(f, " OVER ({over})")?;
        }
        Ok(())
    }
}

impl Display for FunctionArg {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Named(name, value) => write!(f, "{name} => {value}"),
            Self::Positional(value) => write!(f, "{value}"),
        }
    }
}

impl Display for Window {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut parts = vec![];
        if !self.partition_by.is_empty() {
            parts.push(format!("PARTITION BY {}", Sep(&self.partition_by, ", ")));
        }
        if !self.order_by.is_empty() {
            parts.push(format!("ORDER BY {}", Sep(&self.order_by, ", ")));
        }
        if let Some(frame) = &self.frame {
            parts.push(frame.to_string());
        }
        f.write_str(&parts.join(" "))
    }
}

impl Display for Frame {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self.units {
            FrameUnits::Rows => "ROWS ",
            FrameUnits::Range => "RANGE ",
        })?;
        match &self.extent {
            FrameExtent::Single(bound) => write!(f, "{bound}"),
            FrameExtent::Between(start, end) => write!(f, "BETWEEN {start} AND {end}"),
        }
    }
}

impl Display for FrameBound {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unbounded(direction) => write!(f, "UNBOUNDED {direction}"),
            Self::CurrentRow => f.write_str("CURRENT ROW"),
            Self::Offset(offset, direction) => write!(f, "{offset} {direction}"),
        }
    }
}

impl Display for FrameDirection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Preceding => "PRECEDING",
            Self::Following => "FOLLOWING",
        })
    }
}

impl Display for DatePart {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Year => "YEAR",
            Self::Quarter => "QUARTER",
            Self::Month => "MONTH",
            Self::Week => "WEEK",
            Self::Day => "DAY",
            Self::Hour => "HOUR",
            Self::Minute => "MINUTE",
            Self::Second => "SECOND",
            Self::Millisecond => "MILLISECOND",
            Self::Microsecond => "MICROSECOND",
        })
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let modifiers = |f: &mut Formatter<'_>, name: &str, mods: &Option<Vec<u64>>| {
            f.write_str(name)?;
            match mods {
                Some(mods) => write!(f, "({})", Sep(mods, ", ")),
                None => Ok(()),
            }
        };
        match self {
            Self::Boolean => f.write_str("BOOLEAN"),
            Self::TinyInt => f.write_str("TINYINT"),
            Self::SmallInt => f.write_str("SMALLINT"),
            Self::Int => f.write_str("INT"),
            Self::BigInt => f.write_str("BIGINT"),
            Self::UInt16 => f.write_str("UINT16"),
            Self::UInt32 => f.write_str("UINT32"),
            Self::UInt64 => f.write_str("UINT64"),
            Self::Real => f.write_str("REAL"),
            Self::Double => f.write_str("DOUBLE"),
            Self::Float(precision) => write!(f, "FLOAT({precision})"),
            Self::Decimal(mods) => write!(f, "DECIMAL({})", Sep(mods, ", ")),
            Self::Varchar(mods) => modifiers(f, "VARCHAR", mods),
            Self::Binary => f.write_str("BINARY"),
            Self::Date => f.write_str("DATE"),
            Self::Time => f.write_str("TIME"),
            Self::Timestamp(mods) => modifiers(f, "TIMESTAMP", mods),
            Self::Interval => f.write_str("INTERVAL"),
            Self::Array(element, size) => {
                write!(f, "{element}[")?;
                if let Some(size) = size {
                    write!(f, "{size}")?;
                }
                f.write_char(']')
            }
        }
    }
}

impl Display for Ident {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.quoted {
            quote(f, &self.value, '"')
        } else {
            f.write_str(&self.value)
        }
    }
}

impl Display for Lit {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => f.write_str("NULL"),
            Self::Bool(true) => f.write_str("TRUE"),
            Self::Bool(false) => f.write_str("FALSE"),
            Self::Int(i) => write!(f, "{i}"),
            // Debug keeps the decimal point, so `1.0` doesn't re-parse as an integer.
            Self::Float(x) => write!(f, "{x:?}"),
            Self::String(s) => quote(f, s, '\''),
        }
    }
}

impl Display for BinaryOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::IntDiv => "//",
            Self::Mod => "%",
            Self::Eq => "=",
            Self::NotEq => "<>",
            Self::Lt => "<",
            Self::LtEq => "<=",
            Self::Gt => ">",
            Self::GtEq => ">=",
            Self::And => "AND",
            Self::Or => "OR",
        })
    }
}
