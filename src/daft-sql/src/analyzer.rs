use common_error::DaftError;
use common_error::DaftResult;
use daft_plan::LogicalPlanBuilder;
use sqlparser::ast::*;

use crate::catalog::Catalog;

#[macro_export]
macro_rules! not_supported {
    ($($arg:tt)*) => {{
        let msg = format!($($arg)*);
        Err(DaftError::InternalError(msg.to_string()))
    }}
}

/// Context will hold common table expressions (CTEs) and other scoped context information.
pub struct Context {}

/// The `Analyzer` is responsible for,
///
/// 1. Normalization
/// 2. Semantic analysis
/// 3. Logical transformation
pub struct Analyzer {
    catalog: Catalog,
}

impl Analyzer {
    pub fn new(catalog: Catalog) -> Self {
        Analyzer { catalog }
    }

    pub fn analyze(&mut self, statement: Statement) -> DaftResult<LogicalPlanBuilder> {
        if let Statement::Query(query) = statement {
            self.analyze_query(*query)
        } else {
            not_supported!("Statement not supported: {:?}", statement)
        }
    }

    fn analyze_query(&mut self, query: Query) -> DaftResult<LogicalPlanBuilder> {
        // initialize context for this query
        let context = Context {};

        // add CTEs to context
        if let Some(with) = query.with {
            self.analyze_with(with, &context)?;
        }

        // analyze the query body
        let builder = match *(query.body) {
            SetExpr::Select(select) => self.analyze_select(*select, context)?,
            _ => return not_supported!("Query not supported: {:?}", query.body),
        };

        // apply limit and offset
        let builder = self.analyze_fetch(builder, query.limit, query.offset)?;

        // return a query action with the prepared builder
        Ok(builder)
    }

    /// TODO push all defined tables into analyzer state.
    fn analyze_with(&mut self, _with: With, _context: &Context) -> DaftResult<()> {
        not_supported!("WITH not supported")
    }

    /// The SELECT-FROM-WHERE AST node.
    ///
    /// References
    ///  - https://docs.rs/sqlparser/latest/sqlparser/ast/struct.Select.html
    ///  - https://github.com/apache/datafusion/blob/main/datafusion/sql/src/select.rs#L50
    ///
    fn analyze_select(
        &mut self,
        select: Select,
        _context: Context,
    ) -> DaftResult<LogicalPlanBuilder> {
        // UNSUPPORTED FEATURES
        if !select.cluster_by.is_empty() {
            return not_supported!("CLUSTER BY");
        }
        if !select.lateral_views.is_empty() {
            return not_supported!("LATERAL VIEWS");
        }
        if select.qualify.is_some() {
            return not_supported!("QUALIFY");
        }
        if select.top.is_some() {
            return not_supported!("TOP");
        }
        if !select.sort_by.is_empty() {
            return not_supported!("SORT BY");
        }

        // FROM
        let builder = self.analyze_tables(select.from, _context)?;

        // WHERE
        if select.selection.is_some() {
            return not_supported!("WHERE clause not supported");
        }

        // SELECT
        self.analyze_select_list(builder, select.projection)
    }

    /// Produce a relation from the tables.
    fn analyze_tables(
        &self,
        mut tables: Vec<TableWithJoins>,
        _context: Context,
    ) -> DaftResult<LogicalPlanBuilder> {
        match tables.len() {
            0 => not_supported!("SELECT without FROM not supported."),
            1 => self.analyze_table(tables.remove(0), _context),
            _ => not_supported!("CROSS JOIN not supported"),
        }
    }

    fn analyze_select_list(
        &mut self,
        builder: LogicalPlanBuilder,
        select: Vec<SelectItem>,
    ) -> DaftResult<LogicalPlanBuilder> {
        let mut had_star = false;
        for item in select {
            if had_star {
                // Consider EXCLUDE support
                return not_supported!("Multiple * in SELECT");
            }
            match item {
                SelectItem::Wildcard(_) => {
                    had_star = true;
                }
                SelectItem::UnnamedExpr(_) => {
                    return not_supported!("SELECT <expr> not supported");
                }
                SelectItem::ExprWithAlias { expr: _, alias: _ } => {
                    return not_supported!("SELECT <expr> AS <alias> not supported");
                }
                SelectItem::QualifiedWildcard(_, _) => {
                    return not_supported!("SELECT <table>.* not supported");
                }
            }
        }
        if had_star {
            Ok(builder)
        } else {
            not_supported!("SELECT <list> not supported, only SELECT *")
        }
    }

    fn analyze_table(
        &self,
        table: TableWithJoins,
        _context: Context,
    ) -> DaftResult<LogicalPlanBuilder> {
        if !table.joins.is_empty() {
            return not_supported!("JOIN is not supported");
        }
        self.analyze_relation(table.relation, _context)
    }

    fn analyze_relation(
        &self,
        relation: TableFactor,
        _context: Context,
    ) -> DaftResult<LogicalPlanBuilder> {
        match relation {
            TableFactor::Table { name, alias, .. } => {
                if alias.is_some() {
                    return not_supported!("<table> AS alias not supported");
                }
                let name = name.0.first().unwrap().value.to_string();
                let plan = self.catalog.get_table(&name);
                let plan = match plan {
                    Some(plan) => LogicalPlanBuilder::new(plan),
                    None => return not_supported!("Table not found: {}", name),
                };
                Ok(plan)
            }
            TableFactor::Derived { .. } => not_supported!("Derived table"),
            TableFactor::TableFunction { .. } => not_supported!("Table function"),
            TableFactor::Function { .. } => not_supported!("Function"),
            TableFactor::UNNEST { .. } => not_supported!("UNNEST"),
            TableFactor::JsonTable { .. } => not_supported!("JsonTable"),
            TableFactor::NestedJoin { .. } => not_supported!("NestedJoin"),
            TableFactor::Pivot { .. } => not_supported!("Pivot"),
            TableFactor::Unpivot { .. } => not_supported!("Unpivot"),
            TableFactor::MatchRecognize { .. } => not_supported!("MatchRecognize"),
        }
    }

    fn analyze_fetch(
        &mut self,
        builder: LogicalPlanBuilder,
        limit: Option<Expr>,
        _offset: Option<Offset>,
    ) -> DaftResult<LogicalPlanBuilder> {
        if limit.is_some() {
            return not_supported!("LIMIT is not supported");
        }
        Ok(builder)
    }
}
