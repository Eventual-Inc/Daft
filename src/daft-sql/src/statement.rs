use daft_catalog::Identifier;
use daft_logical_plan::{LogicalPlanBuilder, LogicalPlanRef};
use sqlparser::ast;

use crate::{SQLPlanner, error::SQLPlannerResult, unsupported_sql_err};

/// Top-level planning structure
#[derive(Clone)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub enum Statement {
    /// select .. from
    Select(Select),
    /// set a session variable
    Set(Set),
    /// list tables in a catalog
    ShowTables(ShowTables),
    /// use a catalog and optional namespace
    Use(Use),
}

/// SELECT ...
pub type Select = LogicalPlanRef;

/// SET <option> [TO] <value>
#[derive(Clone)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Set {
    pub option: String,
    pub value: String,
}

/// SHOW TABLES [ {FROM|IN} <catalog> ] [ LIKE <pattern> ]
#[derive(Clone)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct ShowTables {
    pub catalog: Option<String>,
    pub pattern: Option<String>,
}

/// USE <catalog> [. <namespace>]
#[derive(Clone)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Use {
    pub catalog: String,
    pub namespace: Option<Identifier>,
}

/// Daft-SQL statement planning.
impl SQLPlanner<'_> {
    /// Generates a logical plan for an ast statement.
    pub(crate) fn plan_statement(
        &mut self,
        statement: &ast::Statement,
    ) -> SQLPlannerResult<Statement> {
        match statement {
            ast::Statement::Query(query) => self.plan_select(query).map(Statement::Select),
            ast::Statement::Explain {
                describe_alias,
                analyze,
                verbose,
                statement,
                format,
            } => self.plan_describe(describe_alias, *analyze, *verbose, statement, *format),
            ast::Statement::ExplainTable {
                describe_alias,
                hive_format,
                has_table_keyword,
                table_name,
            } => self.plan_describe_table(
                describe_alias,
                *hive_format,
                *has_table_keyword,
                table_name,
            ),
            ast::Statement::SetVariable { .. } => {
                todo!("set_variable")
            }
            ast::Statement::ShowTables {
                extended,
                full,
                db_name,
                filter,
            } => self.plan_show_tables(*extended, *full, db_name.as_ref(), filter.as_ref()),
            ast::Statement::Use(use_) => self.plan_use(use_),
            other => unsupported_sql_err!("unsupported statement, {}", other),
        }
    }

    /// SELECT ...
    fn plan_select(&mut self, query: &ast::Query) -> SQLPlannerResult<Select> {
        Ok(self.plan_query(query)?.build())
    }

    /// DESCRIBE <statement>
    fn plan_describe(
        &mut self,
        describe_alias: &ast::DescribeAlias,
        analyze: bool,
        verbose: bool,
        statement: &ast::Statement,
        format: Option<ast::AnalyzeFormat>,
    ) -> SQLPlannerResult<Statement> {
        // err on `DESC | EXPLAIN`
        if *describe_alias != ast::DescribeAlias::Describe {
            unsupported_sql_err!(
                "{} statement is not supported, did you mean DESCRIBE?",
                describe_alias
            )
        }
        // err on DESCRIBE ( options.. )
        if analyze || verbose || format.is_some() {
            unsupported_sql_err!("DESCRIBE ( options.. ) is not supported")
        }
        // plan statement and .describe()
        if let ast::Statement::Query(query) = statement {
            let select = self.plan_select(query)?;
            let describe = LogicalPlanBuilder::from(select).describe()?;
            Ok(Statement::Select(describe.build()))
        } else {
            unsupported_sql_err!("DESCRIBE currently only supports SELECT statements")
        }
    }

    /// DESCRIBE <table>
    fn plan_describe_table(
        &self,
        describe_alias: &ast::DescribeAlias,
        hive_format: Option<ast::HiveDescribeFormat>,
        has_table_keyword: bool,
        table_name: &ast::ObjectName,
    ) -> SQLPlannerResult<Statement> {
        // err on `DESC | EXPLAIN`
        if *describe_alias != ast::DescribeAlias::Describe {
            unsupported_sql_err!(
                "{} statement is not supported, did you mean DESCRIBE?",
                describe_alias
            )
        }
        // err on `DESCRIBE [FORMATTED|EXTENDED]`
        if let Some(hive_format) = hive_format {
            unsupported_sql_err!("DESCRIBE modifier '{}' is not supported", hive_format)
        }
        // err on `DESCRIBE TABLE`
        if has_table_keyword {
            unsupported_sql_err!("DESCRIBE TABLE is not supported, did you mean DESCRIBE?")
        }
        // resolve table and .describe()
        let table = self.plan_relation_table(table_name)?;
        let describe = table.describe()?;
        Ok(Statement::Select(describe.build()))
    }

    #[allow(dead_code)]
    fn plan_set(&self, _: &ast::SetConfigValue) -> SQLPlannerResult<Statement> {
        unsupported_sql_err!("SET statement is not yet supported.")
    }

    fn plan_show_tables(
        &self,
        extended: bool,
        full: bool,
        catalog: Option<&ast::Ident>,
        pattern: Option<&ast::ShowStatementFilter>,
    ) -> SQLPlannerResult<Statement> {
        if extended {
            unsupported_sql_err!("SHOW EXTENDED is not supported.")
        }
        if full {
            unsupported_sql_err!("SHOW FULL is not supported.")
        }
        let catalog = match catalog {
            Some(ident) => {
                if matches!(ident.quote_style, Some('\'')) {
                    unsupported_sql_err!(
                        "Expected catalog identifier, but received a string: {}",
                        ident
                    )
                }
                Some(ident.value.clone())
            }
            None => None,
        };
        let pattern = match pattern {
            Some(ast::ShowStatementFilter::Like(pattern)) => Some(pattern.clone()),
            Some(ast::ShowStatementFilter::Where(_)) => {
                unsupported_sql_err!("SHOW TABLES WHERE is not supported.")
            }
            Some(ast::ShowStatementFilter::ILike(_)) => {
                unsupported_sql_err!("SHOW TABLES ILIKE is not supported.")
            }
            None => None,
        };
        Ok(Statement::ShowTables(ShowTables { catalog, pattern }))
    }

    fn plan_use(&self, use_: &ast::Use) -> SQLPlannerResult<Statement> {
        if let ast::Use::Object(name) = use_ {
            let idents = &name.0;
            let catalog = idents[0].value.clone();
            let namespace = match idents.len() {
                1 => None,
                _ => Some(Identifier::try_new(
                    idents[1..].iter().map(|ident| &ident.value),
                )?),
            };
            return Ok(Statement::Use(Use { catalog, namespace }));
        }
        unsupported_sql_err!("Expected `USE <catalog>` or USE <catalog>.<namespace>")
    }
}

#[cfg(test)]
mod test {
    use daft_session::Session;
    use sqlparser::{dialect::GenericDialect, parser::Parser};

    use super::*;

    fn parse_sql(sql: &str) -> ast::Statement {
        let dialect = GenericDialect {};
        let mut parsed = Parser::parse_sql(&dialect, sql).unwrap();
        assert_eq!(parsed.len(), 1);
        parsed.remove(0)
    }

    #[test]
    fn test_use_catalog() {
        let sql = "USE mycatalog";
        let statement = parse_sql(sql);
        let session = Session::default();
        let mut planner = SQLPlanner::new(&session);
        let plan = planner.plan_statement(&statement).unwrap();

        if let Statement::Use(use_stmt) = plan {
            assert_eq!(use_stmt.catalog, "mycatalog");
            assert_eq!(use_stmt.namespace, None);
        } else {
            panic!("Expected Use statement");
        }
    }

    #[test]
    fn test_use_catalog_with_namespace() -> SQLPlannerResult<()> {
        let sql = "USE mycatalog.myschema";
        let statement = parse_sql(sql);
        let session = Session::default();
        let mut planner = SQLPlanner::new(&session);
        let plan = planner.plan_statement(&statement).unwrap();

        if let Statement::Use(use_stmt) = plan {
            assert_eq!(use_stmt.catalog, "mycatalog");
            assert_eq!(
                use_stmt.namespace,
                Some(Identifier::try_new(vec!["myschema"])?)
            );
            Ok(())
        } else {
            panic!("Expected Use statement");
        }
    }

    #[test]
    fn test_use_catalog_with_multi_level_namespace() -> SQLPlannerResult<()> {
        let sql = "USE mycatalog.myschema.mysubschema";
        let statement = parse_sql(sql);
        let session = Session::default();

        let mut planner = SQLPlanner::new(&session);
        let plan = planner.plan_statement(&statement).unwrap();

        if let Statement::Use(use_stmt) = plan {
            assert_eq!(use_stmt.catalog, "mycatalog");
            assert_eq!(
                use_stmt.namespace,
                Some(Identifier::try_new(vec!["myschema", "mysubschema"])?)
            );
            Ok(())
        } else {
            panic!("Expected Use statement");
        }
    }
}
