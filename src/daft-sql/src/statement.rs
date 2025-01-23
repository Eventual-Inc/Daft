use daft_logical_plan::LogicalPlanBuilder;
use sqlparser::ast;

use crate::{error::SQLPlannerResult, unsupported_sql_err, SQLPlanner};

/// Daft-SQL statement planning.
impl<'a> SQLPlanner<'a> {
    /// Generates a logical plan for an ast statement.
    pub(crate) fn plan_statement(
        &mut self,
        statement: &ast::Statement,
    ) -> SQLPlannerResult<LogicalPlanBuilder> {
        match statement {
            ast::Statement::Query(query) => self.plan_select(query),
            ast::Statement::Explain {
                describe_alias,
                analyze,
                verbose,
                statement,
                format,
            } => self.plan_describe(describe_alias, *analyze, *verbose, statement, format),
            ast::Statement::ExplainTable {
                describe_alias,
                hive_format,
                has_table_keyword,
                table_name,
            } => self.plan_describe_table(
                describe_alias,
                hive_format,
                *has_table_keyword,
                table_name,
            ),
            other => unsupported_sql_err!("unsupported statement, {}", other),
        }
    }

    /// SELECT ...
    fn plan_select(&mut self, query: &ast::Query) -> SQLPlannerResult<LogicalPlanBuilder> {
        self.plan_query(query)
    }

    /// DESCRIBE <statement>
    fn plan_describe(
        &mut self,
        describe_alias: &ast::DescribeAlias,
        analyze: bool,
        verbose: bool,
        statement: &ast::Statement,
        format: &Option<ast::AnalyzeFormat>,
    ) -> SQLPlannerResult<LogicalPlanBuilder> {
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
        Ok(self.plan_statement(statement)?.describe()?)
    }

    /// DESCRIBE <table>
    fn plan_describe_table(
        &self,
        describe_alias: &ast::DescribeAlias,
        hive_format: &Option<ast::HiveDescribeFormat>,
        has_table_keyword: bool,
        table_name: &ast::ObjectName,
    ) -> SQLPlannerResult<LogicalPlanBuilder> {
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
        Ok(self.plan_relation_table(table_name)?.inner.describe()?)
    }
}
