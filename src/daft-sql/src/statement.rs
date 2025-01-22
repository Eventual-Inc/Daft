use daft_logical_plan::LogicalPlanRef;
use sqlparser::ast;

use crate::{error::SQLPlannerResult, unsupported_sql_err, SQLPlanner};

/// Daft-SQL statement planning.
impl<'a> SQLPlanner<'a> {
    /// Generates a logical plan for an ast statement.
    pub(crate) fn plan_statement(
        &mut self,
        statement: &ast::Statement,
    ) -> SQLPlannerResult<LogicalPlanRef> {
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
            other => unsupported_sql_err!("{}", other),
        }
    }

    /// SELECT ...
    fn plan_select(&mut self, query: &ast::Query) -> SQLPlannerResult<LogicalPlanRef> {
        Ok(self.plan_query(query)?.build())
    }

    /// DESCRIBE <statement>
    fn plan_describe(
        &self,
        describe_alias: &ast::DescribeAlias,
        analyze: bool,
        verbose: bool,
        _statement: &ast::Statement,
        format: &Option<ast::AnalyzeFormat>,
    ) -> SQLPlannerResult<LogicalPlanRef> {
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
        unsupported_sql_err!("DESCRIBE <statement> is not supported")
    }

    /// DESCRIBE TABLE <table>
    fn plan_describe_table(
        &self,
        describe_alias: &ast::DescribeAlias,
        hive_format: &Option<ast::HiveDescribeFormat>,
        has_table_keyword: bool,
        table_name: &ast::ObjectName,
    ) -> SQLPlannerResult<LogicalPlanRef> {
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
        let rel = self.plan_relation_table(table_name)?;
        let res = rel.inner.describe()?.build();
        Ok(res)
    }
}
