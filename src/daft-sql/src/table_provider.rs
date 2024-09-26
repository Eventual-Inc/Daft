use std::{collections::HashMap, sync::Arc};

use daft_plan::LogicalPlanBuilder;
use once_cell::sync::Lazy;
use sqlparser::ast::{TableAlias, TableFunctionArgs};

use crate::{
    error::SQLPlannerResult,
    planner::{Relation, SQLPlanner},
    unsupported_sql_err,
};

pub(crate) static SQL_TABLE_FUNCTIONS: Lazy<SQLTableFunctions> = Lazy::new(|| {
    let mut functions = SQLTableFunctions::new();
    functions.add_fn("read_parquet", ReadParquetFunction);

    functions
});

/// TODOs
///   - Use multimap for function variants.
///   - Add more functions..
pub struct SQLTableFunctions {
    pub(crate) map: HashMap<String, Arc<dyn SQLTableFunction>>,
}

impl SQLTableFunctions {
    /// Create a new [SQLFunctions] instance.
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
    /// Add a [FunctionExpr] to the [SQLFunctions] instance.
    pub(crate) fn add_fn<F: SQLTableFunction + 'static>(&mut self, name: &str, func: F) {
        self.map.insert(name.to_string(), Arc::new(func));
    }

    /// Get a function by name from the [SQLFunctions] instance.
    pub(crate) fn get(&self, name: &str) -> Option<&Arc<dyn SQLTableFunction>> {
        self.map.get(name)
    }
}

impl SQLPlanner {
    pub(crate) fn plan_table_function(
        &self,
        fn_name: &str,
        args: &TableFunctionArgs,
        alias: &Option<TableAlias>,
    ) -> SQLPlannerResult<Relation> {
        let fns = &SQL_TABLE_FUNCTIONS;

        let Some(func) = fns.get(fn_name) else {
            unsupported_sql_err!("Function `{}` not found", fn_name);
        };

        let builder = func.plan(self, args)?;
        let name = alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| fn_name.to_string());

        Ok(Relation::new(builder, name))
    }
}

pub(crate) trait SQLTableFunction: Send + Sync {
    fn plan(
        &self,
        planner: &SQLPlanner,
        args: &TableFunctionArgs,
    ) -> SQLPlannerResult<LogicalPlanBuilder>;
}

struct ReadParquetFunction;

impl SQLTableFunction for ReadParquetFunction {
    fn plan(
        &self,
        planner: &SQLPlanner,
        args: &TableFunctionArgs,
    ) -> SQLPlannerResult<LogicalPlanBuilder> {
        let uri = planner.plan_function_arg(&args.args[0])?;
        let Some(uri) = uri.as_literal().and_then(|lit| lit.as_str()) else {
            unsupported_sql_err!(
                "Expected a string literal as the first argument to `read_parquet`"
            );
        };
        LogicalPlanBuilder::parquet_scan(uri, None, None, true).map_err(|e| e.into())
    }
}
