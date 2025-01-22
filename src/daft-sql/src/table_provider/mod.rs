mod read_csv;
mod read_deltalake;
mod read_iceberg;
mod read_json;
mod read_parquet;

use std::{collections::HashMap, sync::Arc};

use daft_logical_plan::LogicalPlanBuilder;
use once_cell::sync::Lazy;
use read_csv::ReadCsvFunction;
use read_deltalake::ReadDeltalakeFunction;
use read_iceberg::SqlReadIceberg;
use read_json::ReadJsonFunction;
use read_parquet::ReadParquetFunction;
use sqlparser::ast::TableFunctionArgs;

use crate::{
    error::SQLPlannerResult,
    modules::config::expr_to_iocfg,
    planner::{Relation, SQLPlanner},
    unsupported_sql_err,
};

pub(crate) static SQL_TABLE_FUNCTIONS: Lazy<SQLTableFunctions> = Lazy::new(|| {
    let mut functions = SQLTableFunctions::new();
    functions.add_fn("read_csv", ReadCsvFunction);
    functions.add_fn("read_deltalake", ReadDeltalakeFunction);
    functions.add_fn("read_iceberg", SqlReadIceberg);
    functions.add_fn("read_json", ReadJsonFunction);
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

impl<'a> SQLPlanner<'a> {
    pub(crate) fn plan_table_function(
        &self,
        fn_name: &str,
        args: &TableFunctionArgs,
    ) -> SQLPlannerResult<Relation> {
        let fns = &SQL_TABLE_FUNCTIONS;

        let Some(func) = fns.get(fn_name) else {
            unsupported_sql_err!("Function `{}` not found", fn_name);
        };

        let builder = func.plan(self, args)?;

        Ok(Relation::new(builder, fn_name.to_string()))
    }
}

// nit cleanup: switch param order and rename to `to_logical_plan` for consistency with SQLFunction.
pub(crate) trait SQLTableFunction: Send + Sync {
    fn plan(
        &self,
        planner: &SQLPlanner,
        args: &TableFunctionArgs,
    ) -> SQLPlannerResult<LogicalPlanBuilder>;
}
