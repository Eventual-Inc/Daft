pub mod read_csv;
pub mod read_parquet;
use std::{collections::HashMap, sync::Arc};

use daft_logical_plan::LogicalPlanBuilder;
use once_cell::sync::Lazy;
use read_csv::ReadCsvFunction;
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
    functions.add_fn("read_parquet", ReadParquetFunction);
    functions.add_fn("read_csv", ReadCsvFunction);
    #[cfg(feature = "python")]
    functions.add_fn("read_deltalake", ReadDeltalakeFunction);

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

pub(crate) trait SQLTableFunction: Send + Sync {
    fn plan(
        &self,
        planner: &SQLPlanner,
        args: &TableFunctionArgs,
    ) -> SQLPlannerResult<LogicalPlanBuilder>;
}

pub struct ReadDeltalakeFunction;

#[cfg(feature = "python")]
impl SQLTableFunction for ReadDeltalakeFunction {
    fn plan(
        &self,
        planner: &SQLPlanner,
        args: &TableFunctionArgs,
    ) -> SQLPlannerResult<LogicalPlanBuilder> {
        let (uri, io_config) = match args.args.as_slice() {
            [uri] => (uri, None),
            [uri, io_config] => {
                let args = planner.parse_function_args(&[io_config.clone()], &["io_config"], 0)?;
                let io_config = args.get_named("io_config").map(expr_to_iocfg).transpose()?;

                (uri, io_config)
            }
            _ => unsupported_sql_err!("Expected one or two arguments"),
        };
        let uri = planner.plan_function_arg(uri)?;

        let Some(uri) = uri.as_literal().and_then(|lit| lit.as_str()) else {
            unsupported_sql_err!("Expected a string literal for the first argument");
        };

        daft_scan::builder::delta_scan(uri, io_config, true).map_err(From::from)
    }
}

#[cfg(not(feature = "python"))]
impl SQLTableFunction for ReadDeltalakeFunction {
    fn plan(
        &self,
        planner: &SQLPlanner,
        args: &TableFunctionArgs,
    ) -> SQLPlannerResult<LogicalPlanBuilder> {
        unsupported_sql_err!("`read_deltalake` function is not supported. Enable the `python` feature to use this function.")
    }
}
