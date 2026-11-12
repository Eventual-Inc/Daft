mod read_csv;
mod read_deltalake;
mod read_iceberg;
mod read_json;
mod read_parquet;

use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, LazyLock},
};

use common_error::DaftResult;
use daft_dsl::{Expr, ExprRef};
use daft_logical_plan::LogicalPlanBuilder;
use read_csv::ReadCsvFunction;
use read_deltalake::ReadDeltalakeFunction;
use read_iceberg::SqlReadIceberg;
use read_json::ReadJsonFunction;
use read_parquet::ReadParquetFunction;
use sqlparser::ast::TableFunctionArgs;

use crate::{
    error::{PlannerError, SQLPlannerResult},
    functions::SQLLiteral,
    invalid_operation_err,
    modules::config::expr_to_iocfg,
    planner::SQLPlanner,
    unsupported_sql_err,
};

pub(crate) static SQL_TABLE_FUNCTIONS: LazyLock<SQLTableFunctions> = LazyLock::new(|| {
    let mut functions = SQLTableFunctions::new();
    functions.add_fn("read_csv", ReadCsvFunction);
    functions.add_fn("read_deltalake", ReadDeltalakeFunction);
    functions.add_fn("read_iceberg", SqlReadIceberg);
    functions.add_fn("read_json", ReadJsonFunction);
    functions.add_fn("read_parquet", ReadParquetFunction);
    functions
});

/// TODO chore: cleanup table_provider module
/// TODO feat: use multimap for function variants.
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

impl SQLPlanner<'_> {
    pub(crate) fn plan_table_function(
        &self,
        fn_name: &str,
        args: &TableFunctionArgs,
    ) -> SQLPlannerResult<LogicalPlanBuilder> {
        let fns = &SQL_TABLE_FUNCTIONS;

        let Some(func) = fns.get(fn_name) else {
            unsupported_sql_err!("Function `{}` not found", fn_name);
        };

        let builder = func.plan(self, args)?;

        Ok(builder)
    }
}

// TODO chore: switch param order and rename to `to_logical_plan` for consistency with SQLFunction.
pub(crate) trait SQLTableFunction: Send + Sync {
    fn plan(
        &self,
        planner: &SQLPlanner,
        args: &TableFunctionArgs,
    ) -> SQLPlannerResult<LogicalPlanBuilder>;
}

// TODO feat: support assignment casts for function arguments
pub(crate) fn try_coerce_list<T: SQLLiteral>(expr: ExprRef) -> Result<Vec<T>, PlannerError> {
    match expr.as_ref() {
        Expr::List(items) => items.iter().map(T::from_expr).collect(),
        Expr::Literal(_) => Ok(vec![T::from_expr(&expr)?]),
        _ => invalid_operation_err!("Expected a scalar or list literal"),
    }
}

/// Drive an async future to completion from synchronous SQL planning.
///
/// Runs `future` on the shared IO runtime, blocking the calling (planner) thread until it
/// resolves. When the `python` feature is enabled the GIL is released for the duration of the
/// wait: planning is entered with the GIL held, so anything the future needs the GIL for (e.g.
/// pyo3-log forwarding a `log` record to Python) would otherwise deadlock.
pub(crate) fn block_on_io_runtime<F>(future: F) -> DaftResult<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let runtime = common_runtime::get_io_runtime(true);
    #[cfg(feature = "python")]
    {
        pyo3::Python::attach(|py| py.detach(|| runtime.block_within_async_context(future)))
    }
    #[cfg(not(feature = "python"))]
    {
        runtime.block_within_async_context(future)
    }
}
