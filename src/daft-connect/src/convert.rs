mod data_conversion;
mod expression;
mod formatting;
mod plan_conversion;
mod schema_conversion;

use std::{
    collections::HashMap,
    ops::{ControlFlow, Try},
    sync::Arc,
};

use common_daft_config::DaftExecutionConfig;
use daft_logical_plan::LogicalPlanRef;
use daft_table::Table;
pub use data_conversion::convert_data;
use eyre::Context;
pub use plan_conversion::to_logical_plan;
pub use schema_conversion::connect_schema;

pub fn run_local<T: Try>(
    logical_plan: &LogicalPlanRef,
    mut f: impl FnMut(&Table) -> T,
    default: impl FnOnce() -> T,
) -> eyre::Result<T> {
    let physical_plan = daft_local_plan::translate(logical_plan)?;
    let cfg = Arc::new(DaftExecutionConfig::default());
    let psets = HashMap::new();

    let stream = daft_local_execution::run_local(&physical_plan, psets, cfg, None)
        .wrap_err("running local execution")?;

    for elem in stream {
        let elem = elem?;
        let tables = elem.get_tables()?;

        for table in tables.as_slice() {
            if let ControlFlow::Break(x) = f(table).branch() {
                return Ok(T::from_residual(x));
            }
        }
    }

    Ok(default())
}
