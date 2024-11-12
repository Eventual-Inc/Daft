mod data_conversion;
mod expression;
mod formatting;
mod plan_conversion;
mod schema_conversion;

use std::{collections::HashMap, pin::Pin, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use daft_logical_plan::LogicalPlanRef;
use daft_table::Table;
pub use data_conversion::convert_data;
use futures::{stream, Stream, StreamExt};
pub use schema_conversion::connect_schema;

pub fn run_local(
    logical_plan: &LogicalPlanRef,
) -> DaftResult<impl Stream<Item = DaftResult<Table>>> {
    let physical_plan = daft_local_plan::translate(logical_plan)?;
    let cfg = Arc::new(DaftExecutionConfig::default());
    let psets = HashMap::new();

    let stream = daft_local_execution::run_local(&physical_plan, psets, cfg, None)?;

    let stream = stream
        .map(|partition| match partition {
            Ok(partition) => partition.get_tables().map_err(DaftError::from),
            Err(err) => Err(err),
        })
        .flat_map(|tables| match tables {
            Ok(tables) => {
                let tables = Arc::unwrap_or_clone(tables);

                let tables = tables.into_iter().map(Ok);
                let stream: Pin<Box<dyn Stream<Item = DaftResult<Table>>>> =
                    Box::pin(stream::iter(tables));

                stream
            }
            Err(err) => Box::pin(stream::once(async { Err(err) })),
        });

    Ok(stream)
}
