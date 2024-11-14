use std::future::ready;

use daft_core::prelude::Series;
use daft_schema::prelude::Schema;
use daft_table::Table;
use eyre::{ensure, Context};
use futures::{stream, Stream};
use spark_connect::{ExecutePlanResponse, Range};

use crate::command::PlanIds;

pub fn range(
    range: Range,
    channel: &PlanIds,
) -> eyre::Result<impl Stream<Item = eyre::Result<ExecutePlanResponse>> + Unpin> {
    let Range {
        start,
        end,
        step,
        num_partitions,
    } = range;

    let start = start.unwrap_or(0);

    ensure!(num_partitions.is_none(), "num_partitions is not supported");

    let step = usize::try_from(step).wrap_err("step must be a positive integer")?;
    ensure!(step > 0, "step must be greater than 0");

    let arrow_array: arrow2::array::Int64Array = (start..end).step_by(step).map(Some).collect();
    let len = arrow_array.len();

    let singleton_series = Series::try_from((
        "range",
        Box::new(arrow_array) as Box<dyn arrow2::array::Array>,
    ))
    .wrap_err("creating singleton series")?;

    let singleton_table = Table::new_with_size(
        Schema::new(vec![singleton_series.field().clone()])?,
        vec![singleton_series],
        len,
    )?;

    let response = channel.gen_response(&singleton_table)?;

    Ok(stream::once(ready(Ok(response))))
}
