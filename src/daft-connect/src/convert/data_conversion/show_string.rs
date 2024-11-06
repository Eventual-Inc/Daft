use daft_core::prelude::Series;
use daft_schema::prelude::Schema;
use daft_table::Table;
use eyre::{ensure, eyre, Context};
use spark_connect::ShowString;

use crate::{
    command::ConcreteDataChannel,
    convert::{plan_conversion::to_logical_plan, run_local},
};

pub fn show_string(
    show_string: ShowString,
    channel: &mut impl ConcreteDataChannel,
) -> eyre::Result<()> {
    let ShowString {
        input,
        num_rows,
        truncate,
        vertical,
    } = show_string;

    ensure!(num_rows > 0, "num_rows must be positive, got {num_rows}");
    ensure!(truncate > 0, "truncate must be positive, got {truncate}");
    ensure!(!vertical, "vertical is not yet supported");

    let input = *input.ok_or_else(|| eyre!("input is None"))?;

    let logical_plan = to_logical_plan(input)?.build();

    run_local(
        &logical_plan,
        |table| -> eyre::Result<()> {
            let display = format!("{table}");

            let arrow_array: arrow2::array::Utf8Array<i64> =
                std::iter::once(display.as_str()).map(Some).collect();

            let singleton_series = Series::try_from((
                "show_string",
                Box::new(arrow_array) as Box<dyn arrow2::array::Array>,
            ))
            .wrap_err("creating singleton series")?;

            let singleton_table = Table::new_with_size(
                Schema::new(vec![singleton_series.field().clone()])?,
                vec![singleton_series],
                1,
            )?;

            channel.send_table(&singleton_table)?;

            Ok(())
        },
        || Ok(()),
    )??;

    Ok(())
}
