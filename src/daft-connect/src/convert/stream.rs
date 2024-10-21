use std::{collections::HashMap, fmt::Display, sync::Arc};

use eyre::{bail, ensure, eyre, Context};
use common_daft_config::DaftExecutionConfig;
use daft_core::series::Series;
use daft_plan::LogicalPlanRef;
use daft_schema::schema::Schema;
use daft_table::Table;
use spark_connect::{
    execute_plan_response::{ArrowBatch, ResponseType},
    relation::RelType,
    ExecutePlanResponse, Relation, ShowString,
};
use uuid::Uuid;

use crate::convert::{fmt::TopLevelDisplay, logical_plan::to_logical_plan, to_daft_stream};

pub fn parse_top_level(
    plan: Relation,
) -> eyre::Result<impl Iterator<Item = eyre::Result<LogicalPlanRef>>> {
    let rel_type = plan.rel_type.ok_or_else(|| eyre!("rel_type is None"))?;

    match rel_type {
        RelType::ShowString(input) => show_string(*input).wrap_err("parsing ShowString"),
        other => Err(eyre!(
            "Unsupported top-level relation: {}",
            TopLevelDisplay::new(&other)
        )),
    }
}

pub fn show_string(show_string: ShowString) -> eyre::Result<LogicalPlanRef> {
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

    let physical_plan = daft_physical_plan::translate(&logical_plan)?;

    let cfg = Arc::new(DaftExecutionConfig::default());
    let psets = HashMap::new();

    let mut data = Vec::new();
    let options = arrow2::io::ipc::write::WriteOptions { compression: None };
    let mut writer = arrow2::io::ipc::write::StreamWriter::new(&mut data, options);

    for (i, elem) in daft_local_execution::run::run_local(&physical_plan, psets, cfg, None)
        .wrap_err("running local execution")?
        .enumerate()
    {
        let elem = elem.wrap_err_with(|| format!("error in partition {i}"))?;
        let tables = elem.get_tables().unwrap();

        let [table] = tables.as_slice() else {
            bail!("expected 1 table, got {} tables. It is a work in progress to support multiple tables", tables.len());
        };

        let display = format!("{table}");

        let arrow_array: arrow2::array::Utf8Array = [display.as_str()].iter().map(Some).collect();
        let singleton_series = Series::try_from((
            "show_string",
            Box::new(arrow_array) as Box<dyn arrow2::array::Array>,
        ))?;

        let singleton_table = Table::new_with_size(
            Schema::new(vec![singleton_series.field().clone()])?,
            vec![singleton_series],
            1,
        )?;

        write_table_to_arrow(&mut writer, &singleton_table);
    }

    Ok(())
}

fn write_table_to_arrow(
    writer: &mut arrow2::io::ipc::write::StreamWriter<&mut Vec<u8>>,
    table: &Table,
) {
    let schema = table.schema.to_arrow().unwrap();
    writer.start(&schema, None).unwrap();

    let arrays = table.get_inner_arrow_arrays();
    let chunk = arrow2::chunk::Chunk::new(arrays);
    writer.write(&chunk, None).unwrap();
}

trait Encoder {
    fn create_batch(&self, row_count: i64, data: Vec<u8>);
}

fn create_response(
    session_id: &str,
    server_side_session_id: &str,
    operation_id: &str,
    data: Vec<u8>,
) -> ExecutePlanResponse {
    let response_type = ResponseType::ArrowBatch(ArrowBatch {
        row_count: 10i64,
        data,
        start_offset: None,
    });

    ExecutePlanResponse {
        session_id: session_id.to_string(),
        server_side_session_id: server_side_session_id.to_string(),
        operation_id: operation_id.to_string(),
        response_id: Uuid::new_v4().to_string(),
        metrics: None,
        observed_metrics: vec![],
        schema: None,
        response_type: Some(response_type),
    }
}

pub fn to_daft_stream(plan: spark_connect::plan::Plan) -> eyre::Result<daft_dsl::LogicalPlan> {
    to_daft_stream(plan).map_err(|e| eyre!(e))
}
