use daft_logical_plan::LogicalPlanBuilder;
use eyre::{bail, WrapErr};
use spark_connect::read::ReadType;
use tracing::warn;

mod data_source;

pub async fn read(read: spark_connect::Read) -> eyre::Result<LogicalPlanBuilder> {
    let spark_connect::Read {
        is_streaming,
        read_type,
    } = read;

    warn!("Ignoring is_streaming: {is_streaming}");

    let Some(read_type) = read_type else {
        bail!("Read type is required");
    };

    let builder = match read_type {
        ReadType::NamedTable(table) => {
            let name = table.unparsed_identifier;
            bail!("Tried to read from table {name} but it is not yet implemented. Try to read from a path instead.");
        }
        ReadType::DataSource(source) => data_source::data_source(source)
            .await
            .wrap_err("Failed to create data source"),
    }?;

    Ok(builder)
}
