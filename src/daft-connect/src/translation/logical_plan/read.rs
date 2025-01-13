use daft_logical_plan::LogicalPlanBuilder;
use daft_scan::builder::{CsvScanBuilder, ParquetScanBuilder};
use eyre::{bail, ensure};
use spark_connect::read::ReadType;
use tracing::debug;

pub async fn read(read: spark_connect::Read) -> eyre::Result<LogicalPlanBuilder> {
    let spark_connect::Read {
        is_streaming,
        read_type,
    } = read;

    debug!("Ignoring is_streaming: {is_streaming}");

    let Some(read_type) = read_type else {
        bail!("Read type is required");
    };

    Ok(match read_type {
        ReadType::NamedTable(table) => {
            let name = table.unparsed_identifier;
            bail!("Tried to read from table {name} but it is not yet implemented. Try to read from a path instead.");
        }
        ReadType::DataSource(source) => data_source(source).await?,
    })
}

pub async fn data_source(
    data_source: spark_connect::read::DataSource,
) -> eyre::Result<LogicalPlanBuilder> {
    let spark_connect::read::DataSource {
        format,
        schema,
        options,
        paths,
        predicates,
    } = data_source;

    let Some(format) = format else {
        bail!("Format is required");
    };

    ensure!(!paths.is_empty(), "Paths are required");

    if let Some(schema) = schema {
        debug!("Ignoring schema: {schema:?}; not yet implemented");
    }

    if !options.is_empty() {
        debug!("Ignoring options: {options:?}; not yet implemented");
    }

    if !predicates.is_empty() {
        debug!("Ignoring predicates: {predicates:?}; not yet implemented");
    }

    Ok(match &*format {
        "parquet" => ParquetScanBuilder::new(paths).finish().await?,

        "csv" => CsvScanBuilder::new(paths).finish().await?,
        "json" => {
            // todo(completeness): implement json reading
            bail!("json reading is not yet implemented");
        }
        other => {
            bail!("Unsupported format: {other}; only parquet and csv are supported");
        }
    })
}
