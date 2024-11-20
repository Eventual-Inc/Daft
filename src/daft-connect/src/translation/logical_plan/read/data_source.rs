use daft_logical_plan::LogicalPlanBuilder;
use daft_scan::builder::ParquetScanBuilder;
use eyre::{bail, ensure, WrapErr};
use tracing::warn;

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

    if format != "parquet" {
        bail!("Unsupported format: {format}; only parquet is supported");
    }

    ensure!(!paths.is_empty(), "Paths are required");

    if let Some(schema) = schema {
        warn!("Ignoring schema: {schema:?}; not yet implemented");
    }

    if !options.is_empty() {
        warn!("Ignoring options: {options:?}; not yet implemented");
    }

    if !predicates.is_empty() {
        warn!("Ignoring predicates: {predicates:?}; not yet implemented");
    }

    let builder = ParquetScanBuilder::new(paths)
        .finish()
        .await
        .wrap_err("Failed to create parquet scan builder")?;

    Ok(builder)
}
