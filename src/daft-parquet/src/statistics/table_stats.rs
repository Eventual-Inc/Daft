use common_error::DaftResult;
use daft_core::schema::Schema;
use daft_stats::{ColumnRangeStatistics, TableStatistics};
use snafu::ResultExt;

use super::Wrap;

use indexmap::IndexMap;

pub fn row_group_metadata_to_table_stats(
    metadata: &crate::metadata::RowGroupMetaData,
    schema: &Schema,
) -> DaftResult<TableStatistics> {
    let mut columns = IndexMap::new();
    for col in metadata.columns() {
        let top_level_column_name = col
            .descriptor()
            .path_in_schema
            .first()
            .expect("Parquet schema should have at least one entry in path_in_schema");
        let daft_field = schema.get_field(top_level_column_name)?;
        let col_stats = if ColumnRangeStatistics::supports_dtype(&daft_field.dtype) {
            let stats = col
                .statistics()
                .transpose()
                .context(super::UnableToParseParquetColumnStatisticsSnafu)?;
            let col_stats: Option<Wrap<ColumnRangeStatistics>> =
                stats.and_then(|v| v.as_ref().try_into().ok());
            col_stats.unwrap_or(ColumnRangeStatistics::Missing.into())
        } else {
            ColumnRangeStatistics::Missing.into()
        };
        columns.insert(top_level_column_name.clone(), col_stats.0);
    }

    Ok(TableStatistics { columns })
}
