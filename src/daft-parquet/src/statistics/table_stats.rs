use common_error::DaftResult;
use daft_core::schema::Schema;
use daft_stats::{ColumnRangeStatistics, TableStatistics};
use snafu::ResultExt;

use super::column_range::parquet_statistics_to_column_range_statistics;

use indexmap::IndexMap;

pub fn row_group_metadata_to_table_stats(
    metadata: &crate::metadata::RowGroupMetaData,
    schema: &Schema,
) -> DaftResult<TableStatistics> {
    // Create a map from {field_name: statistics} from the RowGroupMetaData for easy access
    let mut parquet_column_metadata: IndexMap<_, _> = metadata
        .columns()
        .iter()
        .map(|col| {
            let top_level_column_name = col
                .descriptor()
                .path_in_schema
                .first()
                .expect("Parquet schema should have at least one entry in path_in_schema");
            (top_level_column_name, col.statistics())
        })
        .collect();

    // Iterate through the schema and construct ColumnRangeStatistics per field
    let columns = schema
        .fields
        .iter()
        .map(|(field_name, field)| {
            if ColumnRangeStatistics::supports_dtype(&field.dtype) {
                let stats: ColumnRangeStatistics = parquet_column_metadata
                    .swap_remove(field_name)
                    .expect("Cannot find parsed Daft field in Parquet rowgroup metadata")
                    .transpose()
                    .context(super::UnableToParseParquetColumnStatisticsSnafu)?
                    .and_then(|v| {
                        parquet_statistics_to_column_range_statistics(v.as_ref(), &field.dtype).ok()
                    })
                    .unwrap_or(ColumnRangeStatistics::Missing);
                Ok((field_name.clone(), stats))
            } else {
                Ok((field_name.clone(), ColumnRangeStatistics::Missing))
            }
        })
        .collect::<DaftResult<IndexMap<_, _>>>()?;

    Ok(TableStatistics { columns })
}
