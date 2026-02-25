use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::Schema;
use daft_stats::{ColumnRangeStatistics, TableStatistics};
use indexmap::IndexMap;
use snafu::ResultExt;

use super::column_range::{
    arrowrs_statistics_to_column_range_statistics, parquet_statistics_to_column_range_statistics,
};

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
        .into_iter()
        .map(|field| {
            Ok(if ColumnRangeStatistics::supports_dtype(&field.dtype) {
                parquet_column_metadata
                    .swap_remove(&field.name)
                    .expect("Cannot find parsed Daft field in Parquet rowgroup metadata")
                    .transpose()
                    .context(super::UnableToParseParquetColumnStatisticsSnafu)?
                    .and_then(|v| {
                        parquet_statistics_to_column_range_statistics(v.as_ref(), &field.dtype).ok()
                    })
                    .unwrap_or(ColumnRangeStatistics::Missing)
            } else {
                ColumnRangeStatistics::Missing
            })
        })
        .collect::<DaftResult<_>>()?;

    Ok(TableStatistics::new(columns, Arc::new(schema.clone())))
}

/// Convert arrow-rs RowGroupMetaData to TableStatistics.
///
/// This is the arrow-rs equivalent of [`row_group_metadata_to_table_stats`].
pub fn arrowrs_row_group_metadata_to_table_stats(
    metadata: &parquet::file::metadata::RowGroupMetaData,
    schema: &Schema,
) -> DaftResult<TableStatistics> {
    // Create a map from {field_name: (statistics, column_descriptor)} for easy access
    let mut parquet_column_metadata: IndexMap<&str, _> = metadata
        .columns()
        .iter()
        .map(|col| {
            let top_level_column_name = col
                .column_descr()
                .path()
                .parts()
                .first()
                .expect("Parquet schema should have at least one entry in path");
            (
                top_level_column_name.as_str(),
                (col.statistics(), col.column_descr()),
            )
        })
        .collect();

    // Iterate through the schema and construct ColumnRangeStatistics per field
    let columns = schema
        .into_iter()
        .map(|field| {
            Ok(if ColumnRangeStatistics::supports_dtype(&field.dtype) {
                parquet_column_metadata
                    .swap_remove(field.name.as_str())
                    .and_then(|(stats_opt, col_descr)| {
                        stats_opt.and_then(|stats| {
                            arrowrs_statistics_to_column_range_statistics(
                                stats,
                                col_descr,
                                &field.dtype,
                            )
                            .ok()
                        })
                    })
                    .unwrap_or(ColumnRangeStatistics::Missing)
            } else {
                ColumnRangeStatistics::Missing
            })
        })
        .collect::<DaftResult<_>>()?;

    Ok(TableStatistics::new(columns, Arc::new(schema.clone())))
}
