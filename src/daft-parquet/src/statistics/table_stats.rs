use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::Schema;
use daft_stats::{ColumnRangeStatistics, TableStatistics};
use indexmap::IndexMap;

use super::column_range::parquet_statistics_to_column_range_statistics;

/// Convert arrow-rs RowGroupMetaData to TableStatistics.
pub fn row_group_metadata_to_table_stats(
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
                    .swap_remove(field.name.as_ref())
                    .and_then(|(stats_opt, col_descr)| {
                        stats_opt.and_then(|stats| {
                            parquet_statistics_to_column_range_statistics(
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
