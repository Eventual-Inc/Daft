use std::{collections::BTreeMap, sync::Arc};

use common_error::DaftResult;
use daft_core::{datatypes::Field, schema::Schema};
use daft_stats::{ColumnRangeStatistics, TableStatistics};
use snafu::ResultExt;

use super::column_range::parquet_statistics_to_column_range_statistics;

use indexmap::IndexMap;

pub fn row_group_metadata_to_table_stats(
    metadata: &crate::metadata::RowGroupMetaData,
    pq_file_schema: &Schema,
    field_id_mapping: &Option<Arc<BTreeMap<i32, Field>>>,
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
    let columns = pq_file_schema.fields.iter().map(|(field_name, field)| {
        if ColumnRangeStatistics::supports_dtype(&field.dtype) {
            let stats: ColumnRangeStatistics = parquet_column_metadata
                .remove(field_name)
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
    });

    // Apply `field_id_mapping` against parsed statistics to rename the columns (if provided)
    let file_to_target_colname_mapping: Option<IndexMap<&String, String>> =
        field_id_mapping.as_ref().map(|field_id_mapping| {
            metadata
                .columns()
                .iter()
                .filter_map(|col| {
                    if let Some(target_colname) = col
                        .descriptor()
                        .base_type
                        .get_field_info()
                        .id
                        .and_then(|field_id| field_id_mapping.get(&field_id))
                    {
                        let top_level_column_name = col.descriptor().path_in_schema.first().expect(
                            "Parquet schema should have at least one entry in path_in_schema",
                        );
                        Some((top_level_column_name, target_colname.name.clone()))
                    } else {
                        None
                    }
                })
                .collect()
        });
    let columns = columns.map(|result| {
        if let Some(ref file_to_target_colname_mapping) = file_to_target_colname_mapping {
            result.map(|(field_name, stats)| {
                (
                    file_to_target_colname_mapping
                        .get(&field_name)
                        .cloned()
                        .unwrap_or(field_name.clone()),
                    stats,
                )
            })
        } else {
            result
        }
    });

    Ok(TableStatistics {
        columns: columns.collect::<DaftResult<IndexMap<_, _>>>()?,
    })
}
