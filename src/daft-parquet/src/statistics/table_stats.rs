use common_error::DaftResult;
use daft_stats::{ColumnRangeStatistics, TableStatistics};
use snafu::ResultExt;

use super::Wrap;

use indexmap::IndexMap;

impl TryFrom<&crate::metadata::RowGroupMetaData> for Wrap<TableStatistics> {
    type Error = super::Error;
    fn try_from(value: &crate::metadata::RowGroupMetaData) -> Result<Self, Self::Error> {
        let _num_rows = value.num_rows();
        let mut columns = IndexMap::new();
        for col in value.columns() {
            let stats = col
                .statistics()
                .transpose()
                .context(super::UnableToParseParquetColumnStatisticsSnafu)?;
            let col_stats: Option<Wrap<ColumnRangeStatistics>> =
                stats.and_then(|v| v.as_ref().try_into().ok());
            let col_stats = col_stats.unwrap_or(ColumnRangeStatistics::Missing.into());
            columns.insert(
                col.descriptor().path_in_schema.get(0).unwrap().clone(),
                col_stats.0,
            );
        }

        Ok(TableStatistics { columns }.into())
    }
}

pub fn row_group_metadata_to_table_stats(
    metadata: &crate::metadata::RowGroupMetaData,
) -> DaftResult<TableStatistics> {
    let result = Wrap::<TableStatistics>::try_from(metadata)?;
    Ok(result.0)
}
