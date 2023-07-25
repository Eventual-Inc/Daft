use std::{sync::Arc, collections::HashSet};

use arrow2::io::parquet::read::infer_schema;
use snafu::ResultExt;

use crate::{metadata::read_parquet_metadata, UnableToParseSchemaFromMetadataSnafu};



struct ParquetReaderBuilder {
    uri: String,
    metadata: parquet2::metadata::FileMetaData,
    arrow_schema: arrow2::datatypes::Schema,
    selected_columns: Option<HashSet<String>>,
    row_start_offset: usize,
    num_rows: usize
}


impl ParquetReaderBuilder {
    pub async fn from_uri(uri: &str, io_client: Arc<daft_io::IOClient>) -> super::Result<Self> {
        let size = 10;
        let metadata = read_parquet_metadata(uri, size, io_client).await?;
        let num_rows = metadata.num_rows;
        let schema = infer_schema(&metadata).context(UnableToParseSchemaFromMetadataSnafu::<String> {path: uri.into()})?;
        Ok(ParquetReaderBuilder { uri: uri.into(), metadata: metadata, arrow_schema: schema, selected_columns: None, row_start_offset: 0, num_rows: num_rows})
    }

    pub fn prune_columns(mut self, columns: &[&str]) -> super::Result<Self> {
        let avail_names = self.arrow_schema.fields
            .iter()
            .map(|f| f.name.as_str())
            .collect::<HashSet<_>>();
        let mut names_to_keep = HashSet::new();
        for col_name in columns {
            if avail_names.contains(col_name) {
                names_to_keep.insert(col_name.to_string());
            } else {
                return Err(super::Error::FieldNotFound{
                    field: col_name.to_string(),
                    available_fields: avail_names.iter().map(|v| v.to_string()).collect(),
                    path: self.uri
                });
            }
        }
        self.selected_columns = Some(names_to_keep);
        Ok(self)
    }
    pub fn limit(mut self, start_offset: Option<usize>, num_rows: Option<usize>) -> super::Result<Self> {
        let start_offset = start_offset.unwrap_or(0);
        let num_rows = num_rows.unwrap_or(self.metadata.num_rows.saturating_sub(start_offset));
        self.row_start_offset = start_offset;
        self.num_rows = num_rows;
        Ok(self)
    }

    pub fn build(self) -> super::Result<ParquetReader> {
        let mut row_ranges = vec![];

        let rg_iter = self.metadata.row_groups.iter();

        let mut curr_row_index = 0;

        while let Some(rg) = rg_iter.next() {
            if (curr_row_index + rg.num_rows()) < self.row_start_offset {
                curr_row_index += rg.num_rows();
                continue;
            } else {
                
            }
        }
        

    }

}


struct RowGroupRange {
    row_group_index: usize,
    start: usize,
    end: usize,
}

struct ParquetReader {
    uri: String,
    metadata: parquet2::metadata::FileMetaData,
    arrow_schema: arrow2::datatypes::Schema,
    row_ranges: Vec<RowGroupRange>
}