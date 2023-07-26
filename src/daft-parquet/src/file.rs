use std::{collections::HashSet, sync::Arc};

use arrow2::io::parquet::read::infer_schema;
use common_error::DaftResult;
use daft_core::{utils::arrow::cast_array_for_daft_if_needed, Series};
use daft_io::IOClient;
use daft_table::Table;
use parquet2::read::{BasicDecompressor, PageReader};
use snafu::ResultExt;

use crate::{
    metadata::read_parquet_metadata,
    read_planner::{self, CoalescePass, RangesContainer, ReadPlanner, SplitLargeRequestPass},
    UnableToConvertParquetPagesToArrowSnafu, UnableToOpenFileSnafu,
    UnableToParseSchemaFromMetadataSnafu,
};
use arrow2::io::parquet::read::column_iter_to_arrays;

pub(crate) struct ParquetReaderBuilder {
    uri: String,
    metadata: parquet2::metadata::FileMetaData,
    arrow_schema: arrow2::datatypes::Schema,
    selected_columns: Option<HashSet<String>>,
    row_start_offset: usize,
    num_rows: usize,
}

impl ParquetReaderBuilder {
    pub async fn from_uri(uri: &str, io_client: Arc<daft_io::IOClient>) -> super::Result<Self> {
        // TODO(sammy): We actually don't need this since we can do negative offsets when reading the metadata
        let size = io_client
            .single_url_get_size(uri.into())
            .await
            .context(UnableToOpenFileSnafu::<String> { path: uri.into() })?;

        let metadata = read_parquet_metadata(uri, size, io_client).await?;
        let num_rows = metadata.num_rows;
        let schema =
            infer_schema(&metadata)
                .context(UnableToParseSchemaFromMetadataSnafu::<String> { path: uri.into() })?;
        Ok(ParquetReaderBuilder {
            uri: uri.into(),
            metadata: metadata,
            arrow_schema: schema,
            selected_columns: None,
            row_start_offset: 0,
            num_rows: num_rows,
        })
    }

    pub fn prune_columns(mut self, columns: &[&str]) -> super::Result<Self> {
        let avail_names = self
            .arrow_schema
            .fields
            .iter()
            .map(|f| f.name.as_str())
            .collect::<HashSet<_>>();
        let mut names_to_keep = HashSet::new();
        for col_name in columns {
            if avail_names.contains(col_name) {
                names_to_keep.insert(col_name.to_string());
            } else {
                return Err(super::Error::FieldNotFound {
                    field: col_name.to_string(),
                    available_fields: avail_names.iter().map(|v| v.to_string()).collect(),
                    path: self.uri,
                });
            }
        }
        self.selected_columns = Some(names_to_keep);
        Ok(self)
    }

    pub fn limit(
        mut self,
        start_offset: Option<usize>,
        num_rows: Option<usize>,
    ) -> super::Result<Self> {
        let start_offset = start_offset.unwrap_or(0);
        let num_rows = num_rows.unwrap_or(self.metadata.num_rows.saturating_sub(start_offset));
        self.row_start_offset = start_offset;
        self.num_rows = num_rows;
        Ok(self)
    }

    pub fn build(mut self) -> super::Result<ParquetFileReader> {
        let mut row_ranges = vec![];

        let mut curr_row_index = 0;
        let mut rows_to_add = self.num_rows;

        for (i, rg) in self.metadata.row_groups.iter().enumerate() {
            if (curr_row_index + rg.num_rows()) < self.row_start_offset {
                curr_row_index += rg.num_rows();
                continue;
            } else if rows_to_add > 0 {
                let range_to_add = RowGroupRange {
                    row_group_index: i,
                    start: self.row_start_offset.saturating_sub(curr_row_index),
                    num_rows: rg.num_rows().min(rows_to_add),
                };
                rows_to_add = rows_to_add.saturating_sub(rg.num_rows().min(rows_to_add));
                row_ranges.push(range_to_add);
            } else {
                break;
            }
            curr_row_index += rg.num_rows();
        }

        if let Some(names_to_keep) = self.selected_columns {
            self.arrow_schema
                .fields
                .retain(|f| names_to_keep.contains(f.name.as_str()));
        }

        ParquetFileReader::new(self.uri, self.metadata, self.arrow_schema, row_ranges)
    }
}

struct RowGroupRange {
    row_group_index: usize,
    start: usize,
    num_rows: usize,
}

pub(crate) struct ParquetFileReader {
    uri: String,
    metadata: parquet2::metadata::FileMetaData,
    arrow_schema: arrow2::datatypes::Schema,
    row_ranges: Vec<RowGroupRange>,
}

impl ParquetFileReader {
    fn new(
        uri: String,
        metadata: parquet2::metadata::FileMetaData,
        arrow_schema: arrow2::datatypes::Schema,
        row_ranges: Vec<RowGroupRange>,
    ) -> super::Result<Self> {
        Ok(ParquetFileReader {
            uri,
            metadata,
            arrow_schema,
            row_ranges,
        })
    }

    fn naive_read_plan(&self) -> super::Result<ReadPlanner> {
        let arrow_fields = &self.arrow_schema.fields;

        let mut read_planner = ReadPlanner::new(&self.uri);

        for row_group_range in self.row_ranges.iter() {
            let rg = self
                .metadata
                .row_groups
                .get(row_group_range.row_group_index)
                .unwrap();

            let columns = rg.columns();
            for field in arrow_fields.iter() {
                let field_name = field.name.clone();
                let filtered_cols = columns
                    .iter()
                    .filter(|x| x.descriptor().path_in_schema[0] == field_name)
                    .collect::<Vec<_>>();

                for col in filtered_cols {
                    let (start, len) = col.byte_range();
                    let end = start + len;

                    read_planner.add_range(start as usize, end as usize);
                }
            }
        }

        Ok(read_planner)
    }

    pub async fn prebuffer_ranges(
        &self,
        io_client: Arc<IOClient>,
    ) -> super::Result<RangesContainer> {
        let mut read_planner = self.naive_read_plan()?;
        // TODO(sammy) these values should be populated by io_client
        read_planner.add_pass(Box::new(SplitLargeRequestPass {
            max_request_size: 16 * 1024 * 1024,
            split_threshold: 24 * 1024 * 1024,
        }));

        read_planner.add_pass(Box::new(CoalescePass {
            max_hole_size: 1024 * 1024,
            max_request_size: 16 * 1024 * 1024,
        }));

        read_planner.run_passes()?;
        Ok(read_planner.collect(io_client).await.unwrap())
    }

    pub fn read_from_ranges(self, ranges: RangesContainer) -> DaftResult<Table> {
        let all_series = self
            .arrow_schema
            .fields
            .iter()
            .map(|field| {
                let field_series = self
                    .row_ranges
                    .iter()
                    .map(|row_range| {
                        let rg = self
                            .metadata
                            .row_groups
                            .get(row_range.row_group_index)
                            .expect("Row Group index should be in bounds");
                        let columns = rg.columns();
                        let field_name = &field.name;
                        let filtered_cols = columns
                            .iter()
                            .filter(|x| &x.descriptor().path_in_schema[0] == field_name)
                            .collect::<Vec<_>>();

                        let mut decompressed_iters = Vec::with_capacity(filtered_cols.len());
                        let mut ptypes = Vec::with_capacity(filtered_cols.len());

                        for col in filtered_cols {
                            let (start, len) = col.byte_range();
                            let end = start + len;

                            // should stream this instead
                            let range_reader: read_planner::MultiRead<'_> =
                                ranges.get_range_reader(start as usize..end as usize)?;
                            let pages = PageReader::new(
                                range_reader,
                                col,
                                Arc::new(|_, _| true),
                                vec![],
                                4 * 1024 * 1024,
                            );

                            decompressed_iters.push(BasicDecompressor::new(pages, vec![]));

                            ptypes.push(&col.descriptor().descriptor.primitive_type);
                        }

                        let arr_iter = column_iter_to_arrays(
                            decompressed_iters,
                            ptypes,
                            field.clone(),
                            Some(4096),
                            rg.num_rows().min(row_range.start + row_range.num_rows),
                        )
                        .context(
                            UnableToConvertParquetPagesToArrowSnafu::<String> {
                                path: self.uri.clone(),
                            },
                        )?;

                        let mut all_arrays = vec![];

                        let mut curr_index = 0;

                        for arr in arr_iter {
                            let arr = arr?;

                            if (curr_index + arr.len()) < row_range.start {
                                // throw arrays less than what we need
                                curr_index += arr.len();
                                continue;
                            } else if curr_index < row_range.start {
                                let offset = row_range.start.saturating_sub(curr_index);
                                all_arrays.push(arr.sliced(offset, arr.len() - offset));
                                curr_index += arr.len();
                            } else {
                                curr_index += arr.len();
                                all_arrays.push(arr);
                            }
                        }

                        all_arrays
                            .into_iter()
                            .map(|a| {
                                Series::try_from((
                                    field.name.as_str(),
                                    cast_array_for_daft_if_needed(a),
                                ))
                            })
                            .collect::<DaftResult<Vec<Series>>>()
                    })
                    .collect::<DaftResult<Vec<_>>>()?;

                Series::concat(&field_series.iter().flatten().collect::<Vec<_>>())
            })
            .collect::<DaftResult<Vec<_>>>()?;

        let daft_schema = daft_core::schema::Schema::try_from(&self.arrow_schema)?;

        Table::new(daft_schema, all_series)
    }
}
