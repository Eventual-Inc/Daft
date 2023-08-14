use std::{collections::HashSet, sync::Arc};

use arrow2::io::parquet::read::schema::infer_schema_with_options;
use common_error::DaftResult;
use daft_core::{utils::arrow::cast_array_for_daft_if_needed, Series};
use daft_io::IOClient;
use daft_table::Table;
use futures::{future::try_join_all, StreamExt};
use parquet2::{
    page::{CompressedPage, Page},
    read::get_page_stream_from_column_start,
    FallibleStreamingIterator,
};
use snafu::ResultExt;

use crate::{
    metadata::read_parquet_metadata,
    read::ParquetSchemaInferenceOptions,
    read_planner::{CoalescePass, RangesContainer, ReadPlanner, SplitLargeRequestPass},
    JoinSnafu, OneShotRecvSnafu, UnableToCreateParquetPageStreamSnafu,
    UnableToParseSchemaFromMetadataSnafu,
};
use arrow2::io::parquet::read::column_iter_to_arrays;

pub(crate) struct ParquetReaderBuilder {
    uri: String,
    metadata: parquet2::metadata::FileMetaData,
    selected_columns: Option<HashSet<String>>,
    row_start_offset: usize,
    num_rows: usize,
    schema_inference_options: ParquetSchemaInferenceOptions,
}
use parquet2::read::decompress;

fn streaming_decompression<S: futures::Stream<Item = parquet2::error::Result<CompressedPage>>>(
    input: S,
) -> impl futures::Stream<Item = parquet2::error::Result<Page>> {
    async_stream::stream! {
        for await compressed_page in input {
            let compressed_page = compressed_page?;
            let (send, recv) = tokio::sync::oneshot::channel();

            rayon::spawn(move || {
                let mut buffer = vec![];
                let _ = send.send(decompress(compressed_page, &mut buffer));

            });
            yield recv.await.expect("panic while decompressing page");
        }
    }
}
pub struct VecIterator {
    index: i64,
    src: Vec<parquet2::error::Result<Page>>,
}

impl VecIterator {
    pub fn new(src: Vec<parquet2::error::Result<Page>>) -> Self {
        VecIterator { index: -1, src }
    }
}

impl FallibleStreamingIterator for VecIterator {
    type Error = parquet2::error::Error;
    type Item = Page;
    fn advance(&mut self) -> Result<(), Self::Error> {
        self.index += 1;
        if (self.index as usize) < self.src.len() {
            if let Err(value) = self.src.get(self.index as usize).unwrap() {
                return Err(value.clone());
            }
        }
        Ok(())
    }

    fn get(&self) -> Option<&Self::Item> {
        if self.index < 0 || (self.index as usize) >= self.src.len() {
            return None;
        }

        if let Ok(val) = self.src.get(self.index as usize).unwrap() {
            Some(val)
        } else {
            None
        }
    }
}

impl ParquetReaderBuilder {
    pub async fn from_uri(uri: &str, io_client: Arc<daft_io::IOClient>) -> super::Result<Self> {
        // TODO(sammy): We actually don't need this since we can do negative offsets when reading the metadata
        let size = io_client.single_url_get_size(uri.into()).await?;
        let metadata = read_parquet_metadata(uri, size, io_client).await?;
        let num_rows = metadata.num_rows;
        Ok(ParquetReaderBuilder {
            uri: uri.into(),
            metadata,
            selected_columns: None,
            row_start_offset: 0,
            num_rows,
            schema_inference_options: Default::default(),
        })
    }

    pub fn metadata(&self) -> &parquet2::metadata::FileMetaData {
        &self.metadata
    }

    pub fn parquet_schema(&self) -> &parquet2::metadata::SchemaDescriptor {
        self.metadata().schema()
    }

    pub fn prune_columns(mut self, columns: &[&str]) -> super::Result<Self> {
        let avail_names = self
            .parquet_schema()
            .fields()
            .iter()
            .map(|f| f.name())
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

    pub fn set_infer_schema_options(mut self, opts: &ParquetSchemaInferenceOptions) -> Self {
        self.schema_inference_options = opts.clone();
        self
    }

    pub fn build(self) -> super::Result<ParquetFileReader> {
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

        let mut arrow_schema = infer_schema_with_options(
            &self.metadata,
            &Some(arrow2::io::parquet::read::schema::SchemaInferenceOptions {
                int96_coerce_to_timeunit: self
                    .schema_inference_options
                    .coerce_int96_timestamp_unit
                    .to_arrow(),
            }),
        )
        .context(UnableToParseSchemaFromMetadataSnafu::<String> {
            path: self.uri.clone(),
        })?;

        if let Some(names_to_keep) = self.selected_columns {
            arrow_schema
                .fields
                .retain(|f| names_to_keep.contains(f.name.as_str()));
        }

        ParquetFileReader::new(self.uri, self.metadata, arrow_schema, row_ranges)
    }
}

#[derive(Copy, Clone)]
struct RowGroupRange {
    row_group_index: usize,
    start: usize,
    num_rows: usize,
}

pub(crate) struct ParquetFileReader {
    uri: String,
    metadata: Arc<parquet2::metadata::FileMetaData>,
    arrow_schema: arrow2::datatypes::Schema,
    row_ranges: Arc<Vec<RowGroupRange>>,
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
            metadata: Arc::new(metadata),
            arrow_schema,
            row_ranges: Arc::new(row_ranges),
        })
    }

    pub fn arrow_schema(&self) -> &arrow2::datatypes::Schema {
        &self.arrow_schema
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

    pub fn prebuffer_ranges(&self, io_client: Arc<IOClient>) -> DaftResult<Arc<RangesContainer>> {
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
        read_planner.collect(io_client)
    }

    pub async fn read_from_ranges(self, ranges: Arc<RangesContainer>) -> DaftResult<Table> {
        let metadata = self.metadata;
        let all_handles = self
            .arrow_schema
            .fields
            .iter()
            .map(|field| {
                let owned_row_ranges = self.row_ranges.clone();
                let field_handles = owned_row_ranges
                    .iter()
                    .map(|row_range| {
                        let row_range = *row_range;
                        let field = field.clone();
                        let owned_uri = self.uri.clone();
                        let ranges = ranges.clone();
                        let owned_metadata = metadata.clone();

                        let handle = tokio::task::spawn(async move {
                            let rg = owned_metadata
                                .row_groups
                                .get(row_range.row_group_index)
                                .expect("Row Group index should be in bounds");
                            let num_rows = rg.num_rows().min(row_range.start + row_range.num_rows);
                            let columns = rg.columns();
                            let field_name = &field.name;
                            let filtered_cols = columns
                                .iter()
                                .filter(|x| &x.descriptor().path_in_schema[0] == field_name)
                                .collect::<Vec<_>>();

                            let mut decompressed_pages = Vec::with_capacity(filtered_cols.len());
                            let mut ptypes = Vec::with_capacity(filtered_cols.len());

                            for col in filtered_cols {
                                let (start, len) = col.byte_range();
                                let end = start + len;

                                let range_reader =
                                    ranges.get_range_reader(start as usize..end as usize)?;

                                let mut pinned = Box::pin(range_reader);
                                let compressed_page_stream = get_page_stream_from_column_start(
                                    col,
                                    &mut pinned,
                                    vec![],
                                    Arc::new(|_, _| true),
                                    4 * 1024 * 1024,
                                )
                                .await
                                .with_context(
                                    |_| UnableToCreateParquetPageStreamSnafu::<String> {
                                        path: owned_uri.clone(),
                                    },
                                )?;
                                let page_stream = streaming_decompression(compressed_page_stream);

                                decompressed_pages.push(page_stream.collect::<Vec<_>>().await);

                                ptypes.push(col.descriptor().descriptor.primitive_type.clone());
                            }

                            let decompressed_iters = decompressed_pages
                                .into_iter()
                                .map(VecIterator::new)
                                .collect();

                            let (send, recv) = tokio::sync::oneshot::channel();
                            rayon::spawn(move || {
                                let arr_iter = column_iter_to_arrays(
                                    decompressed_iters,
                                    ptypes.iter().collect(),
                                    field.clone(),
                                    Some(2048),
                                    num_rows,
                                );

                                let ser = (|| {
                                    let mut all_arrays = vec![];
                                    let mut curr_index = 0;

                                    for arr in arr_iter? {
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
                                })();

                                let _ = send.send(ser);
                            });
                            recv.await.context(OneShotRecvSnafu {})?
                        });
                        Ok(handle)
                    })
                    .collect::<DaftResult<Vec<_>>>()?;
                let owned_uri = self.uri.clone();
                let concated_handle = tokio::task::spawn(async move {
                    let series_to_concat =
                        try_join_all(field_handles).await.context(JoinSnafu {
                            path: owned_uri.to_string(),
                        })?;
                    let series_to_concat = series_to_concat
                        .into_iter()
                        .collect::<DaftResult<Vec<_>>>()?;

                    let (send, recv) = tokio::sync::oneshot::channel();
                    rayon::spawn(move || {
                        let concated =
                            Series::concat(&series_to_concat.iter().flatten().collect::<Vec<_>>());
                        let _ = send.send(concated);
                    });
                    recv.await.context(OneShotRecvSnafu {})?
                });
                Ok(concated_handle)
            })
            .collect::<DaftResult<Vec<_>>>()?;

        let all_series = try_join_all(all_handles)
            .await
            .context(JoinSnafu {
                path: self.uri.to_string(),
            })?
            .into_iter()
            .collect::<DaftResult<Vec<_>>>()?;
        let daft_schema = daft_core::schema::Schema::try_from(&self.arrow_schema)?;

        Table::new(daft_schema, all_series)
    }
}
