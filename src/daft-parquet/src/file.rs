use std::{collections::HashSet, sync::Arc};

use arrow2::io::parquet::read::schema::infer_schema_with_options;
use common_error::DaftResult;
use daft_core::{utils::arrow::cast_array_for_daft_if_needed, Series};
use daft_io::IOClient;
use daft_table::Table;
use futures::{future::try_join_all, StreamExt};
use parquet2::{
    page::{CompressedPage, Page},
    read::get_owned_page_stream_from_column_start,
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
    row_groups: Option<Vec<i64>>,
    schema_inference_options: ParquetSchemaInferenceOptions,
}
use parquet2::read::decompress;

fn streaming_decompression<S: futures::Stream<Item = parquet2::error::Result<CompressedPage>>>(
    input: S,
) -> impl futures::Stream<Item = parquet2::error::Result<Page>> {
    async_stream::stream! {
        let mut buffer = vec![];

        for await compressed_page in input {
            let compressed_page = compressed_page?;
            yield decompress(compressed_page, &mut buffer);
        }
        drop(buffer);
    }
}

pub struct StreamIterator<S> {
    curr: Option<Page>,
    src: tokio::sync::Mutex<S>,
    handle: tokio::runtime::Handle,
}

impl<S> StreamIterator<S>
where
    S: futures::Stream<Item = parquet2::error::Result<Page>> + std::marker::Unpin,
{
    pub fn new(src: S, handle: tokio::runtime::Handle) -> Self {
        StreamIterator {
            curr: None,
            src: tokio::sync::Mutex::new(src),
            handle,
        }
    }
}

impl<S> FallibleStreamingIterator for StreamIterator<S>
where
    S: futures::Stream<Item = parquet2::error::Result<Page>> + std::marker::Unpin,
{
    type Error = parquet2::error::Error;
    type Item = Page;
    fn advance(&mut self) -> Result<(), Self::Error> {
        let val = self.handle.block_on(async {
            let mut s_guard = self.src.lock().await;
            s_guard.next().await
        });
        if let Some(val) = val {
            let val = val?;
            self.curr = Some(val);
        } else {
            self.curr = None;
        }
        Ok(())
    }

    fn get(&self) -> Option<&Self::Item> {
        self.curr.as_ref()
    }
}

pub(crate) fn build_row_ranges(
    num_rows: usize,
    row_start_offset: usize,
    row_groups: Option<&[i64]>,
    metadata: &parquet2::metadata::FileMetaData,
    uri: &str,
) -> super::Result<Vec<RowGroupRange>> {
    let mut row_ranges = vec![];
    let mut curr_row_index = 0;
    let mut rows_to_add = num_rows;
    if let Some(row_groups) = row_groups {
        for i in row_groups {
            let i = *i as usize;
            if !(0..metadata.row_groups.len()).contains(&i) {
                return Err(super::Error::ParquetRowGroupOutOfIndex {
                    path: uri.to_string(),
                    row_group: i as i64,
                    total_row_groups: metadata.row_groups.len() as i64,
                });
            }
            let rg = metadata.row_groups.get(i).unwrap();
            let range_to_add = RowGroupRange {
                row_group_index: i,
                start: 0,
                num_rows: rg.num_rows(),
            };
            row_ranges.push(range_to_add);
        }
    } else {
        for (i, rg) in metadata.row_groups.iter().enumerate() {
            if (curr_row_index + rg.num_rows()) < row_start_offset {
                curr_row_index += rg.num_rows();
                continue;
            } else if rows_to_add > 0 {
                let range_to_add = RowGroupRange {
                    row_group_index: i,
                    start: row_start_offset.saturating_sub(curr_row_index),
                    num_rows: rg.num_rows().min(rows_to_add),
                };
                rows_to_add = rows_to_add.saturating_sub(rg.num_rows().min(rows_to_add));
                row_ranges.push(range_to_add);
            } else {
                break;
            }
            curr_row_index += rg.num_rows();
        }
    }
    Ok(row_ranges)
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
            row_groups: None,
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

    pub fn set_row_groups(mut self, row_groups: &[i64]) -> super::Result<Self> {
        self.row_groups = Some(row_groups.to_vec());
        Ok(self)
    }

    pub fn set_infer_schema_options(mut self, opts: ParquetSchemaInferenceOptions) -> Self {
        self.schema_inference_options = opts;
        self
    }

    pub fn build(self) -> super::Result<ParquetFileReader> {
        let row_ranges = build_row_ranges(
            self.num_rows,
            self.row_start_offset,
            self.row_groups.as_deref(),
            &self.metadata,
            &self.uri,
        )?;

        let mut arrow_schema =
            infer_schema_with_options(&self.metadata, &Some(self.schema_inference_options.into()))
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
pub(crate) struct RowGroupRange {
    pub row_group_index: usize,
    pub start: usize,
    pub num_rows: usize,
}

pub(crate) struct ParquetFileReader {
    uri: String,
    metadata: Arc<parquet2::metadata::FileMetaData>,
    arrow_schema: arrow2::datatypes::SchemaRef,
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
            arrow_schema: arrow_schema.into(),
            row_ranges: Arc::new(row_ranges),
        })
    }

    pub fn arrow_schema(&self) -> &Arc<arrow2::datatypes::Schema> {
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

    pub async fn read_from_ranges_into_table(
        self,
        ranges: Arc<RangesContainer>,
    ) -> DaftResult<Table> {
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
                        let rt_handle = tokio::runtime::Handle::current();
                        let field = field.clone();
                        let owned_uri = self.uri.clone();
                        let rg = metadata
                            .row_groups
                            .get(row_range.row_group_index)
                            .expect("Row Group index should be in bounds");
                        let num_rows = rg.num_rows().min(row_range.start + row_range.num_rows);
                        let columns = rg.columns();
                        let field_name = &field.name;
                        let filtered_cols_idx = columns
                            .iter()
                            .enumerate()
                            .filter(|(_, x)| &x.descriptor().path_in_schema[0] == field_name)
                            .map(|(i, _)| i)
                            .collect::<Vec<_>>();

                        let range_readers = filtered_cols_idx
                            .iter()
                            .map(|i| {
                                let c = columns.get(*i).unwrap();
                                let (start, len) = c.byte_range();
                                let end: u64 = start + len;
                                let range_reader = ranges
                                    .get_range_reader(start as usize..end as usize)
                                    .unwrap();

                                Box::pin(range_reader)
                            })
                            .collect::<Vec<_>>();
                        let metadata = metadata.clone();
                        let handle = tokio::task::spawn(async move {
                            let mut decompressed_iters =
                                Vec::with_capacity(filtered_cols_idx.len());
                            let mut ptypes = Vec::with_capacity(filtered_cols_idx.len());

                            for (col_idx, range_reader) in
                                filtered_cols_idx.into_iter().zip(range_readers)
                            {
                                let col = metadata
                                    .row_groups
                                    .get(row_range.row_group_index)
                                    .expect("Row Group index should be in bounds")
                                    .columns()
                                    .get(col_idx)
                                    .expect("Column index should be in bounds");
                                ptypes.push(col.descriptor().descriptor.primitive_type.clone());

                                let compressed_page_stream =
                                    get_owned_page_stream_from_column_start(
                                        col,
                                        range_reader,
                                        vec![],
                                        Arc::new(|_, _| true),
                                        4 * 1024 * 1024,
                                    )
                                    .await
                                    .with_context(|_| {
                                        UnableToCreateParquetPageStreamSnafu::<String> {
                                            path: owned_uri.clone(),
                                        }
                                    })?;
                                let page_stream = streaming_decompression(compressed_page_stream);
                                let pinned_stream = Box::pin(page_stream);
                                decompressed_iters
                                    .push(StreamIterator::new(pinned_stream, rt_handle.clone()))
                            }

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
                        drop(series_to_concat);
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
        let daft_schema = daft_core::schema::Schema::try_from(self.arrow_schema.as_ref())?;

        Table::new(daft_schema, all_series)
    }

    pub async fn read_from_ranges_into_arrow_arrays(
        self,
        ranges: Arc<RangesContainer>,
    ) -> DaftResult<Vec<Vec<Box<dyn arrow2::array::Array>>>> {
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
                        let rt_handle = tokio::runtime::Handle::current();
                        let field = field.clone();
                        let owned_uri = self.uri.clone();
                        let rg = metadata
                            .row_groups
                            .get(row_range.row_group_index)
                            .expect("Row Group index should be in bounds");
                        let num_rows = rg.num_rows().min(row_range.start + row_range.num_rows);
                        let columns = rg.columns();
                        let field_name = &field.name;
                        let filtered_cols_idx = columns
                            .iter()
                            .enumerate()
                            .filter(|(_, x)| &x.descriptor().path_in_schema[0] == field_name)
                            .map(|(i, _)| i)
                            .collect::<Vec<_>>();

                        let range_readers = filtered_cols_idx
                            .iter()
                            .map(|i| {
                                let c = columns.get(*i).unwrap();
                                let (start, len) = c.byte_range();
                                let end: u64 = start + len;
                                let range_reader = ranges
                                    .get_range_reader(start as usize..end as usize)
                                    .unwrap();

                                Box::pin(range_reader)
                            })
                            .collect::<Vec<_>>();
                        let metadata = metadata.clone();
                        let handle = tokio::task::spawn(async move {
                            let mut decompressed_iters =
                                Vec::with_capacity(filtered_cols_idx.len());
                            let mut ptypes = Vec::with_capacity(filtered_cols_idx.len());

                            for (col_idx, range_reader) in
                                filtered_cols_idx.into_iter().zip(range_readers)
                            {
                                let col = metadata
                                    .row_groups
                                    .get(row_range.row_group_index)
                                    .expect("Row Group index should be in bounds")
                                    .columns()
                                    .get(col_idx)
                                    .expect("Column index should be in bounds");
                                ptypes.push(col.descriptor().descriptor.primitive_type.clone());

                                let compressed_page_stream =
                                    get_owned_page_stream_from_column_start(
                                        col,
                                        range_reader,
                                        vec![],
                                        Arc::new(|_, _| true),
                                        4 * 1024 * 1024,
                                    )
                                    .await
                                    .with_context(|_| {
                                        UnableToCreateParquetPageStreamSnafu::<String> {
                                            path: owned_uri.clone(),
                                        }
                                    })?;
                                let page_stream = streaming_decompression(compressed_page_stream);
                                let pinned_stream = Box::pin(page_stream);
                                decompressed_iters
                                    .push(StreamIterator::new(pinned_stream, rt_handle.clone()))
                            }

                            let (send, recv) = tokio::sync::oneshot::channel();
                            rayon::spawn(move || {
                                let arr_iter = column_iter_to_arrays(
                                    decompressed_iters,
                                    ptypes.iter().collect(),
                                    field.clone(),
                                    Some(128 * 1024),
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
                                    Ok(all_arrays)
                                })();

                                let _ = send.send(ser);
                            });
                            recv.await.context(OneShotRecvSnafu {})?
                        });
                        Ok(handle)
                    })
                    .collect::<DaftResult<Vec<_>>>()?;
                let owned_uri = self.uri.clone();
                let array_handle = tokio::task::spawn(async move {
                    let all_arrays = try_join_all(field_handles).await.context(JoinSnafu {
                        path: owned_uri.to_string(),
                    })?;
                    let all_arrays = all_arrays.into_iter().collect::<DaftResult<Vec<_>>>()?;
                    let concated = all_arrays.concat();
                    Ok(concated)
                });
                Ok(array_handle)
            })
            .collect::<DaftResult<Vec<_>>>()?;

        let all_field_arrays = try_join_all(all_handles)
            .await
            .context(JoinSnafu {
                path: self.uri.to_string(),
            })?
            .into_iter()
            .collect::<DaftResult<Vec<_>>>()?;
        Ok(all_field_arrays)
    }
}
