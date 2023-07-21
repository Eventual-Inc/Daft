use arrow2::io::parquet::read::{column_iter_to_arrays, infer_schema};
use common_error::DaftResult;
use daft_core::{utils::arrow::cast_array_for_daft_if_needed, Series};
use daft_io::{get_runtime, IOClient};
use daft_table::Table;
use futures::{AsyncReadExt, StreamExt};
use parquet2::{
    fallible_streaming_iterator::{convert, Convert},
    metadata::FileMetaData,
    page::{self, CompressedPage, Page},
    read::{decompress, get_page_stream_from_column_start, BasicDecompressor, PageReader},
};
use snafu::ResultExt;
use std::{
    collections::{BTreeMap, HashSet},
    ops::Deref,
    sync::Arc,
    time::Instant,
};
use tokio::task::JoinHandle;

use crate::{
    metadata::{self, read_parquet_metadata},
    read_planner::{self, CoalescePass, RangesContainer, ReadPlanBuilder, SplitLargeRequestPass},
    JoinSnafu, UnableToDecompressPageSnafu,
};

fn plan_read_row_groups(
    uri: &str,
    columns: Option<&[&str]>,
    row_groups: Option<&[i64]>,
    metadata: &FileMetaData,
) -> DaftResult<ReadPlanBuilder> {
    let arrow_schema = infer_schema(metadata)?;
    let mut arrow_fields = arrow_schema.fields;
    if let Some(columns) = columns {
        let avail_names = arrow_fields
            .iter()
            .map(|f| f.name.as_str())
            .collect::<HashSet<_>>();
        let mut names_to_keep = HashSet::new();
        for col_name in columns {
            if avail_names.contains(col_name) {
                names_to_keep.insert(*col_name);
            } else {
                return Err(common_error::DaftError::FieldNotFound(format!(
                    "Field: {} not found in {:?} when planning read for parquet file",
                    col_name, avail_names
                )));
            }
        }

        arrow_fields.retain(|f| names_to_keep.contains(f.name.as_str()))
    };

    let num_row_groups = metadata.row_groups.len();
    let mut read_plan = read_planner::ReadPlanBuilder::new(uri);
    let row_groups = match row_groups {
        Some(rg) => rg.to_vec(),
        None => (0i64..num_row_groups as i64).collect(),
    };

    for row_group in row_groups {
        if !(0i64..num_row_groups as i64).contains(&row_group) {
            return Err(super::Error::ParquetRowGroupOutOfIndex {
                path: uri.into(),
                row_group,
                total_row_groups: num_row_groups as i64,
            }
            .into());
        }

        let rg = metadata.row_groups.get(row_group as usize).unwrap();

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

                read_plan.add_range(start as usize, end as usize);
            }
        }
    }
    Ok(read_plan)
}

use async_stream::stream;
use futures::stream::Stream;

fn streaming_decompression<S: Stream<Item = parquet2::error::Result<CompressedPage>>>(
    input: S,
) -> impl Stream<Item = parquet2::error::Result<Page>> {
    stream! {
        for await compressed_page in input {
            let compressed_page = compressed_page?;
            let page = tokio::task::spawn_blocking(move || {
                let mut buffer = vec![];
                decompress(compressed_page, &mut buffer)
            }).await.unwrap();
            yield page;

            // yield decompress(compressed_page?, &mut buffer);
        }
    }
}

use parquet2::FallibleStreamingIterator;

pub struct SyncIterator<T: Stream> {
    curr: Option<T::Item>,
    src: T,
    rt: tokio::runtime::Handle,
}

impl<T: Unpin + Stream> SyncIterator<T> {
    pub fn new(src: T) -> Self {
        Self::new_with_handle(src, tokio::runtime::Handle::current())
    }

    pub fn new_with_handle(src: T, rt: tokio::runtime::Handle) -> Self {
        Self {
            curr: None,
            src,
            rt,
        }
    }
}

impl<T: Unpin + Stream> FallibleStreamingIterator for SyncIterator<T> {
    type Error = parquet2::error::Error;
    type Item = T::Item;
    // fn next(&mut self) -> Option<Self::Item> {
    //     self.rt.block_on(self.src.next())
    // }
    fn advance(&mut self) -> Result<(), Self::Error> {
        self.curr = self.rt.block_on(self.src.next());
        Ok(())
    }

    fn get(&self) -> Option<&Self::Item> {
        self.curr.as_ref()
    }
}

pub struct VecIterator {
    index: i64,
    src: Vec<parquet2::error::Result<Page>>,
}

impl VecIterator {
    pub fn new(src: Vec<parquet2::error::Result<Page>>) -> Self {
        VecIterator {
            index: -1,
            src: src,
        }
    }
}

impl FallibleStreamingIterator for VecIterator {
    type Error = parquet2::error::Error;
    type Item = Page;
    // fn next(&mut self) -> Option<Self::Item> {
    //     self.rt.block_on(self.src.next())
    // }
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
            return Some(val);
        } else {
            None
        }
    }
}

async fn read_row_groups_from_ranges(
    reader: Arc<RangesContainer>,
    columns: Option<&[&str]>,
    row_groups: Option<&[i64]>,
    metadata: Arc<FileMetaData>,
) -> DaftResult<Table> {
    let arrow_schema = infer_schema(&metadata)?;

    let mut arrow_fields = arrow_schema.fields;

    if let Some(columns) = columns {
        let avail_names = arrow_fields
            .iter()
            .map(|f| f.name.as_str())
            .collect::<HashSet<_>>();
        let mut names_to_keep = HashSet::new();
        for col_name in columns {
            if avail_names.contains(col_name) {
                names_to_keep.insert(*col_name);
            } else {
                return Err(common_error::DaftError::FieldNotFound(format!(
                    "Field: {} not found in {:?} when planning read for parquet file",
                    col_name, avail_names
                )));
            }
        }

        arrow_fields.retain(|f| names_to_keep.contains(f.name.as_str()))
    };
    let daft_schema = daft_core::schema::Schema::try_from(&arrow2::datatypes::Schema {
        fields: arrow_fields.clone(),
        metadata: BTreeMap::new(),
    })?;

    let mut daft_series: Vec<Vec<JoinHandle<_>>> = Vec::with_capacity(arrow_fields.len());
    let num_row_groups = metadata.row_groups.len();

    let row_groups = match row_groups {
        Some(rg) => rg.to_vec(),
        None => (0i64..num_row_groups as i64).collect(),
    };

    for _ in 0..arrow_fields.len() {
        daft_series.push(Vec::with_capacity(row_groups.len()));
    }

    for row_group in row_groups {
        if !(0i64..num_row_groups as i64).contains(&row_group) {
            panic!("out of row group index");
        }
        for (ii, field) in arrow_fields.iter().enumerate() {
            let reader = reader.clone();
            let metadata = metadata.clone();
            let field = field.clone();

            let handle = tokio::task::spawn(async move {
                let metadata = metadata.clone();
                let rg = metadata.row_groups.get(row_group as usize).unwrap();

                let columns = rg.columns();

                let field_name = field.name.clone();
                let filtered_cols = columns
                    .iter()
                    .filter(|x| x.descriptor().path_in_schema[0] == field_name)
                    .collect::<Vec<_>>();
                println!("f_name: {field_name}, {}", filtered_cols.len());
                let mut decompressed_iters = vec![];
                let mut ptypes = vec![];

                // let range_readers =
                //     futures::future::try_join_all(filtered_cols.iter().map(|col| {
                //         let (start, len) = col.byte_range();
                //         let end = start + len;
                //         reader.get_range_reader(start as usize..end as usize)
                //     }))
                //     .await?;

                let range_readers: Vec<_> = filtered_cols
                    .iter()
                    .map(|col| {
                        let (start, len) = col.byte_range();
                        let end = start + len;
                        reader
                            .get_range_reader(start as usize..end as usize)
                            .unwrap()
                    })
                    .collect();
                for (col, range_reader) in filtered_cols.iter().zip(range_readers) {
                    // let pages = PageReader::new(
                    //     range_reader,
                    //     col,
                    //     Arc::new(|_, _| true),
                    //     vec![],
                    //     4 * 1024 * 1024,
                    // );
                    let mut pinned = Box::pin(range_reader);
                    let compressed_page_stream = get_page_stream_from_column_start(
                        col,
                        &mut pinned,
                        vec![],
                        Arc::new(|_, _| true),
                        4 * 1024 * 1024,
                    )
                    .await
                    .unwrap();
                    // futures::pin_mut!(page_stream);

                    let page_stream = streaming_decompression(compressed_page_stream);

                    // futures::pin_mut!(page_stream);
                    // let iterator = SyncIterator::new(page_stream);
                    
                    let now = Instant::now();
                    decompressed_iters.push(page_stream.collect::<Vec<_>>().await);

                    println!("{field_name} pipeline time: {}", now.elapsed().as_millis());

                    // decompressed_iters.push(BasicDecompressor::new(pages, vec![]));

                    ptypes.push(col.descriptor().descriptor.primitive_type.clone());
                }

                let num_rows = rg.num_rows();
                let field_name = field.name.clone();
                let decompressed_iters = decompressed_iters
                    .into_iter()
                    .map(|i| VecIterator::new(i))
                    .collect();

                let ser = tokio::task::spawn_blocking(move || {
                    let arr_iter = column_iter_to_arrays(
                        decompressed_iters,
                        ptypes.iter().collect(),
                        field,
                        Some(4096),
                        num_rows,
                    )?;

                    let all_arrays = arr_iter.collect::<arrow2::error::Result<Vec<_>>>()?;
                    let ser = all_arrays
                        .into_iter()
                        .map(|a| {
                            Series::try_from((
                                field_name.as_str(),
                                cast_array_for_daft_if_needed(a),
                            ))
                        })
                        .collect::<DaftResult<Vec<Series>>>()?;
                    DaftResult::Ok(ser)
                })
                .await
                .context(JoinSnafu {})
                .unwrap()
                .unwrap();
                Ok(ser)
            });

            daft_series[ii].push(handle);
        }
    }

    let mut compacted_series = vec![];

    for handles in daft_series.into_iter() {
        let concated_series = tokio::task::spawn(async move {
            let all_series = futures::future::try_join_all(handles)
                .await
                .unwrap()
                .into_iter()
                .collect::<DaftResult<Vec<_>>>()?;

            tokio::task::spawn_blocking(move || {
                Series::concat(all_series.iter().flatten().collect::<Vec<_>>().as_ref())
            })
            .await
            .unwrap()
        });
        compacted_series.push(concated_series);
    }
    let compacted_series = futures::future::try_join_all(compacted_series)
        .await
        .unwrap()
        .into_iter()
        .collect::<DaftResult<Vec<_>>>()?;

    Table::new(daft_schema, compacted_series)
}

pub fn read_parquet(
    uri: &str,
    columns: Option<&[&str]>,
    row_groups: Option<&[i64]>,
    size: Option<usize>,
    io_client: Arc<IOClient>,
) -> DaftResult<Table> {
    let runtime_handle = get_runtime(true)?;
    let _rt_guard = runtime_handle.enter();

    runtime_handle.block_on(async {
        let size = match size {
            Some(size) => size,
            None => io_client.single_url_get_size(uri.into()).await?,
        };
        use std::time::Instant;
        let now = Instant::now();
        let metadata = read_parquet_metadata(uri, size, io_client.clone()).await?;
        log::warn!(
            "total time for read_parquet_metadata: {}",
            now.elapsed().as_micros()
        );

        let now = Instant::now();
        let mut plan = plan_read_row_groups(uri, columns, row_groups, &metadata)?;
        log::warn!(
            "total time for plan_read_row_groups: {}",
            now.elapsed().as_micros()
        );

        plan.add_pass(Box::new(SplitLargeRequestPass {
            max_request_size: 8 * 1024 * 1024,
            split_threshold: 12 * 1024 * 1024,
        }));

        plan.add_pass(Box::new(CoalescePass {
            max_hole_size: 1024 * 1024,
            max_request_size: 8 * 1024 * 1024,
        }));

        let now = Instant::now();
        plan.run_passes()?;
        log::warn!("total time for run_passes: {}", now.elapsed().as_micros());

        let now = Instant::now();
        let dl_ranges = plan.collect(io_client)?;
        log::warn!("total time for plan.collect: {}", now.elapsed().as_micros());

        read_row_groups_from_ranges(dl_ranges, columns, row_groups, metadata).await
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_io::{config::IOConfig, IOClient};

    use super::read_parquet;
    #[test]
    fn test_parquet_read_from_s3() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/parquet-dev/mvp.parquet";

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);

        let table = read_parquet(file, None, None, None, io_client)?;
        assert_eq!(table.len(), 100);

        Ok(())
    }

    use crate::{
        read::plan_read_row_groups, read::read_row_groups_from_ranges, read_planner::CoalescePass,
    };

    #[tokio::test]
    async fn test_parquet_read_planner() -> DaftResult<()> {
        let file = "s3://daft-public-data/test_fixtures/parquet_small/0dad4c3f-da0d-49db-90d8-98684571391b-0.parquet";

        let mut io_config = IOConfig::default();
        io_config.s3.anonymous = true;

        let io_client = Arc::new(IOClient::new(io_config.into())?);
        let size = io_client.single_url_get_size(file.into()).await?;
        let metadata =
            crate::metadata::read_parquet_metadata(file, size, io_client.clone()).await?;
        let mut plan = plan_read_row_groups(file, Some(&["P_PARTKEY"]), Some(&[1, 2]), &metadata)?;

        plan.add_pass(Box::new(CoalescePass {
            max_hole_size: 1024 * 1024,
            max_request_size: 16 * 1024 * 1024,
        }));
        plan.run_passes()?;
        let memory = plan.collect(io_client.clone())?;
        let _table =
            read_row_groups_from_ranges(memory, Some(&["P_PARTKEY"]), Some(&[1, 2]), metadata)
                .await?;
        Ok(())
    }
}
