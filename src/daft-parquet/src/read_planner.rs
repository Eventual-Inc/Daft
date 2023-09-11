use std::{fmt::Display, ops::Range, sync::Arc};

use bytes::Bytes;
use common_error::DaftResult;
use daft_io::IOClient;
use futures::StreamExt;
use tokio::task::JoinHandle;

type RangeList = Vec<Range<usize>>;

pub trait ReadPlanPass: Send {
    fn run(&self, ranges: &RangeList) -> crate::Result<(bool, RangeList)>;
}

pub struct CoalescePass {
    pub max_hole_size: usize,
    pub max_request_size: usize,
}

impl ReadPlanPass for CoalescePass {
    fn run(&self, ranges: &RangeList) -> crate::Result<(bool, RangeList)> {
        let mut ranges = ranges.clone();
        let before_num_ranges = ranges.len();
        // sort by start
        ranges.sort_by_key(|v| v.start);

        // filter out zero length
        ranges.retain(|v| v.end > v.start);

        if ranges.is_empty() {
            return Ok((before_num_ranges != ranges.len(), ranges));
        }

        let mut curr_start = ranges.first().unwrap().start;
        let mut curr_end = ranges.first().unwrap().end;
        let mut new_ranges = vec![];
        for range in ranges.iter().skip(1) {
            if (range.start <= (curr_end + self.max_hole_size))
                && ((range.end.max(curr_end) - curr_start) < self.max_request_size)
            {
                curr_end = range.end.max(curr_end);
            } else {
                new_ranges.push(curr_start..curr_end);
                curr_start = range.start;
                curr_end = range.end;
            }
        }
        new_ranges.push(curr_start..curr_end);
        Ok((before_num_ranges != new_ranges.len(), new_ranges))
    }
}

pub struct SplitLargeRequestPass {
    pub max_request_size: usize,
    pub split_threshold: usize,
}

impl ReadPlanPass for SplitLargeRequestPass {
    fn run(&self, ranges: &RangeList) -> crate::Result<(bool, RangeList)> {
        let mut ranges = ranges.clone();
        let before_num_ranges = ranges.len();

        // filter out zero length
        ranges.retain(|v| v.end > v.start);

        if ranges.is_empty() {
            return Ok((before_num_ranges != ranges.len(), ranges));
        }

        let mut new_ranges = vec![];
        for range in ranges.iter() {
            if (range.end - range.start) > self.split_threshold {
                let mut curr_start = range.start;
                while curr_start < range.end {
                    let target_end = range.end.min(curr_start + self.max_request_size);
                    new_ranges.push(curr_start..target_end);
                    curr_start = target_end;
                }
            } else {
                new_ranges.push(range.clone());
            }
        }
        Ok((before_num_ranges != new_ranges.len(), new_ranges))
    }
}

enum RangeCacheState {
    InFlight(JoinHandle<std::result::Result<Bytes, daft_io::Error>>),
    Ready(Bytes),
}

struct RangeCacheEntry {
    start: usize,
    end: usize,
    state: tokio::sync::Mutex<RangeCacheState>,
}

impl RangeCacheEntry {
    async fn get_or_wait(&self, range: Range<usize>) -> std::result::Result<Bytes, daft_io::Error> {
        {
            let mut _guard = self.state.lock().await;
            match &mut (*_guard) {
                RangeCacheState::InFlight(f) => {
                    // TODO(sammy): thread in url for join error
                    let v = f
                        .await
                        .map_err(|e| daft_io::Error::JoinError { source: e })??;
                    *_guard = RangeCacheState::Ready(v.clone());
                    Ok(v.slice(range))
                }
                RangeCacheState::Ready(v) => Ok(v.slice(range)),
            }
        }
    }
}

pub(crate) struct ReadPlanner {
    source: String,
    ranges: RangeList,
    passes: Vec<Box<dyn ReadPlanPass>>,
}

impl ReadPlanner {
    pub fn new(source: &str) -> Self {
        ReadPlanner {
            source: source.into(),
            ranges: vec![],
            passes: vec![],
        }
    }

    pub fn add_range(&mut self, start: usize, end: usize) {
        self.ranges.push(start..end);
    }

    pub fn add_pass(&mut self, pass: Box<dyn ReadPlanPass>) {
        self.passes.push(pass);
    }

    pub fn run_passes(&mut self) -> super::Result<()> {
        for pass in self.passes.iter() {
            let (changed, ranges) = pass.run(&self.ranges)?;
            if changed {
                self.ranges = ranges;
            }
        }

        Ok(())
    }

    pub fn collect(self, io_client: Arc<IOClient>) -> DaftResult<Arc<RangesContainer>> {
        let mut entries = Vec::with_capacity(self.ranges.len());
        for range in self.ranges {
            let owned_io_client = io_client.clone();
            let owned_url = self.source.clone();
            let start = range.start;
            let end = range.end;
            let join_handle = tokio::spawn(async move {
                let get_result = owned_io_client
                    .single_url_get(owned_url, Some(range.clone()))
                    .await?;
                get_result.bytes().await
            });
            let state = RangeCacheState::InFlight(join_handle);
            let entry = Arc::new(RangeCacheEntry {
                start,
                end,
                state: tokio::sync::Mutex::new(state),
            });
            entries.push(entry);
        }
        Ok(Arc::new(RangesContainer { ranges: entries }))
    }
}

pub(crate) struct RangesContainer {
    ranges: Vec<Arc<RangeCacheEntry>>,
}

impl RangesContainer {
    pub fn get_range_reader(&self, range: Range<usize>) -> DaftResult<impl futures::AsyncRead> {
        let mut current_pos = range.start;
        let mut curr_index;
        let start_point = self.ranges.binary_search_by_key(&current_pos, |e| e.start);

        let mut needed_entries = vec![];
        let mut ranges_to_slice = vec![];
        match start_point {
            Ok(index) => {
                let entry = self.ranges[index].clone();
                let len = entry.end - entry.start;
                assert_eq!(entry.start, current_pos);
                let start_offset = 0;
                let end_offset = len.min(range.end - current_pos);

                needed_entries.push(entry);
                ranges_to_slice.push(start_offset..end_offset);

                current_pos += end_offset - start_offset;
                curr_index = index + 1;
            }
            Err(index) => {
                assert!(
                    index > 0,
                    "range: {range:?}, start: {}, end: {}",
                    &self.ranges[index].start,
                    &self.ranges[index].end
                );
                let index = index - 1;
                let entry = self.ranges[index].clone();
                let start = entry.start;
                let end = entry.end;
                let len = end - start;
                assert!(current_pos >= start && current_pos < end, "range: {range:?}, current_pos: {current_pos}, bytes_start: {start}, end: {end}");
                let start_offset = current_pos - start;
                let end_offset = len.min(range.end - start);
                needed_entries.push(entry);
                ranges_to_slice.push(start_offset..end_offset);
                current_pos += end_offset - start_offset;
                curr_index = index + 1;
            }
        };
        while current_pos < range.end && curr_index < self.ranges.len() {
            let entry = self.ranges[curr_index].clone();
            let start = entry.start;
            let end = entry.end;
            let len = end - start;
            assert_eq!(start, current_pos);
            let start_offset = 0;
            let end_offset = len.min(range.end - start);
            needed_entries.push(entry);
            ranges_to_slice.push(start_offset..end_offset);
            current_pos += end_offset - start_offset;
            curr_index += 1;
        }

        assert_eq!(current_pos, range.end);

        let bytes_iter = tokio_stream::iter(needed_entries.into_iter().zip(ranges_to_slice))
            .then(|(e, r)| async move { e.get_or_wait(r).await });

        let stream_reader = tokio_util::io::StreamReader::new(bytes_iter);
        let convert = async_compat::Compat::new(stream_reader);

        Ok(convert)
    }
}

impl Display for ReadPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "ReadPlanBuilder: {} ranges", self.ranges.len())?;
        for range in self.ranges.iter() {
            writeln!(
                f,
                "{}-{}, {}",
                range.start,
                range.end,
                range.end - range.start
            )?;
        }
        Ok(())
    }
}
