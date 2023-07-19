use std::{fmt::Display, io::Read, ops::Range, sync::Arc};

use bytes::Bytes;
use common_error::DaftResult;
use daft_io::IOClient;
use futures::{StreamExt, TryStreamExt};
use snafu::ResultExt;
use tokio::task::JoinHandle;

use crate::JoinSnafu;

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

pub(crate) struct ReadPlanBuilder {
    source: String,
    ranges: RangeList,
    passes: Vec<Box<dyn ReadPlanPass>>,
}

enum RangeCacheState {
    InFlight(JoinHandle<DaftResult<bytes::Bytes>>),
    Ready(bytes::Bytes),
}

struct RangeCacheEntry {
    start: usize,
    end: usize,
    state: std::sync::Mutex<RangeCacheState>,
}

impl RangeCacheEntry {
    async fn get_or_wait(&self) -> DaftResult<Bytes> {
        {
            let mut _guard = self.state.lock().unwrap();
            match &mut (*_guard) {
                RangeCacheState::InFlight(f) => {
                    let v = f.await.context(JoinSnafu {})??;
                    *_guard = RangeCacheState::Ready(v.clone());
                    return Ok(v);
                }
                RangeCacheState::Ready(v) => Ok(v.clone()),
            }
        }
    }
}

impl ReadPlanBuilder {
    pub fn new(source: &str) -> Self {
        ReadPlanBuilder {
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

    pub fn collect(self, io_client: Arc<IOClient>) -> DaftResult<RangesContainer> {
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
                Ok(get_result.bytes().await?)
            });
            let state = RangeCacheState::InFlight(join_handle);
            let entry = RangeCacheEntry {
                start,
                end,
                state: std::sync::Mutex::new(state),
            };
            entries.push(entry);
        }
        Ok(RangesContainer { ranges: entries })
    }
}

pub(crate) struct RangesContainer {
    ranges: Vec<RangeCacheEntry>,
}

impl RangesContainer {
    pub async fn get_range_reader(&self, range: Range<usize>) -> DaftResult<MultiRead> {
        let mut current_pos = range.start;
        let mut curr_index;
        let start_point = self.ranges.binary_search_by_key(&current_pos, |e| e.start);

        let mut needed_entries = vec![];
        let mut ranges_to_slice = vec![];
        match start_point {
            Ok(index) => {
                let entry = &self.ranges[index];
                let len = entry.end - entry.start;
                assert_eq!(entry.start, current_pos);
                let start_offset = 0;
                let end_offset = len.min(range.end - current_pos);

                needed_entries.push(entry);
                ranges_to_slice.push(start_offset..end_offset);

                current_pos += (end_offset - start_offset);
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
                let entry = &self.ranges[index];
                let start = entry.start;
                let end = entry.end;
                let len = end - start;
                assert!(current_pos >= start && current_pos < end, "range: {range:?}, current_pos: {current_pos}, bytes_start: {start}, end: {end}");
                let start_offset = current_pos - start;
                let end_offset = len.min(range.end - start);
                needed_entries.push(entry);
                ranges_to_slice.push(start_offset..end_offset);
                current_pos += (end_offset - start_offset);
                curr_index = index + 1;
            }
        };
        while current_pos < range.end && curr_index < self.ranges.len() {
            let entry = &self.ranges[curr_index];
            let start = entry.start;
            let end = entry.end;
            let len = end - start;
            assert_eq!(start, current_pos);
            let start_offset = 0;
            let end_offset = len.min(range.end - start);
            needed_entries.push(entry);
            ranges_to_slice.push(start_offset..end_offset);
            current_pos += (end_offset - start_offset);
            curr_index += 1;
        }

        assert_eq!(current_pos, range.end);

        let bytes_objects =
            futures::future::try_join_all(needed_entries.iter().map(|e| e.get_or_wait())).await?;
        let slices = bytes_objects
            .into_iter()
            .zip(ranges_to_slice)
            .map(|(b, r)| b.slice(r))
            .collect();
        Ok(MultiRead::new(slices, range.end - range.start))
    }
}

pub(crate) struct MultiRead {
    sources: Vec<Bytes>,
    pos_in_sources: usize,
    pos_in_current: usize,
    bytes_read: usize,
    total_size: usize,
}

impl MultiRead {
    fn new(sources: Vec<Bytes>, total_size: usize) -> MultiRead {
        MultiRead {
            sources,
            pos_in_sources: 0,
            pos_in_current: 0,
            bytes_read: 0,
            total_size,
        }
    }
}

impl Read for MultiRead {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let current = loop {
            if self.pos_in_sources >= self.sources.len() {
                return Ok(0); // EOF
            }
            let current = &self.sources[self.pos_in_sources];
            if self.pos_in_current < current.len() {
                break current;
            }
            self.pos_in_current = 0;
            self.pos_in_sources += 1;
        };
        let read_size = buf.len().min(current.len() - self.pos_in_current);
        buf[..read_size].copy_from_slice(&current[self.pos_in_current..][..read_size]);
        self.pos_in_current += read_size;
        self.bytes_read += read_size;
        Ok(read_size)
    }

    #[inline]
    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> std::io::Result<usize> {
        if self.bytes_read >= self.total_size {
            return Ok(0);
        }
        let starting_bytes_read = self.bytes_read;
        buf.reserve(self.total_size - self.bytes_read);
        while self.bytes_read < self.total_size {
            let current = &self.sources[self.pos_in_sources];
            let slice = &current[self.pos_in_current..];
            buf.extend_from_slice(slice);
            self.pos_in_current = 0;
            self.pos_in_sources += 1;
            self.bytes_read += slice.len();
        }
        Ok(self.bytes_read - starting_bytes_read)
    }
}

impl Display for ReadPlanBuilder {
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
