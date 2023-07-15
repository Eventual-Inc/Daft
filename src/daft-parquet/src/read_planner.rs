use std::{collections::BTreeMap, fmt::Display, io::Read, ops::Range, sync::Arc};

use common_error::DaftResult;
use daft_io::IOClient;

type RangeList = Vec<Range<usize>>;

pub trait ReadPlanPass {
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

pub(crate) struct ReadPlanBuilder {
    source: String,
    ranges: RangeList,
    passes: Vec<Box<dyn ReadPlanPass>>,
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

    pub async fn collect(self, io_client: Arc<IOClient>) -> DaftResult<RangesContainer> {
        let mut stored_ranges = Vec::with_capacity(self.ranges.len());
        for range in self.ranges.iter() {
            // multithread this
            let get_result = io_client
                .single_url_get(self.source.clone(), Some(range.clone()))
                .await?;
            let bytes = get_result.bytes().await?;
            stored_ranges.push((range.start, bytes.to_vec()));
        }
        stored_ranges.sort_by_key(|(start, _)| *start);
        Ok(RangesContainer {
            ranges: stored_ranges,
        })
    }
}

pub(crate) struct RangesContainer {
    ranges: Vec<(usize, Vec<u8>)>,
}

impl RangesContainer {
    pub fn get_range_reader<'a>(&'a self, range: Range<usize>) -> DaftResult<MultiRead<'a>> {
        let mut current_pos = range.start;
        let mut curr_index = 0;
        let start_point = self.ranges.binary_search_by_key(&current_pos, |(v, _)| *v);

        let mut slice_vec: Vec<&'a [u8]> = vec![];
        match start_point {
            Ok(index) => {
                let (byte_start, bytes_at_index) = &self.ranges[index];
                assert_eq!(*byte_start, current_pos);
                let start_offset = 0;
                let end_offset = bytes_at_index.len().min(range.end - current_pos);
                let curr_slice = &bytes_at_index.as_slice()[start_offset..end_offset];
                slice_vec.push(curr_slice);
                current_pos += curr_slice.len();
                curr_index = index + 1;
            }
            Err(index) => {
                assert!(index > 0);
                let index = index - 1;
                let (byte_start, bytes_at_index) = &self.ranges[index];
                let end = byte_start + bytes_at_index.len();
                println!("curr pos: {current_pos}, byte_start: {byte_start}, byte_end: {end}");
                assert!(current_pos >= *byte_start && current_pos < end);
                let start_offset = current_pos - byte_start;
                let end_offset = bytes_at_index.len().min(range.end - current_pos);
                let curr_slice = &bytes_at_index.as_slice()[start_offset..end_offset];
                slice_vec.push(curr_slice);
                current_pos += curr_slice.len();
                curr_index = index + 1;
            }
        };
        while current_pos < range.end && curr_index < self.ranges.len() {
            let (byte_start, bytes_at_index) = &self.ranges[curr_index];
            assert_eq!(*byte_start, current_pos);
            let start_offset = 0;
            let end_offset = bytes_at_index.len().min(range.end - current_pos);
            let curr_slice = &bytes_at_index.as_slice()[start_offset..end_offset];
            slice_vec.push(curr_slice);
            current_pos += curr_slice.len();
            curr_index += 1;
        }

        assert_eq!(current_pos, range.end);

        Ok(MultiRead::new(slice_vec))
    }
}

pub(crate) struct MultiRead<'a> {
    sources: Vec<&'a [u8]>,
    pos_in_sources: usize,
    pos_in_current: usize,
}

impl<'a> MultiRead<'a> {
    fn new(sources: Vec<&'a [u8]>) -> MultiRead<'a> {
        for s in sources.iter() {
            println!("len of slice: {}", s.len());
        }

        MultiRead {
            sources,
            pos_in_sources: 0,
            pos_in_current: 0,
        }
    }
}

impl Read for MultiRead<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        println!("buf len {}", buf.len());

        let current = loop {
            if self.pos_in_sources >= self.sources.len() {
                return Ok(0); // EOF
            }
            let current = self.sources[self.pos_in_sources];
            if self.pos_in_current < current.len() {
                break current;
            }
            self.pos_in_current = 0;
            self.pos_in_sources += 1;
        };
        let read_size = buf.len().min(current.len() - self.pos_in_current);
        buf[..read_size].copy_from_slice(&current[self.pos_in_current..][..read_size]);
        self.pos_in_current += read_size;
        Ok(read_size)
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
