use std::{fmt::Display, ops::Range};

type RangeList = Vec<Range<usize>>;

pub trait ReadPlanPass {
    fn run(&self, ranges: &RangeList) -> crate::Result<(bool, RangeList)>;
}

pub struct CoalescePass {
    pub max_hole_size: usize,
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
            if range.start <= (curr_end + self.max_hole_size) {
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
    ranges: RangeList,
    passes: Vec<Box<dyn ReadPlanPass>>,
}

impl ReadPlanBuilder {
    pub fn new() -> Self {
        ReadPlanBuilder {
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
