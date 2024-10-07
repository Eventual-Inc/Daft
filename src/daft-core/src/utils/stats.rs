use common_error::DaftResult;

use crate::{
    array::{
        ops::{DaftCountAggable, DaftSumAggable, GroupIndices, VecIndices},
        prelude::{Float64Array, UInt64Array},
    },
    count_mode::CountMode,
};

pub struct Stats {
    pub sum: f64,
    pub count: u64,
    pub mean: Option<f64>,
}

pub fn calculate_stats(array: &Float64Array) -> DaftResult<Stats> {
    let sum = array.sum()?.get(0).unwrap();
    let count = array.count(CountMode::Valid)?.get(0).unwrap();
    let mean = calculate_mean(sum, count);
    Ok(Stats { sum, count, mean })
}

pub fn grouped_stats<'a>(
    array: &Float64Array,
    groups: &'a GroupIndices,
) -> DaftResult<impl Iterator<Item = (Stats, &'a VecIndices)>> {
    let grouped_sum = array.grouped_sum(groups)?;
    let grouped_count = array.grouped_count(groups, CountMode::Valid)?;
    assert_eq!(grouped_sum.len(), grouped_count.len());
    assert_eq!(grouped_sum.len(), groups.len());
    Ok(GroupedStats {
        grouped_sum,
        grouped_count,
        groups: groups.iter().enumerate(),
    })
}

struct GroupedStats<'a, I: Iterator<Item = (usize, &'a VecIndices)>> {
    grouped_sum: Float64Array,
    grouped_count: UInt64Array,
    groups: I,
}

impl<'a, I: Iterator<Item = (usize, &'a VecIndices)>> Iterator for GroupedStats<'a, I> {
    type Item = (Stats, &'a VecIndices);

    fn next(&mut self) -> Option<Self::Item> {
        let (index, group) = self.groups.next()?;
        let sum = self
            .grouped_sum
            .get(index)
            .expect("All values in `self.grouped_sum` must be valid");
        let count = self
            .grouped_count
            .get(index)
            .expect("All values in `self.grouped_count` must be valid");
        let mean = calculate_mean(sum, count);
        let stats = Stats { sum, count, mean };
        Some((stats, group))
    }
}

pub fn calculate_mean(sum: f64, count: u64) -> Option<f64> {
    match count {
        0 => None,
        _ => Some(sum / count as f64),
    }
}

pub fn calculate_stddev(stats: Stats, values: impl Iterator<Item = f64>) -> Option<f64> {
    stats.mean.map(|mean| {
        let sum_of_squares = values.map(|value| (value - mean).powi(2)).sum::<f64>();
        (sum_of_squares / stats.count as f64).sqrt()
    })
}
