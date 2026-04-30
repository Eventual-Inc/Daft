use daft_common_error::DaftResult;

use crate::{
    array::{
        ops::{DaftCountAggable, DaftSumAggable, GroupIndices, VecIndices},
        prelude::{Float64Array, UInt64Array},
    },
    count_mode::CountMode,
};

#[derive(Clone, Copy, Default, Debug)]
pub struct Stats {
    pub sum: f64,
    pub count: f64,
    pub mean: Option<f64>,
}

pub fn calculate_stats(array: &Float64Array) -> DaftResult<Stats> {
    let sum = array.sum()?.get(0);
    let count = array.count(CountMode::Valid)?.get(0);
    let stats = sum
        .zip(count)
        .map_or_else(Default::default, |(sum, count)| Stats {
            sum,
            count: count as _,
            mean: calculate_mean(sum, count),
        });
    Ok(stats)
}

pub fn exact_percentile(values: &Float64Array, percentage: f64) -> DaftResult<Option<f64>> {
    let mut valid_values: Vec<f64> = values.into_iter().flatten().collect();

    if valid_values.is_empty() {
        return Ok(None);
    }

    let rank = percentage * (valid_values.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;

    let (_, lower_ref, greater_partition) =
        valid_values.select_nth_unstable_by(lower, f64::total_cmp);
    let lower_value = *lower_ref;

    if lower == upper {
        Ok(Some(lower_value))
    } else {
        // upper == lower + 1, so upper_value is the min of the greater partition.
        let upper_value = greater_partition
            .iter()
            .copied()
            .min_by(f64::total_cmp)
            .unwrap();
        let weight = rank - lower as f64;
        let percentile = (upper_value - lower_value).mul_add(weight, lower_value);
        Ok(Some(percentile))
    }
}

pub fn is_valid_percentile_percentage(percentage: f64) -> bool {
    (0.0..=1.0).contains(&percentage)
}

pub fn grouped_stats<'a>(
    array: &Float64Array,
    groups: &'a GroupIndices,
) -> DaftResult<impl Iterator<Item = (Stats, &'a VecIndices)>> {
    let grouped_sum = array.grouped_sum(groups)?;
    let grouped_count = array.grouped_count(groups, CountMode::Valid)?;
    debug_assert_eq!(grouped_sum.len(), grouped_count.len());
    debug_assert_eq!(grouped_sum.len(), groups.len());
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
        let sum = self.grouped_sum.get(index);
        let count = self.grouped_count.get(index);
        let stats = sum
            .zip(count)
            .map_or_else(Default::default, |(sum, count)| Stats {
                sum,
                count: count as _,
                mean: calculate_mean(sum, count),
            });
        Some((stats, group))
    }
}

pub fn calculate_mean(sum: f64, count: u64) -> Option<f64> {
    match count {
        0 => None,
        _ => Some(sum / count as f64),
    }
}

pub fn calculate_stddev(
    stats: Stats,
    values: impl Iterator<Item = f64>,
    ddof: usize,
) -> Option<f64> {
    calculate_variance(stats, values, ddof).map(f64::sqrt)
}

pub fn calculate_variance(
    stats: Stats,
    values: impl Iterator<Item = f64>,
    ddof: usize,
) -> Option<f64> {
    stats.mean.and_then(|mean| {
        let n = stats.count as usize;
        if n <= ddof {
            return None; // Not enough data points for the requested ddof
        }
        let sum_of_squares = values.map(|value| (value - mean).powi(2)).sum::<f64>();
        Some(sum_of_squares / (n - ddof) as f64)
    })
}

pub fn calculate_skew(stats: Stats, values: impl Iterator<Item = f64>) -> Option<f64> {
    let count = stats.count;
    stats.mean.map(|mean| {
        // In order to use the same iterator for 2 different calculations
        let (m3, m2) = values.fold((0., 0.), |(m3_acc, m2_acc), v| {
            (
                m3_acc + (v - mean).powi(3),
                (v - mean).mul_add(v - mean, m2_acc),
            )
        });

        (m3 / count) / (m2 / count).powi(3).sqrt()
    })
}
