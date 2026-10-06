use common_error::DaftResult;

use crate::{
    array::{
        ops::{DaftCountAggable, DaftSumAggable, GroupIndices, VecIndices},
        prelude::{Float64Array, UInt64Array},
    },
    count_mode::CountMode,
    datatypes::{DataType, Field},
};

pub const VAR_PARTIAL_COUNT_FIELD: &str = "count";
pub const VAR_PARTIAL_MEAN_FIELD: &str = "mean";
pub const VAR_PARTIAL_M2_FIELD: &str = "m2";

pub fn var_partial_fields() -> Vec<Field> {
    vec![
        Field::new(VAR_PARTIAL_COUNT_FIELD, DataType::UInt64),
        Field::new(VAR_PARTIAL_MEAN_FIELD, DataType::Float64),
        Field::new(VAR_PARTIAL_M2_FIELD, DataType::Float64),
    ]
}

pub fn var_partial_dtype() -> DataType {
    DataType::Struct(var_partial_fields())
}

#[derive(Clone, Copy, Default, Debug)]
pub struct Stats {
    pub sum: f64,
    pub count: f64,
    pub mean: Option<f64>,
}

/// Per-partition variance state.
///
/// `count` valid values with mean `mean` and `m2 = sum((x - mean)^2)`. The default
/// (`count == 0`) is the empty state and the identity for [`Self::merge`].
#[derive(Clone, Copy, Default, Debug)]
pub struct VarPartialState {
    pub count: u64,
    pub mean: f64,
    pub m2: f64,
}

impl VarPartialState {
    /// Chan et al. parallel merge. Only combines deviations, so it stays accurate for large
    /// means.
    #[must_use]
    pub fn merge(self, other: Self) -> Self {
        if other.count == 0 {
            return self;
        }
        if self.count == 0 {
            return other;
        }
        let count = self.count + other.count;
        let (n_a, n_b, n) = (self.count as f64, other.count as f64, count as f64);
        let delta = other.mean - self.mean;
        Self {
            count,
            mean: self.mean + delta * n_b / n,
            m2: self.m2 + other.m2 + delta * delta * n_a * n_b / n,
        }
    }

    /// `m2 / (count - ddof)`, or `None` when `count <= ddof`.
    pub fn variance(self, ddof: usize) -> Option<f64> {
        let n = self.count as usize;
        (n > ddof).then(|| self.m2 / (n - ddof) as f64)
    }
}

/// Corrected two-pass `(count, mean, m2)` over non-null values, per Chan et al. eq. (1.7).
///
/// With `s1 = sum(x - mean0)` and `s2 = sum((x - mean0)^2)`: `mean = mean0 + s1 / n`,
/// `m2 = s2 - s1^2 / n`. The `s1` correction removes the error in the provisional
/// `mean0 = sum / count`, which a plain two-pass inherits as a spurious `n * error^2`
/// term and a Welford update cannot fix once `delta / count` falls below `ulp(mean)`.
pub fn calculate_var_partial(stats: Stats, values: impl Iterator<Item = f64>) -> VarPartialState {
    // `mean` is `None` exactly when there were no valid values.
    let Some(mean0) = stats.mean else {
        return VarPartialState::default();
    };

    let n = stats.count;
    // `mul_add` rounds `delta * delta + s2` once instead of twice.
    let (s1, s2) = values.fold((0.0, 0.0), |(s1, s2), value| {
        let delta = value - mean0;
        (s1 + delta, delta.mul_add(delta, s2))
    });

    VarPartialState {
        count: n as u64,
        mean: mean0 + s1 / n,
        m2: s2 - s1 * s1 / n,
    }
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

/// Variance of `values` with `ddof` degrees of freedom, or `None` when `count <= ddof`.
///
/// Shares [`calculate_var_partial`] with the two-stage lowering so the two cannot diverge
/// numerically.
pub fn calculate_variance(
    stats: Stats,
    values: impl Iterator<Item = f64>,
    ddof: usize,
) -> Option<f64> {
    calculate_var_partial(stats, values).variance(ddof)
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

#[cfg(test)]
mod tests {
    use super::{Stats, VarPartialState, calculate_mean, calculate_var_partial};

    /// Mirrors `calculate_stats`, which derives the provisional mean from the sum kernel.
    fn partial_of(values: &[f64]) -> VarPartialState {
        let sum = values.iter().sum::<f64>();
        let count = values.len() as u64;
        let stats = Stats {
            sum,
            count: count as f64,
            mean: calculate_mean(sum, count),
        };
        calculate_var_partial(stats, values.iter().copied())
    }

    fn merge_all(partials: impl IntoIterator<Item = VarPartialState>) -> VarPartialState {
        partials
            .into_iter()
            .fold(VarPartialState::default(), VarPartialState::merge)
    }

    fn assert_close(actual: f64, expected: f64, rel: f64) {
        assert!(
            (actual - expected).abs() <= rel * expected.abs(),
            "expected {expected}, got {actual} (relative tolerance {rel})"
        );
    }

    #[test]
    fn test_var_partial_large_mean_large_partition() {
        // Exercises the per-partition kernel at scale rather than just the merge.
        // Values are multiples of 1/8 and every base has an ulp of at most 1/8, so the shift
        // is exact and the variance is exactly shift invariant; any deviation is algorithm
        // error. Do not change the divisor without rechecking that.
        const N: usize = 20_000;
        let unshifted: Vec<f64> = (0..N).map(|i| ((i * 7919) % 1000) as f64 / 8.0).collect();
        let expected = partial_of(&unshifted).variance(1).unwrap();

        for base in [1e9, 1e12, 1e15] {
            let shifted: Vec<f64> = unshifted.iter().map(|x| base + x).collect();
            assert!(
                shifted.iter().zip(&unshifted).all(|(s, x)| s - base == *x),
                "shift by {base} must not quantise the data"
            );
            let actual = partial_of(&shifted).variance(1).unwrap();
            assert_close(actual, expected, 1e-12);
        }
    }

    #[test]
    fn test_merge_var_partials_empty_is_identity() {
        let empty = VarPartialState::default();
        let merged = merge_all([empty, partial_of(&[1.0, 2.0, 3.0]), empty]);
        assert_eq!(merged.count, 3);
        assert_eq!(merged.variance(1), Some(1.0));
        assert_eq!(merge_all([empty, empty]).count, 0);
    }

    #[test]
    fn test_merge_var_partials_partition_shape_invariance() {
        // How rows split across morsels/partitions must not change the answer, including
        // single-row partials whose own sample variance would be null.
        for base in [0.0, 1e9] {
            let values: Vec<f64> = (1..=8).map(|i| base + f64::from(i)).collect();
            let halves = merge_all([partial_of(&values[..4]), partial_of(&values[4..])]);
            let uneven = merge_all([partial_of(&values[..1]), partial_of(&values[1..])]);
            let singletons = merge_all(values.iter().map(|v| partial_of(std::slice::from_ref(v))));

            let expected = partial_of(&values).variance(1).unwrap();
            assert_close(expected, 6.0, 1e-12);
            for shape in [halves, uneven, singletons] {
                assert_eq!(shape.count, 8);
                assert_close(shape.variance(1).unwrap(), expected, 1e-9);
            }
        }
    }
}
