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

/// Numerically stable partial variance state, per Chan et al. parallel variance update.
///
/// `count` is the number of valid (non-null) values observed, `mean` is their mean and
/// `m2` is the sum of squared deviations from the mean, i.e. `sum((x - mean)^2)`.
/// The finalized (population) variance is `m2 / count` and the sample variance with
/// `ddof` degrees of freedom is `m2 / (count - ddof)`.
///
/// `mean` and `m2` are `None` when `count == 0`.
#[derive(Clone, Copy, Default, Debug)]
pub struct VarPartialState {
    pub count: u64,
    pub mean: Option<f64>,
    pub m2: Option<f64>,
}

/// Corrected two-pass computation of `(count, mean, m2)` over non-null values.
///
/// This is the per-partition step of the parallel variance algorithm. It starts from the
/// provisional mean already in `stats` (`sum / count`) and then corrects both the mean and
/// the second moment with the residual `s1`, per Chan et al. eq. (1.7):
///
/// ```text
/// s1   = sum(x - mean0)            s2 = sum((x - mean0)^2)
/// mean = mean0 + s1 / n            m2 = s2 - s1^2 / n
/// ```
///
/// It never forms `sum(x^2)`, so unlike `E(x^2) - E(x)^2` it does not catastrophically
/// cancel when the mean is large relative to the variance (e.g. `[1e9 + 1, 1e9 + 2,
/// 1e9 + 3]`, which collapsed to `0.0` before this was introduced).
///
/// The `s1` correction is what makes this robust where the two obvious alternatives are
/// not. A plain two-pass `sum((x - mean0)^2)` inherits the error in `mean0` from the
/// summation kernel as a spurious `n * error^2` term, which matters because the grouped
/// path sums with a sequential fold. A Welford update (`mean += delta / count`) instead
/// stops moving the mean entirely once `delta / count` drops below `ulp(mean)` -- around
/// `1e15` with a spread of ~1 the running mean freezes near its first few values and the
/// result is off by tens of percent. Correcting an explicit `mean0` avoids both.
pub fn calculate_var_partial(stats: Stats, values: impl Iterator<Item = f64>) -> VarPartialState {
    let Some(mean0) = stats.mean else {
        // `mean` is `None` exactly when there were no valid values.
        return VarPartialState {
            count: 0,
            mean: None,
            m2: None,
        };
    };

    let count = stats.count as u64;
    let n = stats.count;
    let (s1, s2) = values.fold((0.0, 0.0), |(s1, s2), value| {
        let delta = value - mean0;
        (s1 + delta, s2 + delta * delta)
    });

    VarPartialState {
        count,
        mean: Some(mean0 + s1 / n),
        m2: Some(s2 - s1 * s1 / n),
    }
}

/// Chan et al. parallel merge of per-partition `(count, mean, m2)` states.
///
/// Each input with `count == 0` (or missing `mean`/`m2`) is an identity and is skipped.
/// The merge is associative up to floating-point rounding and only ever combines
/// deviations (`delta = mean_b - mean_a`), so it stays accurate when the global mean
/// is large and the variance is small.
pub fn merge_var_partials(partials: impl Iterator<Item = VarPartialState>) -> VarPartialState {
    let mut count: u64 = 0;
    let mut mean = 0.0;
    let mut m2 = 0.0;
    for partial in partials {
        if partial.count == 0 {
            continue;
        }
        let (other_n, other_mean, other_m2) = match (partial.mean, partial.m2) {
            (Some(other_mean), Some(other_m2)) => (partial.count, other_mean, other_m2),
            _ => continue,
        };
        if count == 0 {
            count = other_n;
            mean = other_mean;
            m2 = other_m2;
        } else {
            let delta = other_mean - mean;
            let new_count = count + other_n;
            let new_mean = mean + delta * (other_n as f64) / (new_count as f64);
            let new_m2 = m2
                + other_m2
                + delta * delta * (count as f64) * (other_n as f64) / (new_count as f64);
            count = new_count;
            mean = new_mean;
            m2 = new_m2;
        }
    }
    if count == 0 {
        VarPartialState {
            count: 0,
            mean: None,
            m2: None,
        }
    } else {
        VarPartialState {
            count,
            mean: Some(mean),
            m2: Some(m2),
        }
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
/// This goes through [`calculate_var_partial`], the same kernel the two-stage `VarPartial`
/// lowering uses, so `Series::var` and `df.agg(col(..).var())` cannot diverge numerically.
pub fn calculate_variance(
    stats: Stats,
    values: impl Iterator<Item = f64>,
    ddof: usize,
) -> Option<f64> {
    let state = calculate_var_partial(stats, values);
    let n = state.count as usize;
    if n <= ddof {
        return None; // Not enough data points for the requested ddof
    }
    state.m2.map(|m2| m2 / (n - ddof) as f64)
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
    use super::{
        Stats, VarPartialState, calculate_mean, calculate_var_partial, calculate_variance,
        merge_var_partials,
    };

    /// Mirrors `calculate_stats`, which derives the provisional mean from the sum kernel.
    fn stats_of(values: &[f64]) -> Stats {
        let sum = values.iter().sum::<f64>();
        let count = values.len() as u64;
        Stats {
            sum,
            count: count as f64,
            mean: calculate_mean(sum, count),
        }
    }

    fn partial_of(values: &[f64]) -> VarPartialState {
        calculate_var_partial(stats_of(values), values.iter().copied())
    }

    fn var_from_state(state: VarPartialState, ddof: usize) -> Option<f64> {
        if (state.count as usize) <= ddof {
            return None;
        }
        state.m2.map(|m2| m2 / (state.count as f64 - ddof as f64))
    }

    fn assert_close(actual: f64, expected: f64, rel: f64) {
        assert!(
            (actual - expected).abs() <= rel * expected.abs(),
            "expected {expected}, got {actual} (relative tolerance {rel})"
        );
    }

    #[test]
    fn test_var_partial_large_mean() {
        // Regression test for https://github.com/Eventual-Inc/Daft/issues/7468:
        // `E(x^2) - E(x)^2` collapses to 0 for `[1e9 + 1, 1e9 + 2, 1e9 + 3]`.
        let values = [1e9 + 1.0, 1e9 + 2.0, 1e9 + 3.0];
        let state = partial_of(&values);
        assert_eq!(state.count, 3);
        assert_eq!(var_from_state(state, 1), Some(1.0));
        assert_eq!(var_from_state(state, 0), Some(2.0 / 3.0));
    }

    #[test]
    fn test_var_partial_large_mean_large_partition() {
        // The other large-mean tests use three values, so they only exercise the merge.
        // Here a single partition holds 20k values, which is where the *per-partition*
        // kernel decides the answer -- and where a Welford running mean silently stops
        // updating, because `delta / count` falls below `ulp(1e15)`.
        //
        // Every value is a multiple of 1/8, and 1e9, 1e12 and 1e15 all have an ulp of at
        // most 1/8, so `base + x` is exact and the variance is *exactly* shift invariant.
        // Any deviation from the unshifted variance is therefore pure algorithm error.
        const N: usize = 20_000;
        let unshifted: Vec<f64> = (0..N).map(|i| ((i * 7919) % 1000) as f64 / 8.0).collect();
        let expected = var_from_state(partial_of(&unshifted), 1).unwrap();

        for base in [1e9, 1e12, 1e15] {
            let shifted: Vec<f64> = unshifted.iter().map(|x| base + x).collect();
            assert!(
                shifted.iter().zip(&unshifted).all(|(s, x)| s - base == *x),
                "shift by {base} must not quantise the data"
            );
            let actual = var_from_state(partial_of(&shifted), 1).unwrap();
            assert_close(actual, expected, 1e-12);
        }
    }

    #[test]
    fn test_merge_var_partials_singletons() {
        // Merging single-row partials must recover the joint variance, including
        // the `n == 1` case where a per-partition sample variance would be null.
        let partials = [1e9 + 1.0, 1e9 + 2.0, 1e9 + 3.0]
            .into_iter()
            .map(|v| partial_of(&[v]));
        let merged = merge_var_partials(partials);
        assert_eq!(merged.count, 3);
        assert_eq!(var_from_state(merged, 1), Some(1.0));
    }

    #[test]
    fn test_merge_var_partials_empty_is_identity() {
        let empty = VarPartialState {
            count: 0,
            mean: None,
            m2: None,
        };
        let state = partial_of(&[1.0, 2.0, 3.0]);
        let merged = merge_var_partials([empty, state, empty].into_iter());
        assert_eq!(merged.count, 3);
        assert_eq!(var_from_state(merged, 1), Some(1.0));
        assert_eq!(merge_var_partials([empty, empty].into_iter()).count, 0);
    }

    #[test]
    fn test_merge_var_partials_partition_shape_invariance() {
        // How the rows happen to be split across morsels/partitions must not change the
        // answer: the merge is what makes the two-stage lowering agree with a single pass.
        for base in [0.0, 1e9] {
            let values: Vec<f64> = (1..=8).map(|i| base + f64::from(i)).collect();
            let whole = partial_of(&values);
            let halves = merge_var_partials(
                [partial_of(&values[..4]), partial_of(&values[4..])].into_iter(),
            );
            let uneven = merge_var_partials(
                [partial_of(&values[..1]), partial_of(&values[1..])].into_iter(),
            );
            let singletons =
                merge_var_partials(values.iter().map(|v| partial_of(std::slice::from_ref(v))));

            let expected = var_from_state(whole, 1).unwrap();
            assert_close(expected, 6.0, 1e-12);
            for shape in [halves, uneven, singletons] {
                assert_eq!(shape.count, 8);
                assert_close(var_from_state(shape, 1).unwrap(), expected, 1e-9);
            }
        }
    }

    #[test]
    fn test_calculate_variance_matches_var_partial() {
        // `Series::var` goes through `calculate_variance` while the query engine goes
        // through `calculate_var_partial`; they must not be able to disagree.
        for values in [
            vec![1.0, 2.0, 3.0, 4.0, 5.0],
            vec![1e9 + 1.0, 1e9 + 2.0, 1e9 + 3.0],
            vec![5.0],
            vec![],
        ] {
            let stats = stats_of(&values);
            for ddof in [0, 1] {
                assert_eq!(
                    calculate_variance(stats, values.iter().copied(), ddof),
                    var_from_state(calculate_var_partial(stats, values.iter().copied()), ddof),
                    "values={values:?} ddof={ddof}"
                );
            }
        }
    }
}
