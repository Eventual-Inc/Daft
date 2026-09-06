use std::sync::Arc;

use common_error::DaftResult;

use crate::{
    array::{
        DataArray, StructArray,
        ops::{
            DaftMergeVarPartialAggable, DaftVarPartialAggable, DaftVarianceAggable, GroupIndices,
        },
    },
    datatypes::{DataType, Field, Float64Type, UInt64Type},
    prelude::IntoSeries,
    utils::stats::{self, VarPartialState},
};

fn build_var_partial_struct(
    parent_name: &str,
    states: Vec<VarPartialState>,
) -> DaftResult<StructArray> {
    let counts: Vec<Option<u64>> = states.iter().map(|s| Some(s.count)).collect();
    let means: Vec<Option<f64>> = states.iter().map(|s| s.mean).collect();
    let m2s: Vec<Option<f64>> = states.iter().map(|s| s.m2).collect();

    let count_field = Arc::new(Field::new(stats::VAR_PARTIAL_COUNT_FIELD, DataType::UInt64));
    let mean_field = Arc::new(Field::new(stats::VAR_PARTIAL_MEAN_FIELD, DataType::Float64));
    let m2_field = Arc::new(Field::new(stats::VAR_PARTIAL_M2_FIELD, DataType::Float64));

    let count_arr = DataArray::<UInt64Type>::from_iter(count_field, counts);
    let mean_arr = DataArray::<Float64Type>::from_iter(mean_field, means);
    let m2_arr = DataArray::<Float64Type>::from_iter(m2_field, m2s);

    let parent_field = Arc::new(Field::new(parent_name, stats::var_partial_dtype()));
    Ok(StructArray::new(
        parent_field,
        vec![
            count_arr.into_series(),
            mean_arr.into_series(),
            m2_arr.into_series(),
        ],
        None,
    ))
}

fn struct_row_to_state(count: Option<u64>, mean: Option<f64>, m2: Option<f64>) -> VarPartialState {
    match count {
        None | Some(0) => VarPartialState {
            count: 0,
            mean: None,
            m2: None,
        },
        Some(n) => match (mean, m2) {
            (Some(mean), Some(m2)) => VarPartialState {
                count: n,
                mean: Some(mean),
                m2: Some(m2),
            },
            // A partial with a positive count must carry a mean and m2; treat
            // malformed rows as identity rather than corrupting the merge.
            _ => VarPartialState {
                count: 0,
                mean: None,
                m2: None,
            },
        },
    }
}

impl DaftVarianceAggable for DataArray<Float64Type> {
    type Output = DaftResult<Self>;

    fn var(&self, ddof: usize) -> Self::Output {
        let stats = stats::calculate_stats(self)?;
        let values = self.into_iter().flatten();
        let variance = stats::calculate_variance(stats, values, ddof);
        Ok(Self::from_iter(
            self.field().clone(),
            std::iter::once(variance),
        ))
    }

    fn grouped_var(&self, groups: &GroupIndices, ddof: usize) -> Self::Output {
        let grouped_variances_iter = stats::grouped_stats(self, groups)?.map(|(stats, group)| {
            let values = group.iter().filter_map(|&index| self.get(index as _));
            stats::calculate_variance(stats, values, ddof)
        });
        Ok(Self::from_iter(
            self.field().clone(),
            grouped_variances_iter,
        ))
    }
}

impl DaftVarPartialAggable for DataArray<Float64Type> {
    type Output = DaftResult<StructArray>;

    fn var_partial(&self) -> Self::Output {
        let state = stats::calculate_var_partial(self.into_iter().flatten());
        build_var_partial_struct(self.name(), vec![state])
    }

    fn grouped_var_partial(&self, groups: &GroupIndices) -> Self::Output {
        let states = groups
            .iter()
            .map(|group| {
                let values = group.iter().filter_map(|&index| self.get(index as _));
                stats::calculate_var_partial(values)
            })
            .collect();
        build_var_partial_struct(self.name(), states)
    }
}

impl DaftMergeVarPartialAggable for StructArray {
    type Output = DaftResult<Self>;

    fn merge_var_partial(&self) -> Self::Output {
        let counts = self.get(stats::VAR_PARTIAL_COUNT_FIELD)?.u64()?.clone();
        let means = self.get(stats::VAR_PARTIAL_MEAN_FIELD)?.f64()?.clone();
        let m2s = self.get(stats::VAR_PARTIAL_M2_FIELD)?.f64()?.clone();
        let partials =
            (0..self.len()).map(|i| struct_row_to_state(counts.get(i), means.get(i), m2s.get(i)));
        let merged = stats::merge_var_partials(partials);
        build_var_partial_struct(self.name(), vec![merged])
    }

    fn grouped_merge_var_partial(&self, groups: &GroupIndices) -> Self::Output {
        let counts = self.get(stats::VAR_PARTIAL_COUNT_FIELD)?.u64()?.clone();
        let means = self.get(stats::VAR_PARTIAL_MEAN_FIELD)?.f64()?.clone();
        let m2s = self.get(stats::VAR_PARTIAL_M2_FIELD)?.f64()?.clone();
        let states = groups
            .iter()
            .map(|group| {
                let partials = group.iter().map(|&index| {
                    let i = index as usize;
                    struct_row_to_state(counts.get(i), means.get(i), m2s.get(i))
                });
                stats::merge_var_partials(partials)
            })
            .collect();
        build_var_partial_struct(self.name(), states)
    }
}
