use std::sync::Arc;

use arrow::array::Float64Builder;
use common_error::DaftResult;

use crate::{
    array::{
        ListArray,
        ops::{DaftPercentileAggable, GroupIndices},
    },
    datatypes::{DataType, Field, Float64Array},
    utils::stats,
};

impl DaftPercentileAggable for Float64Array {
    type Output = DaftResult<Self>;

    fn percentile(&self, percentage: f64) -> Self::Output {
        let mut builder = Float64Builder::with_capacity(1);
        builder.append_option(stats::exact_percentile(self, percentage)?);
        Self::from_arrow(self.field.clone(), Arc::new(builder.finish()))
    }

    fn grouped_percentile(&self, groups: &GroupIndices, percentage: f64) -> Self::Output {
        let mut builder = Float64Builder::with_capacity(groups.len());
        for group in groups {
            let values = group
                .iter()
                .map(|&index| self.get(index as usize))
                .collect();
            builder.append_option(stats::exact_percentile(&values, percentage)?);
        }
        Self::from_arrow(self.field.clone(), Arc::new(builder.finish()))
    }
}

impl DaftPercentileAggable for ListArray {
    type Output = DaftResult<Float64Array>;

    fn percentile(&self, percentage: f64) -> Self::Output {
        let mut row_iter = (0..self.len()).map(|i| i as u64);
        let percentile = percentile_for_rows(self, &mut row_iter, self.len(), percentage)?;

        let mut builder = Float64Builder::with_capacity(1);
        builder.append_option(percentile);
        Float64Array::from_arrow(
            Field::new(self.name(), DataType::Float64),
            Arc::new(builder.finish()),
        )
    }

    fn grouped_percentile(&self, groups: &GroupIndices, percentage: f64) -> Self::Output {
        let mut builder = Float64Builder::with_capacity(groups.len());
        for group in groups {
            let mut row_iter = group.iter().copied();
            builder.append_option(percentile_for_rows(
                self,
                &mut row_iter,
                group.len(),
                percentage,
            )?);
        }
        Float64Array::from_arrow(
            Field::new(self.name(), DataType::Float64),
            Arc::new(builder.finish()),
        )
    }
}

fn percentile_for_rows(
    list_array: &ListArray,
    rows: &mut dyn Iterator<Item = u64>,
    capacity: usize,
    percentage: f64,
) -> DaftResult<Option<f64>> {
    let child = list_array.flat_child.f64()?;
    let mut values_builder = Float64Builder::with_capacity(capacity);

    for row_idx in rows {
        let row_idx = row_idx as usize;
        if let Some(nulls) = list_array.nulls()
            && !nulls.is_valid(row_idx)
        {
            continue;
        }

        let (start, end) = list_array.offsets().start_end(row_idx);
        for value_idx in start..end {
            values_builder.append_option(child.get(value_idx));
        }
    }

    let values = Float64Array::from_arrow(
        Field::new(list_array.name(), DataType::Float64),
        Arc::new(values_builder.finish()),
    )?;
    stats::exact_percentile(&values, percentage)
}
