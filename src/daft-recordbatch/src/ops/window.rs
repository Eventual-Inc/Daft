use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ops::{arrow2::comparison::build_multi_array_is_equal, IntoGroups},
    prelude::*,
};
use daft_dsl::{AggExpr, ExprRef};

use crate::RecordBatch;

impl RecordBatch {
    pub fn window_grouped_agg(
        &self,
        to_agg: &[AggExpr],
        aliases: &[String],
        group_by: &[ExprRef],
    ) -> DaftResult<Self> {
        if group_by.is_empty() {
            return Err(DaftError::ValueError(
                "Group by cannot be empty for window aggregation".into(),
            ));
        }

        let agg_exprs = to_agg.to_vec();

        if matches!(agg_exprs.as_slice(), [AggExpr::MapGroups { .. }]) {
            return Err(DaftError::ValueError(
                "MapGroups not supported in window functions".into(),
            ));
        }

        // Table with just the groupby columns.
        let groupby_table = self.eval_expression_list(group_by)?;

        // Get the grouped values (by indices, one array of indices per group).
        let (_, groupvals_indices) = groupby_table.make_groups()?;

        let mut row_to_group_mapping = vec![0; self.len()];
        for (group_idx, indices) in groupvals_indices.iter().enumerate() {
            for &row_idx in indices {
                row_to_group_mapping[row_idx as usize] = group_idx;
            }
        }

        // Take fast path short circuit if there is only 1 group
        let group_idx_input = if groupvals_indices.len() == 1 {
            None
        } else {
            Some(&groupvals_indices)
        };

        let grouped_cols = agg_exprs
            .iter()
            .map(|e| self.eval_agg_expression(e, group_idx_input))
            .collect::<DaftResult<Vec<_>>>()?;

        // Instead of returning grouped keys + aggregated columns like agg,
        // broadcast the aggregated values back to the original row indices
        let window_cols = grouped_cols
            .into_iter()
            .zip(aliases)
            .map(|(agg_col, name)| {
                // Create a Series of indices to use with take()
                let take_indices = UInt64Array::from((
                    "row_to_group_mapping",
                    row_to_group_mapping
                        .iter()
                        .map(|&idx| idx as u64)
                        .collect::<Vec<_>>(),
                ))
                .into_series();
                agg_col.rename(name).take(&take_indices)
            })
            .collect::<DaftResult<Vec<_>>>()?;

        // Create a new RecordBatch with just the window columns (no group keys)
        let window_result = Self::from_nonempty_columns(window_cols)?;

        // Union the original data with the window result
        self.union(&window_result)
    }

    pub fn window_agg(&self, to_agg: &AggExpr, name: String) -> DaftResult<Self> {
        if matches!(to_agg, AggExpr::MapGroups { .. }) {
            return Err(DaftError::ValueError(
                "MapGroups not supported in window functions".into(),
            ));
        }

        // Since this is a single partition, we can just evaluate the aggregation expression directly
        let agg_result = self.eval_agg_expression(to_agg, None)?;
        let window_col = agg_result.rename(&name);

        // Broadcast the aggregation result to match the length of the partition
        let broadcast_result = window_col.broadcast(self.len())?;

        // Create a new RecordBatch with just the window column
        let window_result = Self::from_nonempty_columns(vec![broadcast_result])?;

        // Union the original data with the window result
        self.union(&window_result)
    }

    pub fn window_row_number(&self, name: String) -> DaftResult<Self> {
        // Create a sequence of row numbers (1-based)
        let row_numbers: Vec<u64> = (1..=self.len() as u64).collect();

        // Create a Series from the row numbers
        let row_number_series = UInt64Array::from((name.as_str(), row_numbers)).into_series();
        let row_number_batch = Self::from_nonempty_columns(vec![row_number_series])?;

        // Union the original data with the row number column
        self.union(&row_number_batch)
    }

    pub fn window_rank(&self, name: String, order_by: &[ExprRef], dense: bool) -> DaftResult<Self> {
        if self.is_empty() {
            // Empty partition case - no work needed
            let rank_series = UInt64Array::from((name.as_str(), Vec::<u64>::new())).into_series();
            let rank_batch = Self::from_nonempty_columns(vec![rank_series])?;
            return self.union(&rank_batch);
        }

        // Get the order_by columns
        let order_by_table = self.eval_expression_list(order_by)?;

        // Create a comparator for checking equality between rows
        let comparator: Box<dyn Fn(usize, usize) -> bool + Send + Sync> =
            build_multi_array_is_equal(
                order_by_table.columns.as_slice(),
                order_by_table.columns.as_slice(),
                &vec![true; order_by_table.columns.len()],
                &vec![true; order_by_table.columns.len()],
            )?;

        // Use iterator to generate rank numbers
        let rank_numbers: Vec<u64> = std::iter::once(1)
            .chain((1..self.len()).scan((1, 1), |(cur_rank, next_rank), i| {
                // Always increment next_rank for regular rank()
                if !dense {
                    *next_rank += 1;
                }

                // Check if the current row has the same values as the previous row
                let is_equal = comparator(i - 1, i);

                if !is_equal {
                    // Different value, update rank
                    if dense {
                        // For dense_rank, just increment by 1
                        *cur_rank += 1;
                    } else {
                        // For rank(), use the next_rank which accounts for ties
                        *cur_rank = *next_rank;
                    }
                }

                Some(*cur_rank)
            }))
            .collect();

        // Create a Series from the rank numbers
        let rank_series = UInt64Array::from((name.as_str(), rank_numbers)).into_series();
        let rank_batch = Self::from_nonempty_columns(vec![rank_series])?;

        // Union the original data with the rank column
        self.union(&rank_batch)
    }
}
