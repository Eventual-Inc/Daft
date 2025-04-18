use common_error::{DaftError, DaftResult};
use daft_core::{array::ops::IntoGroups, prelude::*};
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
}
