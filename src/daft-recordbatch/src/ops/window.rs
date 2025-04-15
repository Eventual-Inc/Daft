use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ops::{arrow2::comparison::build_multi_array_is_equal, IntoGroups},
    prelude::*,
};
use daft_dsl::{AggExpr, ExprRef, WindowBoundary, WindowFrame, WindowFrameType};

use crate::RecordBatch;

impl RecordBatch {
    pub fn window_agg(
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

    pub fn window_agg_dynamic_frame(
        &self,
        name: String,
        agg_expr: &AggExpr,
        group_by: &[ExprRef],
        _order_by: &[ExprRef],
        _descending: &[bool],
        frame: &WindowFrame,
    ) -> DaftResult<Self> {
        // Check that RANGE frames are not supported yet
        if matches!(frame.frame_type, WindowFrameType::Range) {
            return Err(DaftError::ValueError(
                "RANGE frame type is not supported yet for window_agg_dynamic_frame".into(),
            ));
        }

        if group_by.is_empty() {
            return Err(DaftError::ValueError(
                "Group by cannot be empty for window dynamic frame aggregation".into(),
            ));
        }

        // Table with just the groupby columns.
        let groupby_table = self.eval_expression_list(group_by)?;

        // Get the grouped values (by indices, one array of indices per group).
        let (_, groupvals_indices) = groupby_table.make_groups()?;

        // Prepare vectors to hold our frame indices for each row
        let mut result_values = Vec::with_capacity(self.len());

        // For each partition of rows
        for indices in &groupvals_indices {
            // For each row in the partition
            for (i, &row_idx) in indices.iter().enumerate() {
                // Calculate frame boundaries based on frame specification
                let start_idx = match &frame.start {
                    WindowBoundary::UnboundedPreceding() => 0,
                    WindowBoundary::Offset(offset) => {
                        let pos = i as i64 + offset;
                        pos.max(0) as usize
                    }
                    WindowBoundary::UnboundedFollowing() => {
                        return Err(DaftError::ValueError(
                            "UNBOUNDED FOLLOWING is not valid as a starting frame boundary".into(),
                        ));
                    }
                };

                let end_idx = match &frame.end {
                    WindowBoundary::UnboundedFollowing() => indices.len(),
                    WindowBoundary::Offset(offset) => {
                        let pos = i as i64 + offset;
                        (pos + 1).min(indices.len() as i64) as usize
                    }
                    WindowBoundary::UnboundedPreceding() => {
                        return Err(DaftError::ValueError(
                            "UNBOUNDED PRECEDING is not valid as an ending frame boundary".into(),
                        ));
                    }
                };

                // Validate frame boundaries
                if start_idx > end_idx {
                    return Err(DaftError::ValueError(format!(
                        "Invalid window frame: start_idx ({}) > end_idx ({})",
                        start_idx, end_idx
                    )));
                }

                // Extract the rows in the frame
                let frame_indices = indices[start_idx..end_idx].to_vec();

                // Handle empty frame case
                if frame_indices.is_empty() {
                    continue;
                }

                // Create a UInt64Array for the frame indices
                let frame_idx_array = UInt64Array::from(("indices", frame_indices)).into_series();

                // Take the rows for this frame
                let frame_data = self.take(&frame_idx_array)?;

                // Apply aggregation to the frame data
                // We pass None for groups since we're already processing one group at a time
                let agg_result = frame_data.eval_agg_expression(agg_expr, None)?;

                // Store the aggregated result for this row
                result_values[row_idx as usize] = agg_result;
            }
        }

        // Create a single Series from all the aggregation results
        let final_result_series = Series::concat(&result_values.iter().collect::<Vec<_>>())?;
        let renamed_result = final_result_series.rename(&name);

        // Create a new RecordBatch with the window column
        let window_batch = Self::from_nonempty_columns(vec![renamed_result])?;

        // Union the original data with the window result
        self.union(&window_batch)
    }

    pub fn window_row_number(&self, name: String, group_by: &[ExprRef]) -> DaftResult<Self> {
        if group_by.is_empty() {
            return Err(DaftError::ValueError(
                "Group by cannot be empty for window row number".into(),
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

        // Create row numbers within each group
        let mut row_numbers = vec![0u64; self.len()];
        for indices in &groupvals_indices {
            for (i, &row_idx) in indices.iter().enumerate() {
                row_numbers[row_idx as usize] = (i + 1) as u64;
            }
        }

        // Create a Series from the row numbers
        let row_number_series = UInt64Array::from((name.as_str(), row_numbers)).into_series();
        let row_number_batch = Self::from_nonempty_columns(vec![row_number_series])?;

        // Union the original data with the row number column
        self.union(&row_number_batch)
    }

    pub fn window_rank(
        &self,
        name: String,
        group_by: &[ExprRef],
        order_by: &[ExprRef],
        dense: bool,
    ) -> DaftResult<Self> {
        if group_by.is_empty() {
            return Err(DaftError::ValueError(
                "Group by cannot be empty for window rank".into(),
            ));
        }

        // Filter out any order_by expressions that are also in group_by
        let mut filtered_order_by = Vec::new();
        for o in order_by {
            let o_name = o.name();
            let in_group_by = group_by.iter().any(|g| g.name() == o_name);
            if !in_group_by {
                filtered_order_by.push(o.clone());
            }
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

        // Create rank numbers within each group
        let mut rank_numbers = vec![0u64; self.len()];

        // If order_by is empty after filtering, use all 1s (no ordering within groups)
        if filtered_order_by.is_empty() {
            for indices in &groupvals_indices {
                for &row_idx in indices {
                    rank_numbers[row_idx as usize] = 1;
                }
            }
        } else {
            // Otherwise compute ranks based on order_by columns
            // Evaluate order by expressions
            let order_by_table = self.eval_expression_list(&filtered_order_by)?;

            for indices in &groupvals_indices {
                if indices.len() <= 1 {
                    // Optimization for single-row groups
                    for &row_idx in indices {
                        rank_numbers[row_idx as usize] = 1;
                    }
                    continue;
                }

                // For groups with multiple rows, we need to determine the ranks
                let mut cur_rank = 0;
                let mut next_rank = 0;

                // Get the rows for this group, ordered by the order_by expressions
                let idx_array = UInt64Array::from(("indices", indices.clone())).into_series();
                let group_order_by = order_by_table.take(&idx_array)?;

                // Create a comparator for checking equality between rows
                let comparator: Box<dyn Fn(usize, usize) -> bool + Send + Sync> =
                    build_multi_array_is_equal(
                        group_order_by.columns.as_slice(),
                        group_order_by.columns.as_slice(),
                        &vec![true; group_order_by.columns.len()],
                        &vec![true; group_order_by.columns.len()],
                    )?;

                for (i, &row_idx) in indices.iter().enumerate() {
                    // Always increment next_rank for regular rank()
                    if !dense {
                        next_rank += 1;
                    }

                    // Check if the current row has the same values as the previous row
                    let is_first_row = i == 0;
                    let is_equal = if is_first_row {
                        false
                    } else {
                        comparator(i - 1, i)
                    };

                    if i == 0 || !is_equal {
                        // Different value, update rank
                        if dense {
                            // For dense_rank, just increment by 1
                            cur_rank += 1;
                        } else {
                            // For rank(), use the next_rank which accounts for ties
                            cur_rank = next_rank;
                        }
                    }

                    rank_numbers[row_idx as usize] = cur_rank;
                }
            }
        }

        // Create a Series from the rank numbers
        let rank_series = UInt64Array::from((name.as_str(), rank_numbers)).into_series();
        let rank_batch = Self::from_nonempty_columns(vec![rank_series])?;

        // Union the original data with the rank column
        self.union(&rank_batch)
    }

    pub fn window_offset(
        &self,
        name: String,
        expr: ExprRef,
        group_by: &[ExprRef],
        offset: i64,
        default: Option<ExprRef>,
    ) -> DaftResult<Self> {
        if group_by.is_empty() {
            return Err(DaftError::ValueError(
                "Group by cannot be empty for window offset".into(),
            ));
        }

        // Short-circuit if offset is 0 - just return the value itself
        if offset == 0 {
            // Evaluate the expression
            let expr_col = self.eval_expression(&expr)?;
            let renamed_col = expr_col.rename(&name);
            let result_batch = Self::from_nonempty_columns(vec![renamed_col])?;
            return self.union(&result_batch);
        }

        // Evaluate the expression to get the values we'll need to offset
        let expr_col = self.eval_expression(&expr)?;

        // Evaluate default if provided
        let default_col = if let Some(default_expr) = default {
            Some(self.eval_expression(&default_expr)?)
        } else {
            None
        };

        // Table with just the groupby columns
        let groupby_table = self.eval_expression_list(group_by)?;

        // Get the grouped values (by indices, one array of indices per group)
        let (_, groupvals_indices) = groupby_table.make_groups()?;

        // For the offset indices, we'll use a combination of:
        // 1. A vector of u64 values with placeholder values
        // 2. A validity bitmap to track which entries are null
        let mut indices_values = Vec::with_capacity(self.len());
        let mut indices_validity = Vec::with_capacity(self.len());

        // Initialize with placeholder values (0) and all nulls (false validity)
        for _ in 0..self.len() {
            indices_values.push(0u64);
            indices_validity.push(false);
        }

        // For each group, compute the offset indices
        for indices in &groupvals_indices {
            for (i, &row_idx) in indices.iter().enumerate() {
                let target_idx = i as i64 + offset;

                if target_idx >= 0 && target_idx < indices.len() as i64 {
                    // Within bounds, use the offset value
                    let target_row_idx = indices[target_idx as usize];
                    indices_values[row_idx as usize] = target_row_idx;
                    indices_validity[row_idx as usize] = true;
                }
            }
        }

        // Create a UInt64Array with the indices values and validity
        let take_indices = UInt64Array::new(
            Field::new("indices", DataType::UInt64).into(),
            Box::new(
                arrow2::array::PrimitiveArray::from_vec(indices_values).with_validity(Some(
                    arrow2::bitmap::Bitmap::from_trusted_len_iter(indices_validity.into_iter()),
                )),
            ),
        )?
        .into_series();

        // Use take() to get the offset values
        let mut result_col = expr_col.take(&take_indices)?;

        // If default is provided, use it to fill nulls
        if let Some(default_val) = default_col {
            // Fill nulls with the default value
            result_col = result_col.fill_null(&default_val)?;
        }

        // Rename to the specified name
        result_col = result_col.rename(&name);

        // Create a new RecordBatch with the offset column
        let offset_batch = Self::from_nonempty_columns(vec![result_col])?;

        // Union the original data with the offset column
        self.union(&offset_batch)
    }
}
