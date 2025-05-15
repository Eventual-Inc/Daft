use arrow2::bitmap::{Bitmap, MutableBitmap};
use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ops::{arrow2::comparison::build_multi_array_is_equal, IntoGroups},
    prelude::*,
};
use daft_dsl::{
    expr::bound_expr::{BoundAggExpr, BoundExpr},
    AggExpr, WindowBoundary, WindowFrame,
};

use crate::{
    ops::window_states::{create_window_agg_state, WindowAggStateOps},
    RecordBatch,
};

impl RecordBatch {
    pub fn window_grouped_agg(
        &self,
        to_agg: &[BoundAggExpr],
        aliases: &[String],
        partition_by: &[BoundExpr],
    ) -> DaftResult<Self> {
        if partition_by.is_empty() {
            return Err(DaftError::ValueError(
                "Partition by cannot be empty for window aggregation".into(),
            ));
        }

        let agg_exprs = to_agg.to_vec();

        if let [agg_expr] = agg_exprs.as_slice()
            && matches!(agg_expr.as_ref(), AggExpr::MapGroups { .. })
        {
            return Err(DaftError::ValueError(
                "MapGroups not supported in window functions".into(),
            ));
        }

        let partitionby_table = self.eval_expression_list(partition_by)?;
        let (_, partitionvals_indices) = partitionby_table.make_groups()?;

        let mut row_to_partition_mapping = vec![0; self.len()];
        for (partition_idx, indices) in partitionvals_indices.iter().enumerate() {
            for &row_idx in indices {
                row_to_partition_mapping[row_idx as usize] = partition_idx;
            }
        }

        // Take fast path short circuit if there is only 1 partition
        let partition_idx_input = if partitionvals_indices.len() == 1 {
            None
        } else {
            Some(&partitionvals_indices)
        };

        let partitioned_cols = agg_exprs
            .iter()
            .map(|e| self.eval_agg_expression(e, partition_idx_input))
            .collect::<DaftResult<Vec<_>>>()?;

        // Instead of returning partitioned keys + aggregated columns like agg,
        // broadcast the aggregated values back to the original row indices
        let window_cols = partitioned_cols
            .into_iter()
            .zip(aliases)
            .map(|(agg_col, name)| {
                let take_indices = UInt64Array::from((
                    "row_to_partition_mapping",
                    row_to_partition_mapping
                        .iter()
                        .map(|&idx| idx as u64)
                        .collect::<Vec<_>>(),
                ))
                .into_series();
                agg_col.rename(name).take(&take_indices)
            })
            .collect::<DaftResult<Vec<_>>>()?;

        let window_result = Self::from_nonempty_columns(window_cols)?;
        self.union(&window_result)
    }

    pub fn window_agg(&self, to_agg: &BoundAggExpr, name: String) -> DaftResult<Self> {
        if matches!(to_agg.as_ref(), AggExpr::MapGroups { .. }) {
            return Err(DaftError::ValueError(
                "MapGroups not supported in window functions".into(),
            ));
        }

        let agg_result = self.eval_agg_expression(to_agg, None)?;
        let window_col = agg_result.rename(&name);

        // Broadcast the aggregation result to match the length of the partition
        let broadcast_result = window_col.broadcast(self.len())?;

        let window_result = Self::from_nonempty_columns(vec![broadcast_result])?;
        self.union(&window_result)
    }

    fn is_range_frame(start_boundary: &WindowBoundary, end_boundary: &WindowBoundary) -> bool {
        matches!(start_boundary, WindowBoundary::RangeOffset(_))
            || matches!(end_boundary, WindowBoundary::RangeOffset(_))
    }

    fn validate_range_frame_order_by(order_by: &[BoundExpr]) -> DaftResult<()> {
        if order_by.len() != 1 {
            return Err(DaftError::ValueError(
                "Range frame requires exactly one ORDER BY column, multiple columns are not supported".into(),
            ));
        }
        Ok(())
    }

    fn extract_row_offsets(
        start_boundary: &WindowBoundary,
        end_boundary: &WindowBoundary,
    ) -> (Option<i64>, Option<i64>) {
        let start = match start_boundary {
            WindowBoundary::Offset(offset) => Some(*offset),
            _ => None,
        };

        let end = match end_boundary {
            WindowBoundary::Offset(offset) => Some(*offset),
            _ => None,
        };

        (start, end)
    }

    // Helper method to calculate row-based frame boundaries
    fn calculate_row_frame_bounds(
        row_idx: usize,
        start_boundary: Option<i64>,
        end_boundary: Option<i64>,
        total_rows: usize,
    ) -> DaftResult<(usize, usize)> {
        let frame_start = match start_boundary {
            None => 0, // Unbounded preceding
            Some(offset) => row_idx
                .saturating_add_signed(offset as isize)
                .min(total_rows),
        };

        let frame_end = match end_boundary {
            None => total_rows, // Unbounded following
            Some(offset) => (row_idx + 1)
                .saturating_add_signed(offset as isize)
                .min(total_rows),
        };

        if frame_end < frame_start {
            return Err(DaftError::ValueError(
                "Negative frame size is not allowed".into(),
            ));
        }

        Ok((frame_start, frame_end))
    }

    // Helper method to calculate range-based frame boundaries
    #[allow(clippy::too_many_arguments)]
    fn calculate_range_frame_bounds(
        row_idx: usize,
        prev_frame_start: usize,
        prev_frame_end: usize,
        start_boundary: &WindowBoundary,
        end_boundary: &WindowBoundary,
        order_by_col: &Series,
        current_row_order_by: Series,
        descending: bool,
        total_rows: usize,
    ) -> DaftResult<(usize, usize)> {
        let frame_start = match start_boundary {
            WindowBoundary::UnboundedPreceding => 0, // Unbounded preceding
            WindowBoundary::Offset(offset) => row_idx
                .saturating_add_signed(*offset as isize)
                .min(total_rows),
            WindowBoundary::RangeOffset(offset) => {
                let lower_bound = (current_row_order_by.clone() + offset.to_series())?;
                let cmp = |i: usize| -> bool {
                    if descending {
                        order_by_col
                            .slice(i, i + 1)
                            .unwrap()
                            .lte(&lower_bound)
                            .unwrap()
                            .get(0)
                            .unwrap_or(false)
                    } else {
                        order_by_col
                            .slice(i, i + 1)
                            .unwrap()
                            .gte(&lower_bound)
                            .unwrap()
                            .get(0)
                            .unwrap_or(false)
                    }
                };
                let mut idx = prev_frame_start;
                while idx < total_rows {
                    if cmp(idx) {
                        break;
                    }
                    idx += 1;
                }
                idx
            }
            WindowBoundary::UnboundedFollowing => {
                return Err(DaftError::ValueError(
                    "UNBOUNDED FOLLOWING is not valid as a starting frame boundary".into(),
                ));
            }
        };

        let frame_end = match end_boundary {
            WindowBoundary::UnboundedFollowing => total_rows, // Unbounded following
            WindowBoundary::Offset(offset) => {
                (row_idx + 1) // End is exclusive, hence +1
                    .saturating_add_signed(*offset as isize)
                    .min(total_rows)
            }
            WindowBoundary::RangeOffset(offset) => {
                let upper_bound = (current_row_order_by + offset.to_series())?;
                let cmp = |i: usize| -> bool {
                    if descending {
                        order_by_col
                            .slice(i, i + 1)
                            .unwrap()
                            .lt(&upper_bound)
                            .unwrap()
                            .get(0)
                            .unwrap_or(false)
                    } else {
                        order_by_col
                            .slice(i, i + 1)
                            .unwrap()
                            .gt(&upper_bound)
                            .unwrap()
                            .get(0)
                            .unwrap_or(false)
                    }
                };
                let mut idx = prev_frame_end;
                while idx < total_rows {
                    if cmp(idx) {
                        break;
                    }
                    idx += 1;
                }
                idx
            }
            WindowBoundary::UnboundedPreceding => {
                return Err(DaftError::ValueError(
                    "UNBOUNDED PRECEDING is not valid as an ending frame boundary".into(),
                ));
            }
        };

        if frame_end < frame_start {
            return Err(DaftError::ValueError(
                "Negative frame size is not allowed".into(),
            ));
        }

        Ok((frame_start, frame_end))
    }

    // Helper to update incremental state
    fn update_incremental_state(
        prev_frame_start: &mut usize,
        prev_frame_end: &mut usize,
        frame_start: usize,
        frame_end: usize,
        agg_state: &mut Box<dyn WindowAggStateOps>,
    ) -> DaftResult<()> {
        // Remove values that left the window (values that were in the previous window but not in the current one)
        if frame_start > *prev_frame_start {
            agg_state.remove(*prev_frame_start, frame_start)?;
        }

        // Add new values that entered the window (values that are in the current window but weren't in the previous)
        if frame_end > *prev_frame_end {
            agg_state.add(*prev_frame_end, frame_end)?;
        }

        // Update previous boundaries for the next iteration
        *prev_frame_start = frame_start;
        *prev_frame_end = frame_end;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn window_agg_dynamic_frame(
        &self,
        name: String,
        agg_expr: &BoundAggExpr,
        order_by: &[BoundExpr],
        descending: &[bool],
        min_periods: usize,
        dtype: &DataType,
        frame: &WindowFrame,
    ) -> DaftResult<Self> {
        let total_rows = self.len();

        if matches!(frame.start, WindowBoundary::UnboundedFollowing) {
            return Err(DaftError::ValueError(
                "UNBOUNDED FOLLOWING is not valid as a starting frame boundary".into(),
            ));
        }

        if matches!(frame.end, WindowBoundary::UnboundedPreceding) {
            return Err(DaftError::ValueError(
                "UNBOUNDED PRECEDING is not valid as an ending frame boundary".into(),
            ));
        }

        let child_exprs = agg_expr
            .as_ref()
            .children()
            .into_iter()
            .map(BoundExpr::new_unchecked)
            .collect::<Vec<_>>();
        let sources = self.eval_expression_list(&child_exprs)?;
        // Check if we can initialize an incremental state
        match create_window_agg_state(&sources, agg_expr, total_rows)? {
            Some(agg_state) => {
                if Self::is_range_frame(&frame.start, &frame.end) {
                    Self::validate_range_frame_order_by(order_by)?;
                    self.window_agg_range_incremental(
                        &name,
                        &frame.start,
                        &frame.end,
                        &order_by[0],
                        descending[0],
                        min_periods,
                        total_rows,
                        agg_state,
                    )
                } else {
                    let (start, end) = Self::extract_row_offsets(&frame.start, &frame.end);
                    self.window_agg_rows_incremental(
                        &name,
                        start,
                        end,
                        min_periods,
                        total_rows,
                        agg_state,
                    )
                }
            }
            None => {
                if Self::is_range_frame(&frame.start, &frame.end) {
                    Self::validate_range_frame_order_by(order_by)?;
                    self.window_agg_range(
                        agg_expr,
                        &name,
                        dtype,
                        &frame.start,
                        &frame.end,
                        &order_by[0],
                        descending[0],
                        min_periods,
                        total_rows,
                    )
                } else {
                    let (start, end) = Self::extract_row_offsets(&frame.start, &frame.end);
                    self.window_agg_rows(
                        agg_expr,
                        &name,
                        dtype,
                        start,
                        end,
                        min_periods,
                        total_rows,
                    )
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn window_agg_rows_incremental(
        &self,
        name: &str,
        start_boundary: Option<i64>,
        end_boundary: Option<i64>,
        min_periods: usize,
        total_rows: usize,
        mut agg_state: Box<dyn WindowAggStateOps>,
    ) -> DaftResult<Self> {
        // Track previous window boundaries
        let mut prev_frame_start = 0;
        let mut prev_frame_end = 0;
        let mut validity = MutableBitmap::from_len_zeroed(total_rows);

        for row_idx in 0..total_rows {
            // Calculate frame bounds for this row
            let (frame_start, frame_end) = Self::calculate_row_frame_bounds(
                row_idx,
                start_boundary,
                end_boundary,
                total_rows,
            )?;

            let frame_size = frame_end - frame_start;

            // Check min_periods requirement
            if frame_size >= min_periods {
                Self::update_incremental_state(
                    &mut prev_frame_start,
                    &mut prev_frame_end,
                    frame_start,
                    frame_end,
                    &mut agg_state,
                )?;
                validity.set(row_idx, true);
            }

            // Evaluate current state to get the result for this row
            agg_state.evaluate()?;
        }

        let mut validity = Bitmap::from(validity);
        let agg_state = agg_state.build()?.rename(name);
        if let Some(agg_validity) = agg_state.validity() {
            validity = arrow2::bitmap::and(&validity, agg_validity);
        }

        // Build the final result series
        let renamed_result = agg_state.with_validity(Some(validity))?;
        let window_batch = Self::from_nonempty_columns(vec![renamed_result])?;
        self.union(&window_batch)
    }

    #[allow(clippy::too_many_arguments)]
    fn window_agg_rows(
        &self,
        agg_expr: &BoundAggExpr,
        name: &str,
        dtype: &DataType,
        start_boundary: Option<i64>,
        end_boundary: Option<i64>,
        min_periods: usize,
        total_rows: usize,
    ) -> DaftResult<Self> {
        let null_series = Series::full_null(name, dtype, 1);

        // Use the non-optimized implementation (recalculate for each row)
        let mut result_series: Vec<Series> = Vec::with_capacity(total_rows);

        for row_idx in 0..total_rows {
            // Calculate frame bounds for this row
            let (frame_start, frame_end) = Self::calculate_row_frame_bounds(
                row_idx,
                start_boundary,
                end_boundary,
                total_rows,
            )?;

            let frame_size = frame_end - frame_start;

            if frame_size < min_periods {
                // Add a null series for this row
                result_series.push(null_series.clone());
            } else {
                // Calculate aggregation for this frame
                let frame_data = self.slice(frame_start, frame_end)?;
                let agg_result = frame_data.eval_agg_expression(agg_expr, None)?;
                result_series.push(agg_result);
            }
        }

        // Rename the result and create the final record batch
        let final_result_series = Series::concat(&result_series.iter().collect::<Vec<_>>())?;
        let renamed_result = final_result_series.rename(name);
        let window_batch = Self::from_nonempty_columns(vec![renamed_result])?;
        self.union(&window_batch)
    }

    #[allow(clippy::too_many_arguments)]
    fn window_agg_range_incremental(
        &self,
        name: &str,
        start_boundary: &WindowBoundary,
        end_boundary: &WindowBoundary,
        order_by: &BoundExpr,
        descending: bool,
        min_periods: usize,
        total_rows: usize,
        mut agg_state: Box<dyn WindowAggStateOps>,
    ) -> DaftResult<Self> {
        // Use the optimized implementation with incremental state updates
        // Initialize the state for incremental aggregation
        let order_by_col = self.eval_expression(order_by)?;
        let mut validity = MutableBitmap::with_capacity(total_rows);

        // Track previous window boundaries
        let mut prev_frame_start = 0;
        let mut prev_frame_end = 0;

        // Make mutable copies for swapping if needed
        let mut start = start_boundary.clone();
        let mut end = end_boundary.clone();

        if descending
            && matches!(start, WindowBoundary::RangeOffset(_))
            && matches!(end, WindowBoundary::RangeOffset(_))
        {
            std::mem::swap(&mut start, &mut end);
        }

        for row_idx in 0..total_rows {
            let current_row_order_by = order_by_col.slice(row_idx, row_idx + 1)?;

            // Calculate frame bounds for this row using the provided boundaries
            let (frame_start, frame_end) = Self::calculate_range_frame_bounds(
                row_idx,
                prev_frame_start,
                prev_frame_end,
                &start,
                &end,
                &order_by_col,
                current_row_order_by,
                descending,
                total_rows,
            )?;

            let frame_size = frame_end - frame_start;

            // Check min_periods requirement
            if frame_size >= min_periods {
                Self::update_incremental_state(
                    &mut prev_frame_start,
                    &mut prev_frame_end,
                    frame_start,
                    frame_end,
                    &mut agg_state,
                )?;
                validity.push(true);
            } else {
                validity.push(false);
            }

            // Evaluate current state to get the result for this row
            agg_state.evaluate()?;
        }

        let mut validity = Bitmap::from(validity);
        let agg_state = agg_state.build()?.rename(name);
        if let Some(agg_validity) = agg_state.validity() {
            validity = arrow2::bitmap::and(&validity, agg_validity);
        }

        // Build the final result series
        let renamed_result = agg_state.with_validity(Some(validity))?;
        let window_batch = Self::from_nonempty_columns(vec![renamed_result])?;
        self.union(&window_batch)
    }

    #[allow(clippy::too_many_arguments)]
    fn window_agg_range(
        &self,
        agg_expr: &BoundAggExpr,
        name: &str,
        dtype: &DataType,
        start_boundary: &WindowBoundary,
        end_boundary: &WindowBoundary,
        order_by: &BoundExpr,
        descending: bool,
        min_periods: usize,
        total_rows: usize,
    ) -> DaftResult<Self> {
        let order_by_col = self.eval_expression(order_by)?;
        let null_series = Series::full_null(name, dtype, 1);

        // Use the non-optimized implementation (recalculate for each row)
        let mut result_series: Vec<Series> = Vec::with_capacity(total_rows);

        // Make mutable copies for swapping if needed
        let mut start = start_boundary.clone();
        let mut end = end_boundary.clone();

        if descending
            && matches!(start, WindowBoundary::RangeOffset(_))
            && matches!(end, WindowBoundary::RangeOffset(_))
        {
            std::mem::swap(&mut start, &mut end);
        }

        let mut prev_frame_start = 0;
        let mut prev_frame_end = 0;

        for row_idx in 0..total_rows {
            let current_row_order_by = order_by_col.slice(row_idx, row_idx + 1)?;

            // Calculate frame bounds for this row using the provided boundaries
            let (frame_start, frame_end) = Self::calculate_range_frame_bounds(
                row_idx,
                prev_frame_start,
                prev_frame_end,
                &start,
                &end,
                &order_by_col,
                current_row_order_by,
                descending,
                total_rows,
            )?;

            prev_frame_start = frame_start;
            prev_frame_end = frame_end;

            let frame_size = frame_end - frame_start;

            if frame_size < min_periods {
                // Add a null series for this row
                result_series.push(null_series.clone());
            } else {
                // Calculate aggregation for this frame
                let frame_data = self.slice(frame_start, frame_end)?;
                let agg_result = frame_data.eval_agg_expression(agg_expr, None)?;
                result_series.push(agg_result);
            }
        }

        // Rename the result and create the final record batch
        let final_result_series = Series::concat(&result_series.iter().collect::<Vec<_>>())?;
        let renamed_result = final_result_series.rename(name);
        let window_batch = Self::from_nonempty_columns(vec![renamed_result])?;
        self.union(&window_batch)
    }

    pub fn window_row_number(&self, name: String) -> DaftResult<Self> {
        let row_numbers: Vec<u64> = (1..=self.len() as u64).collect();
        let row_number_series = UInt64Array::from((name.as_str(), row_numbers)).into_series();
        let row_number_batch = Self::from_nonempty_columns(vec![row_number_series])?;

        self.union(&row_number_batch)
    }

    pub fn window_rank(
        &self,
        name: String,
        order_by: &[BoundExpr],
        dense: bool,
    ) -> DaftResult<Self> {
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

        let rank_series = UInt64Array::from((name.as_str(), rank_numbers)).into_series();
        let rank_batch = Self::from_nonempty_columns(vec![rank_series])?;

        self.union(&rank_batch)
    }

    pub fn window_offset(
        &self,
        name: String,
        expr: BoundExpr,
        offset: isize,
        default: Option<BoundExpr>,
    ) -> DaftResult<Self> {
        // Short-circuit if offset is 0 - just return the value itself
        if offset == 0 {
            let expr_col = self.eval_expression(&expr)?;
            let renamed_col = expr_col.rename(&name);
            let result_batch = Self::from_nonempty_columns(vec![renamed_col])?;
            return self.union(&result_batch);
        }

        let expr_col = self.eval_expression(&expr)?;
        let abs_offset = offset.unsigned_abs();

        let process_default = |default_opt: &Option<BoundExpr>,
                               target_type: &DataType,
                               slice_start: usize,
                               slice_end: usize,
                               target_length: usize|
         -> DaftResult<Series> {
            if let Some(default_expr) = default_opt {
                // Only evaluate on the slice we need for efficiency
                let default_slice = self.slice(slice_start, slice_end)?;
                let def_col = default_slice.eval_expression(default_expr)?;

                let def_col = if def_col.data_type() != target_type {
                    def_col.cast(target_type)?
                } else {
                    def_col
                };

                if def_col.len() != target_length {
                    def_col.broadcast(target_length)
                } else {
                    Ok(def_col)
                }
            } else {
                // Otherwise, create a column of nulls
                Ok(Series::full_null(
                    expr_col.name(),
                    target_type,
                    target_length,
                ))
            }
        };

        let mut result_col = if self.is_empty() || abs_offset >= self.len() {
            // Special case: empty array or offset exceeds array length
            process_default(&default, expr_col.data_type(), 0, self.len(), self.len())?
        } else if offset > 0 {
            // LEAD: shift values ahead by offset
            let source_values = expr_col.slice(abs_offset, self.len())?;
            let default_values = process_default(
                &default,
                expr_col.data_type(),
                self.len() - abs_offset,
                self.len(),
                abs_offset,
            )?;

            // Construct result by concatenating source and default values
            let cols = vec![&source_values, &default_values];
            Series::concat(&cols)?
        } else {
            // LAG: shift values back by offset
            let default_values =
                process_default(&default, expr_col.data_type(), 0, abs_offset, abs_offset)?;

            let source_values = expr_col.slice(0, self.len() - abs_offset)?;

            // Construct result by concatenating default and source values
            let cols = vec![&default_values, &source_values];
            Series::concat(&cols)?
        };

        result_col = result_col.rename(&name);
        let offset_batch = Self::from_nonempty_columns(vec![result_col])?;

        self.union(&offset_batch)
    }
}
