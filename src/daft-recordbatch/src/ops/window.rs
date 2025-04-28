use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ops::{arrow2::comparison::build_multi_array_is_equal, IntoGroups},
    prelude::*,
};
use daft_dsl::{AggExpr, ExprRef, WindowBoundary, WindowFrame, WindowFrameType};

use crate::RecordBatch;

impl RecordBatch {
    pub fn window_grouped_agg(
        &self,
        to_agg: &[AggExpr],
        aliases: &[String],
        partition_by: &[ExprRef],
    ) -> DaftResult<Self> {
        if partition_by.is_empty() {
            return Err(DaftError::ValueError(
                "Partition by cannot be empty for window aggregation".into(),
            ));
        }

        let agg_exprs = to_agg.to_vec();

        if matches!(agg_exprs.as_slice(), [AggExpr::MapGroups { .. }]) {
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

    pub fn window_agg(&self, to_agg: &AggExpr, name: String) -> DaftResult<Self> {
        if matches!(to_agg, AggExpr::MapGroups { .. }) {
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

    #[allow(clippy::too_many_arguments)]
    pub fn window_agg_dynamic_frame(
        &self,
        name: String,
        agg_expr: &AggExpr,
        min_periods: usize,
        dtype: &DataType,
        frame: &WindowFrame,
    ) -> DaftResult<Self> {
        if matches!(frame.frame_type, WindowFrameType::Range) {
            return Err(DaftError::ValueError(
                "RANGE frame type is not supported yet for window_agg_dynamic_frame".into(),
            ));
        }

        let total_rows = self.len();

        // Convert WindowBoundary to Option<i64>
        let start_boundary = match &frame.start {
            WindowBoundary::UnboundedPreceding() => None,
            WindowBoundary::Offset(offset) => Some(*offset),
            WindowBoundary::UnboundedFollowing() => {
                return Err(DaftError::ValueError(
                    "UNBOUNDED FOLLOWING is not valid as a starting frame boundary".into(),
                ));
            }
        };

        let end_boundary = match &frame.end {
            WindowBoundary::UnboundedFollowing() => None,
            WindowBoundary::Offset(offset) => Some(*offset),
            WindowBoundary::UnboundedPreceding() => {
                return Err(DaftError::ValueError(
                    "UNBOUNDED PRECEDING is not valid as an ending frame boundary".into(),
                ));
            }
        };

        self.window_agg_rows_between(
            agg_expr,
            &name,
            dtype,
            start_boundary,
            end_boundary,
            min_periods,
            total_rows,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn window_agg_rows_between(
        &self,
        agg_expr: &AggExpr,
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

            let Some(frame_size) = frame_end.checked_sub(frame_start) else {
                return Err(DaftError::ValueError(
                    "Negative frame size is not allowed".into(),
                ));
            };

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

        let rank_series = UInt64Array::from((name.as_str(), rank_numbers)).into_series();
        let rank_batch = Self::from_nonempty_columns(vec![rank_series])?;

        self.union(&rank_batch)
    }

    pub fn window_offset(
        &self,
        name: String,
        expr: ExprRef,
        offset: isize,
        default: Option<ExprRef>,
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

        let process_default = |default_opt: &Option<ExprRef>,
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
