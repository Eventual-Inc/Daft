use common_error::{DaftError, DaftResult};
use daft_core::{array::ops::IntoGroups, prelude::*};
use daft_dsl::{AggExpr, Expr, ExprRef};

use crate::RecordBatch;

impl RecordBatch {
    pub fn window_agg(&self, to_agg: &[ExprRef], group_by: &[ExprRef]) -> DaftResult<Self> {
        // Dispatch depending on whether we're doing groupby or just a global agg.
        match group_by.len() {
            0 => self.agg_global(to_agg),
            _ => self.window_agg_groupby(to_agg, group_by),
        }
    }

    pub fn window_agg_groupby(&self, to_agg: &[ExprRef], group_by: &[ExprRef]) -> DaftResult<Self> {
        let agg_exprs = to_agg
            .iter()
            .map(|e| match e.as_ref() {
                Expr::Agg(e) => Ok(e),
                _ => Err(DaftError::ValueError(format!(
                    "Trying to run non-Agg expression in Grouped Agg! {e}"
                ))),
            })
            .collect::<DaftResult<Vec<_>>>()?;

        #[cfg(feature = "python")]
        if let [AggExpr::MapGroups { func, inputs }] = &agg_exprs[..] {
            return self.map_groups(func, inputs, group_by);
        }

        // Table with just the groupby columns.
        let groupby_table = self.eval_expression_list(group_by)?;

        // Get the unique group keys (by indices)
        // and the grouped values (also by indices, one array of indices per group).
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

        // Instead of returning grouped keys + aggregated columns, broadcast
        // the aggregated values back to the original row indices
        let window_cols = grouped_cols
            .into_iter()
            .map(|agg_col| {
                // Create a Series of indices to use with take()
                let take_indices = UInt64Array::from((
                    "",
                    row_to_group_mapping
                        .iter()
                        .map(|&idx| idx as u64)
                        .collect::<Vec<_>>(),
                ))
                .into_series();
                agg_col.take(&take_indices)
            })
            .collect::<DaftResult<Vec<_>>>()?;

        for col in window_cols.clone() {
            println!("{}", col);
        }

        // Create a new RecordBatch with just the window columns (no group keys)
        Self::from_nonempty_columns(window_cols)
    }
}
