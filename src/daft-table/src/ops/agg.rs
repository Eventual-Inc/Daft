use daft_core::{array::ops::IntoGroups, datatypes::UInt64Array, series::IntoSeries};
use daft_dsl::Expr;

use common_error::{DaftError, DaftResult};

use crate::Table;

impl Table {
    pub fn agg(&self, to_agg: &[Expr], group_by: &[Expr]) -> DaftResult<Table> {
        // Dispatch depending on whether we're doing groupby or just a global agg.
        match group_by.len() {
            0 => self.agg_global(to_agg),
            _ => self.agg_groupby(to_agg, group_by),
        }
    }

    pub fn agg_global(&self, to_agg: &[Expr]) -> DaftResult<Table> {
        self.eval_expression_list(to_agg)
    }

    pub fn agg_groupby(&self, to_agg: &[Expr], group_by: &[Expr]) -> DaftResult<Table> {
        // Table with just the groupby columns.
        let groupby_table = self.eval_expression_list(group_by)?;

        // Get the unique group keys (by indices)
        // and the grouped values (also by indices, one array of indices per group).
        let (groupkey_indices, groupvals_indices) = groupby_table.make_groups()?;

        // Table with the aggregated (deduplicated) group keys.
        let groupkeys_table = {
            let indices_as_series = UInt64Array::from(("", groupkey_indices)).into_series();
            groupby_table.take(&indices_as_series)?
        };
        let agg_exprs = to_agg
            .iter()
            .map(|e| match e {
                Expr::Agg(e) => Ok(e),
                _ => Err(DaftError::ValueError(format!(
                    "Trying to run non-Agg expression in Grouped Agg! {e}"
                ))),
            })
            .collect::<DaftResult<Vec<_>>>()?;

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

        // Combine the groupkey columns and the aggregation result columns.
        Self::from_columns([&groupkeys_table.columns[..], &grouped_cols].concat())
    }
}
