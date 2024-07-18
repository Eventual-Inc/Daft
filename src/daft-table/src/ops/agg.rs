use daft_core::{array::ops::IntoGroups, datatypes::UInt64Array, series::IntoSeries, Series};
use daft_dsl::{functions::FunctionExpr, AggExpr, Expr};

use common_error::{DaftError, DaftResult};

use crate::Table;

use daft_dsl::ExprRef;

impl Table {
    pub fn agg(&self, to_agg: &[ExprRef], group_by: &[ExprRef]) -> DaftResult<Table> {
        // Dispatch depending on whether we're doing groupby or just a global agg.
        match group_by.len() {
            0 => self.agg_global(to_agg),
            _ => self.agg_groupby(to_agg, group_by),
        }
    }

    pub fn agg_global(&self, to_agg: &[ExprRef]) -> DaftResult<Table> {
        self.eval_expression_list(to_agg)
    }

    pub fn agg_groupby(&self, to_agg: &[ExprRef], group_by: &[ExprRef]) -> DaftResult<Table> {
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
        let (groupkey_indices, groupvals_indices) = groupby_table.make_groups()?;

        // Table with the aggregated (deduplicated) group keys.
        let groupkeys_table = {
            let indices_as_series = UInt64Array::from(("", groupkey_indices)).into_series();
            groupby_table.take(&indices_as_series)?
        };

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
        Self::from_nonempty_columns([&groupkeys_table.columns[..], &grouped_cols].concat())
    }

    #[cfg(feature = "python")]
    pub fn map_groups(
        &self,
        func: &FunctionExpr,
        inputs: &[ExprRef],
        group_by: &[ExprRef],
    ) -> DaftResult<Table> {
        use daft_core::schema::Schema;

        let udf = match func {
            FunctionExpr::Python(udf) => udf,
            _ => {
                return Err(DaftError::ComputeError(
                    "Trying to run non-UDF function in MapGroups!".to_string(),
                ))
            }
        };

        // Table with just the groupby columns.
        let groupby_table = self.eval_expression_list(group_by)?;

        // Get the unique group keys (by indices)
        // and the grouped values (also by indices, one array of indices per group).
        let (groupkey_indices, groupvals_indices) = groupby_table.make_groups()?;

        let evaluated_inputs = inputs
            .iter()
            .map(|e| self.eval_expression(e))
            .collect::<DaftResult<Vec<_>>>()?;

        // Take fast path short circuit if there is only 1 group
        let (groupkeys_table, grouped_col) = if groupvals_indices.is_empty() {
            let empty_groupkeys_table = Table::empty(Some(groupby_table.schema.clone()))?;
            let empty_udf_output_col = Series::empty(
                evaluated_inputs
                    .first()
                    .map_or_else(|| "output", |s| s.name()),
                &udf.return_dtype,
            );
            (empty_groupkeys_table, empty_udf_output_col)
        } else if groupvals_indices.len() == 1 {
            let grouped_col = udf.call_udf(evaluated_inputs.as_slice())?;
            let groupkeys_table = {
                let indices_as_series = UInt64Array::from(("", groupkey_indices)).into_series();
                groupby_table.take(&indices_as_series)?
            };
            (groupkeys_table, grouped_col)
        } else {
            let grouped_results = groupkey_indices
                .iter()
                .zip(groupvals_indices.iter())
                .map(|(groupkey_index, groupval_indices)| {
                    let evaluated_grouped_col = {
                        // Convert group indices to Series
                        let indices_as_series =
                            UInt64Array::from(("", groupval_indices.clone())).into_series();

                        // Take each input Series by the group indices
                        let input_groups = evaluated_inputs
                            .iter()
                            .map(|s| s.take(&indices_as_series))
                            .collect::<DaftResult<Vec<_>>>()?;

                        // Call the UDF on the grouped inputs
                        udf.call_udf(input_groups.as_slice())?
                    };

                    let broadcasted_groupkeys_table = {
                        // Convert groupkey indices to Series
                        let groupkey_indices_as_series =
                            UInt64Array::from(("", vec![*groupkey_index])).into_series();

                        // Take the group keys by the groupkey indices
                        let groupkeys_table = groupby_table.take(&groupkey_indices_as_series)?;

                        // Broadcast the group keys to the length of the grouped column, because output of UDF can be more than one row
                        let broadcasted_groupkeys = groupkeys_table
                            .columns
                            .iter()
                            .map(|c| c.broadcast(evaluated_grouped_col.len()))
                            .collect::<DaftResult<Vec<_>>>()?;

                        // Combine the broadcasted group keys into a Table
                        Table::from_nonempty_columns(broadcasted_groupkeys)?
                    };

                    Ok((broadcasted_groupkeys_table, evaluated_grouped_col))
                })
                .collect::<DaftResult<Vec<_>>>()?;

            let series_refs = grouped_results.iter().map(|(_, s)| s).collect::<Vec<_>>();
            let concatenated_grouped_col = Series::concat(series_refs.as_slice())?;

            let table_refs = grouped_results.iter().map(|(t, _)| t).collect::<Vec<_>>();
            let concatenated_groupkeys_table = Table::concat(table_refs.as_slice())?;

            (concatenated_groupkeys_table, concatenated_grouped_col)
        };

        // Broadcast either the keys or the grouped_cols, depending on which is unit-length
        let final_len = grouped_col.len();
        let final_columns = [&groupkeys_table.columns[..], &[grouped_col]].concat();
        let final_schema = Schema::new(final_columns.iter().map(|s| s.field().clone()).collect())?;
        Self::new_with_broadcast(final_schema, final_columns, final_len)
    }
}
