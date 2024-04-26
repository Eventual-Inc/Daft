use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ops::{as_arrow::AsArrow, IntoGroups},
    datatypes::UInt64Array,
    IntoSeries,
};
use daft_dsl::ExprRef;

use crate::Table;

impl Table {
    pub fn pivot(
        &self,
        group_by: ExprRef,
        pivot_col: ExprRef,
        values_col: ExprRef,
    ) -> DaftResult<Table> {
        let groupby_series = self.eval_expression(&group_by)?;
        let pivot_series = self.eval_expression(&pivot_col)?;
        let value_series = self.eval_expression(&values_col)?;

        let (groupkey_indices, groupvals_indices) = groupby_series.make_groups()?;
        let (pivotkey_indices, pivotvals_indices) = pivot_series.make_groups()?;

        let groupkeys_series = {
            let indices_as_series = UInt64Array::from(("", groupkey_indices)).into_series();
            groupby_series.take(&indices_as_series)?
        };
        let pivotkeys_series = {
            let indices_as_series = UInt64Array::from(("", pivotkey_indices.clone())).into_series();
            pivot_series.take(&indices_as_series)?
        };

        let pivot_names = pivotkeys_series.to_str_values()?;
        let pivot_keys_to_names = pivot_names
            .utf8()?
            .as_arrow()
            .iter()
            .zip(pivotkey_indices.iter())
            .map(|(name, idx)| (idx, name))
            .collect::<std::collections::HashMap<_, _>>();

        let mut pivot_key_to_value_indices = std::collections::HashMap::new();
        for g_indices in groupvals_indices.iter() {
            for (p_key, p_indices) in pivotkey_indices.iter().zip(pivotvals_indices.iter()) {
                let matches: Vec<(u64, u64)> = g_indices
                    .iter()
                    .flat_map(|&item1| p_indices.iter().map(move |&item2| (item1, item2)))
                    .filter(|&(item1, item2)| item1 == item2)
                    .collect();

                let idx = match matches.len() {
                    1 => Some(matches[0].0),
                    0 => None,
                    _ => {
                        return Err(DaftError::ComputeError(
                            "Pivot column has more than one unique value".to_string(),
                        ));
                    }
                };
                pivot_key_to_value_indices
                    .entry(p_key)
                    .or_insert_with(Vec::new)
                    .push(idx);
            }
        }
        let pivoted_cols = pivot_key_to_value_indices
            .iter()
            .map(|(p_key, indices)| {
                let indices_as_arrow = arrow2::array::UInt64Array::from_iter(indices.iter());
                let indices_as_series =
                    UInt64Array::from(("", Box::new(indices_as_arrow))).into_series();
                let values = value_series.take(&indices_as_series)?;
                let name = pivot_keys_to_names[p_key].unwrap();
                Ok(values.rename(name))
            })
            .collect::<DaftResult<Vec<_>>>()?;

        Self::from_columns([&[groupkeys_series], &pivoted_cols[..]].concat())
    }
}
