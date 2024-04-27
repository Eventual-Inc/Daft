use crate::Table;
use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ops::{as_arrow::AsArrow, IntoGroups},
    datatypes::UInt64Array,
    IntoSeries,
};
use daft_dsl::ExprRef;

impl Table {
    pub fn pivot(
        &self,
        group_by: ExprRef,
        pivot_col: ExprRef,
        values_col: ExprRef,
        names: Vec<String>,
    ) -> DaftResult<Table> {
        let groupby_series = self.eval_expression(&group_by)?;
        let pivot_series = self.eval_expression(&pivot_col)?;
        let value_series = self.eval_expression(&values_col)?;

        let (group_key_indices, group_vals_indices) = groupby_series.make_groups()?;
        let (pivot_key_indices, pivot_vals_indices) = pivot_series.make_groups()?;

        let group_keys_series = {
            let indices_as_series = UInt64Array::from(("", group_key_indices)).into_series();
            groupby_series.take(&indices_as_series)?
        };
        let pivot_keys_series = {
            let indices_as_series =
                UInt64Array::from(("", pivot_key_indices.clone())).into_series();
            pivot_series.take(&indices_as_series)?
        };
        let pivot_names_as_series = pivot_keys_series.to_str_values()?;
        let pivot_names = pivot_names_as_series.utf8()?.as_arrow();
        if pivot_names.len() > names.len() {
            return Err(DaftError::ComputeError(format!(
                "Pivot column has more values than provided names: {} vs {}",
                pivot_names.len(),
                names.len()
            )));
        }

        let mut pivot_name_to_pivot_key_idx = Vec::new();
        for name in names.iter() {
            let mut found = false;
            for (idx, pivot_name) in pivot_names.iter().enumerate() {
                if name == pivot_name.unwrap() {
                    found = true;
                    pivot_name_to_pivot_key_idx.push((name, Some(idx)));
                    break;
                }
            }
            if !found {
                pivot_name_to_pivot_key_idx.push((name, None));
            }
        }

        let mut pivot_name_to_value_indices = std::collections::HashMap::new();
        for name in names.iter() {
            pivot_name_to_value_indices.insert(name, Vec::new());
        }
        let group_vals_indices_hashset: Vec<std::collections::HashSet<_>> = group_vals_indices
            .iter()
            .map(|indices| indices.iter().copied().collect())
            .collect();
        let pivot_vals_indices_hashset: Vec<std::collections::HashSet<_>> = pivot_vals_indices
            .iter()
            .map(|indices| indices.iter().copied().collect())
            .collect();
        for g_indices_hashset in group_vals_indices_hashset.iter() {
            for (name, pivot_key_idx) in pivot_name_to_pivot_key_idx.iter() {
                let idx = match pivot_key_idx {
                    Some(idx) => {
                        let p_indices_hashset = &pivot_vals_indices_hashset[*idx];
                        let matches = g_indices_hashset
                            .intersection(p_indices_hashset)
                            .copied()
                            .collect::<Vec<_>>();
                        match matches.len() {
                            0 => None,
                            1 => Some(matches[0]),
                            _ => {
                                return Err(DaftError::ComputeError(
                                    "Pivot column has more than one unique value".to_string(),
                                ));
                            }
                        }
                    }
                    None => None,
                };
                pivot_name_to_value_indices.get_mut(name).unwrap().push(idx);
            }
        }
        let pivoted_cols = names
            .iter()
            .map(|name| {
                let indices = pivot_name_to_value_indices.get(name).unwrap();
                let indices_as_arrow = arrow2::array::UInt64Array::from_iter(indices.iter());
                let indices_as_series =
                    UInt64Array::from(("", Box::new(indices_as_arrow))).into_series();
                let values = value_series.take(&indices_as_series)?;
                Ok(values.rename(name))
            })
            .collect::<DaftResult<Vec<_>>>()?;

        Self::from_columns([&[group_keys_series], &pivoted_cols[..]].concat())
    }
}
