use crate::Table;
use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ops::{as_arrow::AsArrow, IntoGroups},
    datatypes::UInt64Array,
    IntoSeries, Series,
};
use daft_dsl::ExprRef;

fn map_name_to_pivot_key_idx<'a>(
    pivot_series: &'a Series,
    pivot_key_indices: &'a [u64],
    names: &'a [String],
) -> DaftResult<std::collections::HashMap<&'a String, u64>> {
    let pivot_keys_series = {
        let indices_as_series = UInt64Array::from(("", pivot_key_indices.to_owned())).into_series();
        pivot_series.take(&indices_as_series)?
    };
    let pivot_keys_str_values = pivot_keys_series.to_str_values()?;
    let pivot_key_str_to_idx_mapping = pivot_keys_str_values
        .utf8()?
        .as_arrow()
        .iter()
        .zip(pivot_key_indices.iter())
        .map(|(key_str, idx)| (key_str.unwrap(), *idx))
        .collect::<std::collections::HashMap<_, _>>();

    let mut name_to_pivot_key_idx_mapping = std::collections::HashMap::new();
    for name in names.iter() {
        if let Some(pivot_key_idx) = pivot_key_str_to_idx_mapping.get(name.as_str()) {
            name_to_pivot_key_idx_mapping.insert(name, *pivot_key_idx);
        }
    }

    Ok(name_to_pivot_key_idx_mapping)
}

fn map_pivot_key_idx_to_values_indices(
    group_vals_indices: &[Vec<u64>],
    pivot_vals_indices: &[Vec<u64>],
    pivot_keys_indices: &[u64],
) -> DaftResult<std::collections::HashMap<u64, Vec<Option<u64>>>> {
    let group_vals_indices_hashsets = group_vals_indices
        .iter()
        .map(|indices| indices.iter().collect())
        .collect::<Vec<std::collections::HashSet<_>>>();

    let mut pivot_key_idx_to_values_indices = std::collections::HashMap::new();
    for (p_key, p_indices) in pivot_keys_indices.iter().zip(pivot_vals_indices.iter()) {
        let p_indices_hashset = p_indices.iter().collect::<std::collections::HashSet<_>>();
        let mut values_indices = Vec::new();
        for g_indices_hashset in group_vals_indices_hashsets.iter() {
            let matches = g_indices_hashset
                .intersection(&p_indices_hashset)
                .collect::<Vec<_>>();
            let idx = match matches.len() {
                0 => None,
                1 => Some(**matches[0]),
                _ => {
                    return Err(DaftError::ComputeError(
                        "Pivot column has more than one unique value".to_string(),
                    ));
                }
            };
            values_indices.push(idx);
        }
        pivot_key_idx_to_values_indices.insert(*p_key, values_indices);
    }
    Ok(pivot_key_idx_to_values_indices)
}

impl Table {
    pub fn pivot(
        &self,
        group_by: &[ExprRef],
        pivot_col: ExprRef,
        values_col: ExprRef,
        names: Vec<String>,
    ) -> DaftResult<Table> {
        // This function pivots the table based on the given group_by, pivot, and values column.
        //
        // At a high level this function does two things:
        //  1. Map each unique value in the pivot column to a list of the corresponding indices of values in the values column.
        //  2. Do a .take() operation on the values column for each list of indices to create the new pivoted columns.
        //
        // Notes:
        // - This function assumes that there are no duplicate values in the pivot column.
        // - If a name in the names vector does not exist in the pivot column, a new column with null values is created.

        let groupby_table = self.eval_expression_list(group_by)?;
        let pivot_series = self.eval_expression(&pivot_col)?;
        let value_series = self.eval_expression(&values_col)?;

        let (group_keys_indices, group_vals_indices) = groupby_table.make_groups()?;
        let (pivot_keys_indices, pivot_vals_indices) = pivot_series.make_groups()?;

        let name_to_pivot_key_idx =
            map_name_to_pivot_key_idx(&pivot_series, &pivot_keys_indices, &names)?;
        let pivot_key_idx_to_values_indices = map_pivot_key_idx_to_values_indices(
            &group_vals_indices,
            &pivot_vals_indices,
            &pivot_keys_indices,
        )?;

        let pivoted_cols = names
            .iter()
            .map(|name| match name_to_pivot_key_idx.get(name) {
                Some(pivot_key_idx) => {
                    let indices = pivot_key_idx_to_values_indices.get(pivot_key_idx).unwrap();
                    let indices_as_arrow = arrow2::array::UInt64Array::from_iter(indices.iter());
                    let indices_as_series =
                        UInt64Array::from(("", Box::new(indices_as_arrow))).into_series();
                    let values = value_series.take(&indices_as_series)?;
                    Ok(values.rename(name))
                }
                None => Ok(Series::full_null(
                    name,
                    value_series.data_type(),
                    group_keys_indices.len(),
                )),
            })
            .collect::<DaftResult<Vec<_>>>()?;

        let group_keys_table = {
            let indices_as_series = UInt64Array::from(("", group_keys_indices)).into_series();
            groupby_table.take(&indices_as_series)?
        };

        Self::from_nonempty_columns([&group_keys_table.columns[..], &pivoted_cols[..]].concat())
    }
}
