use crate::{
    array::{ops::arrow2::comparison::build_multi_array_is_equal, BaseArray},
    datatypes::{UInt64Array, UInt64Type},
    dsl::Expr,
    error::DaftResult,
    series::Series,
    table::Table,
};

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
        let (groupkey_indices, groupvals_indices) = groupby_table.sort_grouper()?;

        // Table with the aggregated (deduplicated) group keys.
        let groupkeys_table = {
            let indices_as_arrow = arrow2::array::PrimitiveArray::from_vec(groupkey_indices);
            let indices_as_series =
                UInt64Array::from(("", Box::new(indices_as_arrow))).into_series();
            groupby_table.take(&indices_as_series)?
        };

        // Table with the aggregated values, one row for each group.
        let agged_values_table = {
            // Agg each group into its own table.
            let mut agged_groups: Vec<Self> = vec![];
            for group_indices_array in groupvals_indices.iter() {
                let group = {
                    let indices_as_arrow = group_indices_array.downcast();
                    let indices_as_series =
                        UInt64Array::from(("", Box::new(indices_as_arrow.clone()))).into_series();
                    self.take(&indices_as_series)?
                };
                let agged_group = group.agg_global(to_agg)?;
                agged_groups.push(agged_group.to_owned());
            }

            match agged_groups.len() {
                0 => self.head(0)?.agg_global(to_agg)?.head(0)?,
                _ => Self::concat(agged_groups.iter().collect::<Vec<&Self>>().as_slice())?,
            }
        };

        // Combine the groupkey columns and the aggregation result columns.
        Self::from_columns(
            [
                &groupkeys_table.columns[..],
                &agged_values_table.columns[..],
            ]
            .concat(),
        )
    }

    fn sort_grouper(&self) -> DaftResult<(Vec<u64>, Vec<UInt64Array>)> {
        // Argsort the table, but also group identical values together.
        //
        // Given a table, returns a tuple:
        // 1. An argsort of the entire table, deduplicated.
        // 2. An argsort of the entire table, with identical values grouped.
        //
        // e.g. given a table [B, B, A, B, C, C]
        // returns: (
        //      [2, 0, 4]  <-- indices of A, B, and C
        //      [[2], [0, 1, 3], [4, 5]]  <--- indices of all A, all B, all C
        // )

        // Begin by doing the argsort.
        let argsort_series =
            Series::argsort_multikey(self.columns.as_slice(), &vec![false; self.columns.len()])?;
        let argsort_array = argsort_series.downcast::<UInt64Type>()?;

        // The result indices.
        let mut key_indices: Vec<u64> = vec![];
        let mut values_indices: Vec<UInt64Array> = vec![];

        let comparator = build_multi_array_is_equal(
            self.columns.as_slice(),
            self.columns.as_slice(),
            true,
            true,
        )?;

        // To group the argsort values together, we will traverse the table in argsort order,
        // collecting the indices traversed whenever the table value changes.

        // The current table value we're looking at, but represented only by the index in the table.
        // For convenience, also keep the index's index in the argarray.

        let mut group_begin_indices: Option<(usize, usize)> = None;

        for (argarray_index, table_index) in argsort_array.downcast().iter().enumerate() {
            let table_index = *table_index.unwrap() as usize;

            match group_begin_indices {
                None => group_begin_indices = Some((table_index, argarray_index)),
                Some((begin_table_index, begin_argarray_index)) => {
                    let is_equal = comparator(begin_table_index, table_index);
                    if !is_equal {
                        // The value has changed.
                        // Record results for the previous group.
                        key_indices.push(begin_table_index as u64);
                        values_indices
                            .push(argsort_array.slice(begin_argarray_index, argarray_index)?);

                        // Update the current value.
                        group_begin_indices = Some((table_index, argarray_index));
                    }
                }
            }
        }

        // Record results for the last group (since the for loop doesn't detect the last group closing).
        if let Some((begin_table_index, begin_argsort_index)) = group_begin_indices {
            key_indices.push(begin_table_index as u64);
            values_indices.push(argsort_array.slice(begin_argsort_index, argsort_array.len())?);
        }

        Ok((key_indices, values_indices))
    }
}
