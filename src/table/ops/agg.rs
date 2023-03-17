use std::cmp::Ordering;

use crate::{
    array::{ops::build_multi_array_compare, BaseArray},
    datatypes::{UInt64Array, UInt64Type},
    dsl::{AggExpr, Expr},
    error::DaftResult,
    series::Series,
    table::Table,
};

impl Table {
    pub fn agg(&self, to_agg: &[(Expr, &str)], group_by: &[Expr]) -> DaftResult<Table> {
        match group_by.len() {
            0 => self.agg_global(to_agg),
            _ => self.agg_groupby(to_agg, group_by),
        }
    }

    pub fn agg_global(&self, to_agg: &[(Expr, &str)]) -> DaftResult<Table> {
        // Convert the input (child, name) exprs to the enum form.
        //  e.g. (expr, "sum") to Expr::Agg(AggExpr::Sum(expr))
        // (We could do this at the pyo3 layer but I'm not sure why they're strings in the first place)
        let agg_expr_list = to_agg
            .iter()
            .map(|(e, s)| AggExpr::from_name_and_child_expr(s, e))
            .collect::<DaftResult<Vec<AggExpr>>>()?;
        let expr_list = agg_expr_list
            .iter()
            .map(|ae| Expr::Agg(ae.clone()))
            .collect::<Vec<Expr>>();
        self.eval_expression_list(&expr_list)
    }

    pub fn agg_groupby(&self, to_agg: &[(Expr, &str)], group_by: &[Expr]) -> DaftResult<Table> {
        // Table with just the groupby columns.
        let groupby_table = self.eval_expression_list(group_by)?;

        let (groupkey_indices, groupvals_indices) = groupby_table.sort_grouper()?;

        // Table with the aggregated (deduplicated) group keys.
        let groupkeys_table = {
            let indices_as_arrow = arrow2::array::PrimitiveArray::from_vec(groupkey_indices);
            let indices_as_series =
                UInt64Array::from(("__TEMP_DAFT_GROUP_INDICES", Box::new(indices_as_arrow)))
                    .into_series();
            groupby_table.take(&indices_as_series)?
        };

        println!("{}", groupkeys_table);

        // Table with the aggregated values, one row for each group.
        let agged_values_table = {
            let mut subresults: Vec<Self> = vec![];

            for group_indices_array in groupvals_indices.iter() {
                let subtable = {
                    let indices_as_arrow = group_indices_array.downcast();
                    let indices_as_series = UInt64Array::from((
                        "__TEMP_DAFT_GROUP_INDICES",
                        Box::new(indices_as_arrow.clone()),
                    ))
                    .into_series();
                    self.take(&indices_as_series)?
                };
                println!("{}", subtable);
                let subresult = subtable.agg_global(to_agg)?;
                println!("{}", subresult);
                subresults.push(subresult.to_owned());
            }

            match subresults.len() {
                0 => self.head(0)?.agg_global(to_agg)?.head(0)?,
                _ => Self::concat(subresults.iter().collect::<Vec<&Self>>().as_slice())?,
            }
        };

        println!("{}", agged_values_table);

        // Final result - concat the groupkey columns and the agg result columns together.
        Self::from_columns(
            [
                &groupkeys_table.columns[..],
                &agged_values_table.columns[..],
            ]
            .concat(),
        )
    }

    fn sort_grouper(&self) -> DaftResult<(Vec<u64>, Vec<UInt64Array>)> {
        let argsort_series =
            Series::argsort_multikey(self.columns.as_slice(), &vec![false; self.columns.len()])?;

        let argsort_array = argsort_series.downcast::<UInt64Type>()?;

        let mut groupvals_indices: Vec<UInt64Array> = vec![];
        let mut groupkey_indices: Vec<u64> = vec![];

        let comparator =
            build_multi_array_compare(self.columns.as_slice(), &vec![false; self.columns.len()])?;

        // (argsort index, data index).
        let mut group_begin_indices: Option<(usize, usize)> = None;

        for (argsort_index, data_index) in argsort_array.downcast().iter().enumerate() {
            let data_index = *data_index.unwrap() as usize;

            // Start a new group result if the groupkey has changed (or if there was no previous groupkey).
            match group_begin_indices {
                None => group_begin_indices = Some((argsort_index, data_index)),
                Some((begin_argsort_index, begin_data_index)) => {
                    let comp_result = comparator(begin_data_index, data_index);
                    if comp_result != Ordering::Equal {
                        groupkey_indices.push(begin_data_index as u64);
                        groupvals_indices
                            .push(argsort_array.slice(begin_argsort_index, argsort_index)?);
                        group_begin_indices = Some((argsort_index, data_index));
                    }
                }
            }
        }

        if let Some((begin_argsort_index, begin_data_index)) = group_begin_indices {
            groupkey_indices.push(begin_data_index as u64);
            groupvals_indices.push(argsort_array.slice(begin_argsort_index, argsort_array.len())?);
        }

        Ok((groupkey_indices, groupvals_indices))
    }
}
