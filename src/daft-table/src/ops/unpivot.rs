use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::{UInt64Array, Utf8Array},
    schema::Schema,
    series::cast_series_to_supertype,
    IntoSeries, Series,
};
use daft_dsl::ExprRef;

use crate::Table;

impl Table {
    pub fn unpivot(
        &self,
        ids: &[ExprRef],
        values: &[ExprRef],
        variable_name: &str,
        value_name: &str,
    ) -> DaftResult<Self> {
        if values.is_empty() {
            return Err(DaftError::ValueError(
                "Unpivot requires at least one value column".to_string(),
            ));
        }

        let unpivoted_len = self.len() * values.len();

        let ids_table = self.eval_expression_list(ids)?;
        let values_table = self.eval_expression_list(values)?;

        let ids_idx = UInt64Array::from((
            "ids_indices",
            (0..(self.len() as u64))
                .cycle()
                .take(unpivoted_len)
                .collect::<Vec<_>>(),
        ))
        .into_series();

        let ids_series = ids_table.take(&ids_idx)?.columns;
        let ids_schema = ids_table.schema;

        let values_names = values_table.column_names();
        let variable_column = values_names
            .iter()
            .flat_map(|n| std::iter::repeat(n).take(self.len()));
        let variable_arr = Box::new(arrow2::array::Utf8Array::from_iter_values(variable_column));
        let variable_series = Utf8Array::from((variable_name, variable_arr)).into_series();

        let values_cols: Vec<&Series> = values_table.columns.iter().collect();
        let values_casted = cast_series_to_supertype(&values_cols)?;

        let value_series =
            Series::concat(&values_casted.iter().collect::<Vec<_>>())?.rename(value_name);

        let unpivot_schema = ids_schema.union(&Schema::new(vec![
            variable_series.field().clone(),
            value_series.field().clone(),
        ])?)?;
        let unpivot_series = [ids_series, vec![variable_series, value_series]].concat();

        Table::new_with_size(unpivot_schema, unpivot_series, unpivoted_len)
    }
}
