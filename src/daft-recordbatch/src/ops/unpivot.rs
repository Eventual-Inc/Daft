use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_core::{prelude::*, series::cast_series_to_supertype};
use daft_dsl::expr::bound_expr::BoundExpr;

use crate::RecordBatch;

impl RecordBatch {
    pub fn unpivot(
        &self,
        ids: &[BoundExpr],
        values: &[BoundExpr],
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

        let ids_idx = UInt64Array::from_vec(
            "ids_indices",
            (0..(self.len() as u64))
                .cycle()
                .take(unpivoted_len)
                .collect::<Vec<_>>(),
        );

        let ids_series: Vec<Series> = Arc::unwrap_or_clone(ids_table.take(&ids_idx)?.columns)
            .into_iter()
            .map(|c| c.take_materialized_series())
            .collect();
        let ids_schema = ids_table.schema;

        let variable_column = values_table
            .schema
            .field_names()
            .flat_map(|n| std::iter::repeat_n(n, self.len()))
            .collect::<Vec<_>>();
        let variable_series =
            Utf8Array::from_slice(variable_name, variable_column.as_ref()).into_series();

        let values_cols: Vec<&Series> = values_table
            .columns
            .iter()
            .map(|c| c.as_materialized_series())
            .collect();
        let values_casted = cast_series_to_supertype(&values_cols)?;

        let value_series =
            Series::concat(&values_casted.iter().collect::<Vec<_>>())?.rename(value_name);

        let unpivot_schema = Schema::new(ids_schema.into_iter().cloned().chain([
            variable_series.field().clone(),
            value_series.field().clone(),
        ]));

        let unpivot_series = [ids_series, vec![variable_series, value_series]].concat();

        Self::new_with_size(unpivot_schema, unpivot_series, unpivoted_len)
    }
}
