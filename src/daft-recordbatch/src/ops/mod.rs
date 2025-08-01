mod agg;
mod explode;
mod groups;
pub mod hash;
mod joins;
mod partition;
mod pivot;
mod search_sorted;
mod sort;
mod unpivot;
mod window;
mod window_states;

use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::Schema;
use daft_dsl::expr::BoundColumn;
pub use joins::{get_column_by_name, get_columns_by_name};

use crate::RecordBatch;

impl RecordBatch {
    pub fn select_columns(&self, columns: &[BoundColumn]) -> DaftResult<Self> {
        let new_schema = Arc::new(Schema::new(
            columns.iter().map(|col| self.schema[col.index].clone()),
        ));
        let columns = columns
            .iter()
            .map(|col| self.columns[col.index].clone())
            .collect::<Vec<_>>();
        Self::new_with_size(new_schema, columns, self.num_rows)
    }
}
