use crate::{
    datatypes::UInt64Array,
    error::{DaftError, DaftResult},
    table::Table,
};

impl Table {
    pub fn search_sorted(&self, keys: &Self, descending: &[bool]) -> DaftResult<UInt64Array> {
        if self.schema != keys.schema {
            return Err(DaftError::SchemaMismatch(format!(
                "Schema Mismatch in search_sorted: data: {} vs keys: {}",
                self.schema, keys.schema
            )));
        }
        if self.num_columns() != descending.len() {
            return Err(DaftError::ValueError(format!("Mismatch in number of arguments for `descending` in search sorted: num_columns: {} vs : descending.len() {}", self.num_columns(), descending.len())));
        }

        if self.num_columns() == 1 {
            return self
                .get_column_by_index(0)?
                .search_sorted(keys.get_column_by_index(0)?, *descending.first().unwrap());
        }
        todo!("impl multicol search_sorted")
    }
}
