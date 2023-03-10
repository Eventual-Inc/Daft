use crate::{
    datatypes::UInt64Array,
    error::{DaftError, DaftResult},
    table::Table,
};

impl Table {
    pub fn hash_rows(&self) -> DaftResult<UInt64Array> {
        if self.num_columns() == 0 {
            return Err(DaftError::ValueError(
                "Attempting to Hash Table with no columns".to_string(),
            ));
        }
        let mut hash_so_far = self.columns.first().unwrap().hash(None)?;
        for c in self.columns.iter().skip(1) {
            hash_so_far = c.hash(Some(&hash_so_far))?;
        }
        Ok(hash_so_far)
    }
}
