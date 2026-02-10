use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::{DataType, Field, UInt64Array},
    kernels::search_sorted::search_sorted_multi_array,
    series::Series,
};

use crate::RecordBatch;

impl RecordBatch {
    pub fn search_sorted(&self, keys: &Self, descending: &[bool]) -> DaftResult<UInt64Array> {
        if self.schema != keys.schema {
            return Err(DaftError::SchemaMismatch(format!(
                "Schema Mismatch in search_sorted: data: {} vs keys: {}",
                self.schema, keys.schema
            )));
        }
        if self.num_columns() != descending.len() {
            return Err(DaftError::ValueError(format!(
                "Mismatch in number of arguments for `descending` in search sorted: num_columns: {} vs : descending.len() {}",
                self.num_columns(),
                descending.len()
            )));
        }

        if self.num_columns() == 1 {
            return self
                .get_column(0)
                .search_sorted(keys.get_column(0), *descending.first().unwrap());
        }
        unsafe {
            multicol_search_sorted(self.columns.as_slice(), keys.columns.as_slice(), descending)
        }
    }
}

unsafe fn multicol_search_sorted(
    data: &[Series],
    keys: &[Series],
    descending: &[bool],
) -> DaftResult<UInt64Array> {
    let data_arrow_rs_vec: Vec<_> = data
        .iter()
        .map(|s| s.to_arrow())
        .collect::<DaftResult<Vec<_>>>()?;

    let keys_arrow_rs_vec: Vec<_> = keys
        .iter()
        .map(|s| s.to_arrow())
        .collect::<DaftResult<Vec<_>>>()?;

    let data_refs: Vec<_> = data_arrow_rs_vec.iter().map(|a| a.as_ref()).collect();
    let keys_refs: Vec<_> = keys_arrow_rs_vec.iter().map(|a| a.as_ref()).collect();

    let indices = search_sorted_multi_array(&data_refs, &keys_refs, &Vec::from(descending))?;
    UInt64Array::from_arrow(Field::new("indices", DataType::UInt64), Arc::new(indices))
}
