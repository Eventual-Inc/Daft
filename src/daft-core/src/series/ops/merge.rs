use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_schema::{dtype::DataType, field::Field};

use crate::series::Series;

impl Series {
    /// Merges series into a single series of lists.
    /// ex:
    /// ```text
    /// A: Series := ( a_0, a_1, .. , a_n )
    /// B: Series := ( b_0, b_1, .. , b_n )
    /// C: Series := MERGE(A, B) <-> ( [a_0, b_0], [a_1, b_1], [a_2, b_2] )
    /// ```
    pub fn merge(series: &[&Self]) -> DaftResult<Self> {
        if series.is_empty() {
            return Err(DaftError::ValueError(
                "Need at least 1 series to perform merge".to_string(),
            ));
        }
        // TODO implement merge
        let data = vec![Some(1i64), Some(2i64)];
        let data = Box::new(arrow2::array::Int64Array::from(data));
        let field = Arc::new(Field::new("test", DataType::Int64));
        let s = Self::from_arrow(field, data)?.agg_list(None)?;
        Ok(s)
    }
}
