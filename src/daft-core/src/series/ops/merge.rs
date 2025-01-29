use arrow2::offset::Offsets;
use common_error::{DaftError, DaftResult};
use daft_schema::{dtype::DataType, field::Field};

use crate::{
    array::{growable::make_growable, ListArray},
    series::{IntoSeries, Series},
};

impl Series {
    /// Merges series into a single series of lists.
    /// ex:
    /// ```text
    /// A: Series := ( a_0, a_1, .. , a_n )
    /// B: Series := ( b_0, b_1, .. , b_n )
    /// C: Series := MERGE(A, B) <-> ( [a_0, b_0], [a_1, b_1], [a_2, b_2] )
    /// ```
    pub fn merge(series: &[&Self]) -> DaftResult<Self> {
        // err if no series to merge
        if series.is_empty() {
            return Err(DaftError::ValueError(
                "Need at least 1 series to perform merge".to_string(),
            ));
        }

        // compute some basic info that's re-used
        let len = series[0].len();
        let dtype = series[0].data_type();
        let capacity = series.iter().map(|s| s.len()).sum();
        let mut offsets = Offsets::<i64>::with_capacity(capacity);

        // grow the child by merging each series
        let mut child = make_growable("list", dtype, series.to_vec(), true, capacity);
        for row in 0..len {
            let mut n = 0;
            for (i, col) in series.iter().enumerate() {
                if is_null(col) {
                    child.extend_nulls(1);
                } else {
                    n += 1;
                    child.extend(i, row, 1);
                }
            }
            offsets.try_push(n)?;
        }

        // create list_array
        let child = child.build()?;
        let field = Field::new("list", DataType::new_list(dtype.clone()));
        let items = ListArray::new(field, child, offsets.into(), None);
        Ok(items.into_series())
    }
}

/// Same null check logic as in Series::concat, but may need an audit since there are other is_null impls.
fn is_null(series: &&Series) -> bool {
    series.data_type() == &DataType::Null
}
