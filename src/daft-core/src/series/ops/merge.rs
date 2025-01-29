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
    pub fn merge(field: Field, series: &[&Self]) -> DaftResult<Self> {
        // err if no series to merge
        if series.is_empty() {
            return Err(DaftError::ValueError(
                "Need at least 1 series to perform merge".to_string(),
            ));
        }

        // homogeneity checks happen in lower-levels, assume ok.
        let dtype = if let DataType::List(dtype) = &field.dtype {
            dtype.as_ref()
        } else {
            return Err(DaftError::ValueError(
                "Cannot merge field with non-list type".to_string(),
            ));
        };

        // build a null series mask so we can skip making full_nulls and avoid downcast "Null to T" errors.
        let mut mask: Vec<Option<usize>> = vec![];
        let mut rows = 0;
        let mut capacity = 0;
        let mut arrays = vec![];
        for s in series {
            if is_null(s) {
                mask.push(None);
            } else {
                mask.push(Some(arrays.len()));
                arrays.push(*s);
            }
            let len = s.len();
            rows = std::cmp::max(rows, len);
            capacity += len;
        }

        // initialize a growable child
        let mut offsets = Offsets::<i64>::with_capacity(capacity);
        let mut child = make_growable("list", dtype, arrays, true, capacity);
        let sublist_len = series.len() as i64;

        // merge each series based upon the mask
        for row in 0..rows {
            for i in &mask {
                if let Some(i) = *i {
                    child.extend(i, row, 1);
                } else {
                    child.extend_nulls(1);
                }
            }
            offsets.try_push(sublist_len)?;
        }

        // create the outer array with offsets
        Ok(ListArray::new(field, child.build()?, offsets.into(), None).into_series())
    }
}

/// Same null check logic as in Series::concat, but may need an audit since there are other is_null impls.
fn is_null(series: &&Series) -> bool {
    series.data_type() == &DataType::Null
}
