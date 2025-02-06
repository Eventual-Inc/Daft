use std::cmp::{max, min};

use arrow2::offset::Offsets;
use common_error::{DaftError, DaftResult};
use daft_schema::{dtype::DataType, field::Field};

use crate::{
    array::{growable::make_growable, ListArray},
    series::{IntoSeries, Series},
};

impl Series {
    /// Zips series into a single series of lists.
    /// ex:
    /// ```text
    /// A: Series := ( a_0, a_1, .. , a_n )
    /// B: Series := ( b_0, b_1, .. , b_n )
    /// C: Series := Zip(A, B) <-> ( [a_0, b_0], [a_1, b_1], [a_2, b_2] )
    /// ```
    pub fn zip(field: Field, series: &[&Self]) -> DaftResult<Self> {
        // err if no series to zip
        if series.is_empty() {
            return Err(DaftError::ValueError(
                "Need at least 1 series to perform zip".to_string(),
            ));
        }

        // homogeneity checks naturally happen in make_growable's downcast.
        let dtype = match &field.dtype {
            DataType::List(dtype) => dtype.as_ref(),
            DataType::FixedSizeList(..) => {
                return Err(DaftError::ValueError(
                    "Fixed size list constructor is currently not supported".to_string(),
                ));
            }
            _ => {
                return Err(DaftError::ValueError(
                    "Cannot zip field with non-list type".to_string(),
                ));
            }
        };

        // 0 -> index of child in 'arrays' vector
        // 1 -> last index of child
        type Child = (usize, usize);

        // build a null series mask so we can skip making full_nulls and avoid downcast "Null to T" errors.
        let mut mask: Vec<Option<Child>> = vec![];
        let mut rows = 0;
        let mut capacity = 0;
        let mut arrays = vec![];

        for s in series {
            let len = s.len();
            if is_null(s) {
                mask.push(None);
            } else {
                mask.push(Some((arrays.len(), len - 1)));
                arrays.push(*s);
            }
            rows = max(rows, len);
            capacity += len;
        }

        // initialize a growable child
        let mut offsets = Offsets::<i64>::with_capacity(capacity);
        let mut child = make_growable("list", dtype, arrays, true, capacity);
        let sublist_len = series.len() as i64;

        // merge each series based upon the mask
        for row in 0..rows {
            for i in &mask {
                if let Some((i, end)) = *i {
                    child.extend(i, min(row, end), 1);
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
