use std::{marker::PhantomData, sync::Arc};

use arrow::array::{ArrayData, MutableArrayData, make_array};
use common_error::DaftResult;
use daft_arrow::array::to_data;

use super::Growable;
use crate::{
    array::prelude::*,
    datatypes::prelude::*,
    series::{IntoSeries, Series},
};

/// Operation recorded by `extend`/`add_nulls` and replayed in `build()`.
enum GrowOp {
    Extend {
        index: usize,
        start: usize,
        len: usize,
    },
    AddNulls(usize),
}

/// Single generic growable for all `DaftArrowBackedType` variants (bool, int, float, string,
/// binary, decimal, interval, extension, etc.).
///
/// Source arrays are converted from arrow2 to arrow-rs `ArrayData` once in `new()`.
/// Operations are deferred and replayed in `build()` because `MutableArrayData` borrows
/// `&ArrayData`, and storing both owned data and the borrower in one struct would be
/// self-referential.
// TODO(desmond): Once Daft stores arrow-rs arrays natively, remove GrowOp and write
// directly to a `MutableArrayData` field.
pub struct ArrowGrowable<'a, T: DaftArrowBackedType> {
    name: String,
    dtype: DataType,
    source_data: Vec<ArrayData>,
    ops: Vec<GrowOp>,
    use_validity: bool,
    capacity: usize,
    _phantom: PhantomData<&'a T>,
}

impl<'a, T: DaftArrowBackedType> ArrowGrowable<'a, T> {
    pub fn new(
        name: &str,
        dtype: &DataType,
        arrays: Vec<&'a DataArray<T>>,
        use_validity: bool,
        capacity: usize,
    ) -> Self {
        let source_data = arrays.iter().map(|s| to_data(s.data())).collect();
        Self {
            name: name.to_string(),
            dtype: dtype.clone(),
            source_data,
            ops: Vec::new(),
            use_validity,
            capacity,
            _phantom: PhantomData,
        }
    }
}

impl<T: DaftArrowBackedType> Growable for ArrowGrowable<'_, T>
where
    DataArray<T>: IntoSeries,
{
    #[inline]
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        self.ops.push(GrowOp::Extend { index, start, len });
    }

    #[inline]
    fn add_nulls(&mut self, additional: usize) {
        self.ops.push(GrowOp::AddNulls(additional));
    }

    fn build(&mut self) -> DaftResult<Series> {
        let refs: Vec<&ArrayData> = self.source_data.iter().collect();
        let mut mutable = MutableArrayData::new(refs, self.use_validity, self.capacity);

        // Replay recorded operations.
        // Note: MutableArrayData::extend takes (index, start, end) not (index, start, len).
        for op in self.ops.drain(..) {
            match op {
                GrowOp::Extend { index, start, len } => {
                    mutable.extend(index, start, start + len);
                }
                GrowOp::AddNulls(n) => {
                    mutable.extend_nulls(n);
                }
            }
        }

        let data = mutable.freeze();
        let arrow_array = make_array(data);
        let field = Arc::new(Field::new(self.name.clone(), self.dtype.clone()));
        Ok(DataArray::<T>::from_arrow(field, arrow_array)?.into_series())
    }
}

/// Simplified null growable — just tracks a length counter.
/// No sources or MutableArrayData needed since every element is null.
pub struct ArrowNullGrowable {
    name: String,
    dtype: DataType,
    len: usize,
}

impl ArrowNullGrowable {
    pub fn new(name: &str, dtype: &DataType) -> Self {
        Self {
            name: name.to_string(),
            dtype: dtype.clone(),
            len: 0,
        }
    }
}

impl Growable for ArrowNullGrowable {
    #[inline]
    fn extend(&mut self, _index: usize, _start: usize, len: usize) {
        self.len += len;
    }

    #[inline]
    fn add_nulls(&mut self, additional: usize) {
        self.len += additional;
    }

    #[inline]
    fn build(&mut self) -> DaftResult<Series> {
        let len = self.len;
        self.len = 0;
        Ok(NullArray::full_null(&self.name, &self.dtype, len).into_series())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Extends from a non-zero start to catch start+len vs start..len confusion
    /// in the MutableArrayData::extend call (which takes (index, start, end)).
    #[test]
    fn test_extend_from_nonzero_start() {
        let field = Field::new("test", DataType::Int32);
        let src = Int32Array::from_iter(
            field.clone(),
            vec![Some(10), Some(20), Some(30), Some(40), Some(50)],
        );
        let mut growable =
            ArrowGrowable::<Int32Type>::new("test", &DataType::Int32, vec![&src], false, 0);
        // Take elements at indices 2..4 → [30, 40]
        growable.extend(0, 2, 2);
        let result = growable.build().unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.i32().unwrap().get(0), Some(30));
        assert_eq!(result.i32().unwrap().get(1), Some(40));
    }
}
