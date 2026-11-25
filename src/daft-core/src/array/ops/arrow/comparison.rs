use arrow::{
    array::{Array, ArrayRef, ArrowPrimitiveType, PrimitiveArray, make_comparator},
    buffer::{NullBuffer, ScalarBuffer},
    compute::SortOptions,
    datatypes::{DataType, Float32Type, Float64Type},
};
use common_error::DaftResult;
use num_traits::Float;

use crate::{kernels::cmp::cmp_float, series::Series};

fn build_is_equal_float<T>(
    left: &dyn Array,
    right: &dyn Array,
    nan_equal: bool,
) -> Box<dyn Fn(usize, usize) -> bool + Send + Sync>
where
    T: ArrowPrimitiveType,
    T::Native: Float,
{
    let left = left
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .unwrap()
        .clone();
    let right = right
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .unwrap()
        .clone();
    if nan_equal {
        Box::new(move |i, j| cmp_float::<T::Native>(&left.value(i), &right.value(j)).is_eq())
    } else {
        Box::new(move |i, j| left.value(i).eq(&right.value(j)))
    }
}

fn build_is_equal_with_nan(
    left: &dyn Array,
    right: &dyn Array,
    nan_equal: bool,
) -> DaftResult<Box<dyn Fn(usize, usize) -> bool + Send + Sync>> {
    match (left.data_type(), right.data_type()) {
        (DataType::Float32, DataType::Float32) => {
            Ok(build_is_equal_float::<Float32Type>(left, right, nan_equal))
        }
        (DataType::Float64, DataType::Float64) => {
            Ok(build_is_equal_float::<Float64Type>(left, right, nan_equal))
        }
        // Null arrays have no values to compare; the null handling in build_is_equal
        // covers all cases. Return a trivially-true function that will never be called.
        (DataType::Null, DataType::Null) => Ok(Box::new(|_, _| true)),
        _ => {
            let comp = make_comparator(left, right, SortOptions::new(false, false))?;
            Ok(Box::new(move |i, j| comp(i, j).is_eq()))
        }
    }
}

pub fn build_is_equal(
    left: &dyn Array,
    right: &dyn Array,
    nulls_equal: bool,
    nan_equal: bool,
) -> DaftResult<Box<dyn Fn(usize, usize) -> bool + Send + Sync>> {
    // NullArray has no validity bitmap, so is_valid() returns true for all indices.
    // Handle it explicitly: all values in a Null column are null.
    if *left.data_type() == DataType::Null && *right.data_type() == DataType::Null {
        return Ok(Box::new(move |_, _| nulls_equal));
    }

    let is_equal_fn = build_is_equal_with_nan(left, right, nan_equal)?;

    let left_data = left.to_data();
    let right_data = right.to_data();
    Ok(Box::new(move |i: usize, j: usize| {
        match (left_data.is_valid(i), right_data.is_valid(j)) {
            (true, true) => is_equal_fn(i, j),
            (false, false) => nulls_equal,
            _ => false,
        }
    }))
}

pub fn build_multi_array_is_equal(
    left: &[Series],
    right: &[Series],
    nulls_equal: &[bool],
    nans_equal: &[bool],
) -> DaftResult<Box<dyn Fn(usize, usize) -> bool + Send + Sync>> {
    let left_arrays: Vec<ArrayRef> = left
        .iter()
        .map(|s| s.to_arrow())
        .collect::<DaftResult<_>>()?;
    let right_arrays: Vec<ArrayRef> = right
        .iter()
        .map(|s| s.to_arrow())
        .collect::<DaftResult<_>>()?;
    build_multi_array_is_equal_from_arrays(&left_arrays, &right_arrays, nulls_equal, nans_equal)
}

// The multi-column equality path below specializes the generic per-column
// `build_is_equal` closures above. Building one boxed closure per column and
// calling it through a vtable on every row showed up in profiles for wide
// join keys, so the columns are lowered once into an enum-dispatched comparator
// with the value semantics preserved exactly (see `float_is_equal`, which
// mirrors `build_is_equal_float`).

/// Per-column value comparator.
///
/// Dispatching through an enum instead of a `Box<dyn Fn>` per column avoids a
/// vtable indirection on every comparison. The float variants additionally hold
/// zero-copy `ScalarBuffer` handles (an `Arc` clone of the backing buffer)
/// rather than reading values back through `dyn Array`.
enum ValueComparator {
    /// Null array: values are never compared, validity alone decides equality.
    Null,
    Float32 {
        left: ScalarBuffer<f32>,
        right: ScalarBuffer<f32>,
        nan_equal: bool,
    },
    Float64 {
        left: ScalarBuffer<f64>,
        right: ScalarBuffer<f64>,
        nan_equal: bool,
    },
    /// Fallback comparator for all other types.
    Generic(Box<dyn Fn(usize, usize) -> bool + Send + Sync>),
}

impl ValueComparator {
    #[inline(always)]
    fn is_equal(&self, i: usize, j: usize) -> bool {
        match self {
            Self::Null => true,
            Self::Float32 {
                left,
                right,
                nan_equal,
            } => float_is_equal(left[i], right[j], *nan_equal),
            Self::Float64 {
                left,
                right,
                nan_equal,
            } => float_is_equal(left[i], right[j], *nan_equal),
            Self::Generic(cmp) => cmp(i, j),
        }
    }
}

/// Float equality matching [`build_is_equal_float`]: NaNs compare equal only
/// when `nan_equal` is set, otherwise the standard IEEE comparison is used.
#[inline(always)]
fn float_is_equal<F: Float>(l: F, r: F, nan_equal: bool) -> bool {
    if nan_equal {
        cmp_float(&l, &r).is_eq()
    } else {
        l.eq(&r)
    }
}

/// Per-column validity, with the left and right null buffers stored separately.
struct ColumnValidity {
    left_nulls: Option<NullBuffer>,
    right_nulls: Option<NullBuffer>,
    nulls_equal: bool,
    // NullArray carries no validity bitmap but every value is null, so it is
    // tracked explicitly instead of relying on `nulls()`.
    left_is_null_array: bool,
    right_is_null_array: bool,
}

impl ColumnValidity {
    #[inline(always)]
    fn left_is_valid(&self, i: usize) -> bool {
        !self.left_is_null_array && self.left_nulls.as_ref().is_none_or(|n| n.is_valid(i))
    }

    #[inline(always)]
    fn right_is_valid(&self, j: usize) -> bool {
        !self.right_is_null_array && self.right_nulls.as_ref().is_none_or(|n| n.is_valid(j))
    }
}

/// Multi-column equality comparator, built once and evaluated per row pair.
struct MultiColumnComparator {
    values: Vec<ValueComparator>,
    validity: Vec<ColumnValidity>,
}

impl MultiColumnComparator {
    fn new(
        left: &[ArrayRef],
        right: &[ArrayRef],
        nulls_equal: &[bool],
        nan_equal: &[bool],
    ) -> DaftResult<Self> {
        let n = left.len();
        let mut values = Vec::with_capacity(n);
        let mut validity = Vec::with_capacity(n);

        for idx in 0..n {
            let l = &left[idx];
            let r = &right[idx];

            validity.push(ColumnValidity {
                left_nulls: l.nulls().cloned(),
                right_nulls: r.nulls().cloned(),
                nulls_equal: nulls_equal[idx],
                left_is_null_array: *l.data_type() == DataType::Null,
                right_is_null_array: *r.data_type() == DataType::Null,
            });

            let value = match (l.data_type(), r.data_type()) {
                (DataType::Null, DataType::Null) => ValueComparator::Null,
                (DataType::Float32, DataType::Float32) => ValueComparator::Float32 {
                    left: downcast::<Float32Type>(l).values().clone(),
                    right: downcast::<Float32Type>(r).values().clone(),
                    nan_equal: nan_equal[idx],
                },
                (DataType::Float64, DataType::Float64) => ValueComparator::Float64 {
                    left: downcast::<Float64Type>(l).values().clone(),
                    right: downcast::<Float64Type>(r).values().clone(),
                    nan_equal: nan_equal[idx],
                },
                _ => {
                    let comp = make_comparator(l, r, SortOptions::new(false, false))?;
                    ValueComparator::Generic(Box::new(move |i, j| comp(i, j).is_eq()))
                }
            };
            values.push(value);
        }

        Ok(Self { values, validity })
    }

    /// Compare row `a_idx` (left) with row `b_idx` (right), exiting on the first
    /// column that differs.
    #[inline(always)]
    fn is_equal(&self, a_idx: usize, b_idx: usize) -> bool {
        for (value, validity) in self.values.iter().zip(self.validity.iter()) {
            let left_valid = validity.left_is_valid(a_idx);
            let right_valid = validity.right_is_valid(b_idx);

            if left_valid && right_valid {
                if !value.is_equal(a_idx, b_idx) {
                    return false;
                }
            } else if left_valid != right_valid {
                // Exactly one side is null: never equal.
                return false;
            } else if !validity.nulls_equal {
                // Both sides null: equal only when nulls are treated as equal.
                return false;
            }
        }
        true
    }
}

#[inline]
fn downcast<T: ArrowPrimitiveType>(array: &ArrayRef) -> &PrimitiveArray<T> {
    array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap()
}

pub fn build_multi_array_is_equal_from_arrays(
    left: &[ArrayRef],
    right: &[ArrayRef],
    nulls_equal: &[bool],
    nans_equal: &[bool],
) -> DaftResult<Box<dyn Fn(usize, usize) -> bool + Send + Sync>> {
    let comparator = MultiColumnComparator::new(left, right, nulls_equal, nans_equal)?;

    Ok(Box::new(move |a_idx: usize, b_idx: usize| -> bool {
        comparator.is_equal(a_idx, b_idx)
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float32Array, Int32Array, NullArray};

    use super::*;

    fn single_column_is_equal(
        left: ArrayRef,
        right: ArrayRef,
        nulls_equal: bool,
        nan_equal: bool,
    ) -> Box<dyn Fn(usize, usize) -> bool + Send + Sync> {
        build_multi_array_is_equal_from_arrays(&[left], &[right], &[nulls_equal], &[nan_equal])
            .unwrap()
    }

    #[test]
    fn integer_equality() {
        let left = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let right = Arc::new(Int32Array::from(vec![1, 9, 3])) as ArrayRef;
        let is_equal = single_column_is_equal(left, right, false, false);
        assert!(is_equal(0, 0));
        assert!(!is_equal(1, 1));
        assert!(is_equal(2, 2));
    }

    #[test]
    fn float_nan_respects_nan_equal_flag() {
        let left = Arc::new(Float32Array::from(vec![f32::NAN])) as ArrayRef;
        let right = Arc::new(Float32Array::from(vec![f32::NAN])) as ArrayRef;

        let nan_eq = single_column_is_equal(left.clone(), right.clone(), false, true);
        assert!(nan_eq(0, 0), "NaN == NaN when nan_equal is set");

        let nan_ne = single_column_is_equal(left, right, false, false);
        assert!(!nan_ne(0, 0), "NaN != NaN by default");
    }

    #[test]
    fn float_signed_zero_is_equal() {
        let left = Arc::new(Float32Array::from(vec![0.0f32])) as ArrayRef;
        let right = Arc::new(Float32Array::from(vec![-0.0f32])) as ArrayRef;
        let is_equal = single_column_is_equal(left, right, false, false);
        assert!(is_equal(0, 0), "-0.0 == 0.0");
    }

    #[test]
    fn nulls_respect_nulls_equal_flag() {
        let left = Arc::new(Int32Array::from(vec![None, Some(1)])) as ArrayRef;
        let right = Arc::new(Int32Array::from(vec![None, Some(1)])) as ArrayRef;

        let null_eq = single_column_is_equal(left.clone(), right.clone(), true, false);
        assert!(null_eq(0, 0), "null == null when nulls_equal is set");

        let null_ne = single_column_is_equal(left, right, false, false);
        assert!(!null_ne(0, 0), "null != null by default");
    }

    #[test]
    fn one_side_null_is_never_equal() {
        let left = Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef;
        let right = Arc::new(Int32Array::from(vec![None])) as ArrayRef;
        let is_equal = single_column_is_equal(left, right, true, false);
        assert!(!is_equal(0, 0));
    }

    #[test]
    fn null_array_against_itself_respects_nulls_equal_flag() {
        // A NullArray has no validity bitmap, so equality is decided entirely by
        // the `left_is_null_array` / `right_is_null_array` flags treating every
        // row as null.
        let left = Arc::new(NullArray::new(2)) as ArrayRef;
        let right = Arc::new(NullArray::new(2)) as ArrayRef;

        let null_eq = single_column_is_equal(left.clone(), right.clone(), true, false);
        assert!(
            null_eq(0, 0),
            "all-null rows are equal when nulls_equal is set"
        );

        let null_ne = single_column_is_equal(left, right, false, false);
        assert!(!null_ne(0, 0), "all-null rows are not equal by default");
    }

    #[test]
    fn null_column_alongside_typed_column_uses_null_validity() {
        // A multi-column key with one all-null column and one typed column: the
        // Null column forces every row pair to disagree on that column unless
        // both rows are null *and* nulls_equal is set. This exercises the
        // `*_is_null_array` flags in the real (matching-type) multi-column path,
        // without the unsupported cross-type pairing on a single column.
        let left_null = Arc::new(NullArray::new(2)) as ArrayRef;
        let left_int = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
        let right_null = Arc::new(NullArray::new(2)) as ArrayRef;
        let right_int = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;

        // nulls_equal = false on the Null column: the null column never matches,
        // so the whole key never matches even though the int column is equal.
        let ne = build_multi_array_is_equal_from_arrays(
            &[left_null.clone(), left_int.clone()],
            &[right_null.clone(), right_int.clone()],
            &[false, false],
            &[false, false],
        )
        .unwrap();
        assert!(
            !ne(0, 0),
            "all-null column blocks the match when nulls_equal is false"
        );

        // nulls_equal = true on the Null column: the null column matches, so the
        // key matches iff the int column matches.
        let eq = build_multi_array_is_equal_from_arrays(
            &[left_null, left_int],
            &[right_null, right_int],
            &[true, false],
            &[false, false],
        )
        .unwrap();
        assert!(
            eq(0, 0),
            "matching int rows are equal when the null column allows it"
        );
        assert!(
            !eq(0, 1),
            "differing int rows are unequal even when the null column allows it"
        );
    }

    #[test]
    fn multi_column_exits_on_first_mismatch() {
        let left_a = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
        let left_b = Arc::new(Int32Array::from(vec![2])) as ArrayRef;
        let right_a = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
        let right_b = Arc::new(Int32Array::from(vec![9])) as ArrayRef;

        let is_equal = build_multi_array_is_equal_from_arrays(
            &[left_a, left_b],
            &[right_a, right_b],
            &[false, false],
            &[false, false],
        )
        .unwrap();
        assert!(!is_equal(0, 0), "second column differs");
    }
}
