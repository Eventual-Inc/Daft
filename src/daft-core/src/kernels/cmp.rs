use std::cmp::Ordering;

use arrow::{
    array::{Array, ArrowPrimitiveType, PrimitiveArray, make_comparator},
    compute::SortOptions,
    datatypes::*,
};
use common_error::DaftResult;
use num_traits::Float;

#[allow(clippy::eq_op)]
#[inline]
pub fn cmp_float<F: Float>(l: &F, r: &F) -> std::cmp::Ordering {
    match (l.is_nan(), r.is_nan()) {
        (false, false) => unsafe { l.partial_cmp(r).unwrap_unchecked() },
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
    }
}

fn build_nan_aware_comparator<T>(
    left: PrimitiveArray<T>,
    right: PrimitiveArray<T>,
    sort_options: SortOptions,
) -> Box<dyn Fn(usize, usize) -> Ordering + Send + Sync>
where
    T: ArrowPrimitiveType,
    T::Native: Float,
{
    let SortOptions {
        descending,
        nulls_first,
    } = sort_options;

    if left.null_count() == 0 && right.null_count() == 0 {
        let left_values = left.values().clone();
        let right_values = right.values().clone();
        Box::new(move |i: usize, j: usize| {
            let cmp = cmp_float(&left_values[i], &right_values[j]);
            if descending { cmp.reverse() } else { cmp }
        })
    } else {
        Box::new(
            move |i: usize, j: usize| match (left.is_valid(i), right.is_valid(j)) {
                (true, true) => {
                    let cmp = cmp_float(&left.value(i), &right.value(j));
                    if descending { cmp.reverse() } else { cmp }
                }
                (false, false) => Ordering::Equal,
                (false, _) => {
                    if nulls_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (_, false) => {
                    if nulls_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
            },
        )
    }
}

/// Build a comparator that respects Daft's NaN ordering contract.
///
/// For float types, builds a zero-allocation comparator using `cmp_float`.
/// For all other types, delegates to arrow-rs `make_comparator`.
pub(crate) fn make_daft_comparator(
    left: &dyn Array,
    right: &dyn Array,
    sort_options: SortOptions,
) -> DaftResult<Box<dyn Fn(usize, usize) -> Ordering + Send + Sync>> {
    macro_rules! downcast_float {
        ($t:ty) => {{
            let left = left
                .as_any()
                .downcast_ref::<PrimitiveArray<$t>>()
                .unwrap()
                .clone();
            let right = right
                .as_any()
                .downcast_ref::<PrimitiveArray<$t>>()
                .unwrap()
                .clone();
            Ok(build_nan_aware_comparator(left, right, sort_options))
        }};
    }
    match left.data_type() {
        DataType::Float16 => downcast_float!(Float16Type),
        DataType::Float32 => downcast_float!(Float32Type),
        DataType::Float64 => downcast_float!(Float64Type),
        _ => Ok(make_comparator(left, right, sort_options)?),
    }
}

/// Compare the values at two arbitrary indices in two arrays.
pub type DynPartialComparator = Box<dyn Fn(usize, usize) -> Option<Ordering> + Send + Sync>;

pub fn build_partial_compare_with_nulls(
    left: &dyn Array,
    right: &dyn Array,
    reversed: bool,
) -> DaftResult<DynPartialComparator> {
    // `reversed` is ambiguous, but based on historical behaviour, the default is to sort in ascending order and nulls last.
    let comparator = make_daft_comparator(
        left,
        right,
        SortOptions::new(
            /*descending=*/ reversed, /*nulls_first=*/ reversed,
        ),
    )?;

    let left_data = left.to_data();
    let right_data = right.to_data();
    Ok(Box::new(move |i: usize, j: usize| {
        match (left_data.is_valid(i), right_data.is_valid(j)) {
            (true, true) => Some(comparator(i, j)),
            (false, true) => Some(Ordering::Greater),
            (true, false) => Some(Ordering::Less),
            (false, false) => None,
        }
    }))
}

macro_rules! is_nearer_int {
    ($a:expr, $b:expr, $pv:expr) => {
        match ($a).abs_diff($pv).cmp(&($b).abs_diff($pv)) {
            Ordering::Less => true,
            Ordering::Greater => false,
            Ordering::Equal => $a > $b,
        }
    };
}

macro_rules! is_nearer_float {
    ($a:expr, $b:expr, $pv:expr) => {{
        let ad = ($a - $pv).abs();
        let bd = ($b - $pv).abs();
        match cmp_float(&ad, &bd) {
            Ordering::Less => true,
            Ordering::Greater => false,
            Ordering::Equal => cmp_float(&$a, &$b).is_gt(),
        }
    }};
}

/// Returns `true` if `a_arr[a_idx]` is strictly closer to `pivot_arr[pivot_idx]` than
/// `b_arr[b_idx]` is.  Type dispatch runs on every call; Numeric / Temporal types supported.
pub fn is_nearer(
    a_arr: &dyn Array,
    a_idx: usize,
    b_arr: &dyn Array,
    b_idx: usize,
    pivot_arr: &dyn Array,
    pivot_idx: usize,
) -> bool {
    if !a_arr.is_valid(a_idx) || !b_arr.is_valid(b_idx) || !pivot_arr.is_valid(pivot_idx) {
        unreachable!("null keys must be filtered before calling is_nearer");
    }

    macro_rules! extract_scalar {
        ($arr:expr, $T:ty, $i:expr) => {
            $arr.as_any()
                .downcast_ref::<PrimitiveArray<$T>>()
                .unwrap()
                .value($i)
        };
    }

    macro_rules! extract_and_cmp_int {
        ($T:ty) => {{
            let a = extract_scalar!(a_arr, $T, a_idx);
            let b = extract_scalar!(b_arr, $T, b_idx);
            let p = extract_scalar!(pivot_arr, $T, pivot_idx);
            is_nearer_int!(a, b, p)
        }};
    }

    macro_rules! extract_and_cmp_float {
        ($T:ty) => {{
            let a = extract_scalar!(a_arr, $T, a_idx);
            let b = extract_scalar!(b_arr, $T, b_idx);
            let p = extract_scalar!(pivot_arr, $T, pivot_idx);
            is_nearer_float!(a, b, p)
        }};
    }

    match a_arr.data_type() {
        DataType::Int8 => extract_and_cmp_int!(Int8Type),
        DataType::Int16 => extract_and_cmp_int!(Int16Type),
        DataType::Int32 | DataType::Date32 | DataType::Time32(_) => extract_and_cmp_int!(Int32Type),
        DataType::Int64
        | DataType::Date64
        | DataType::Timestamp(_, _)
        | DataType::Time64(_)
        | DataType::Duration(_) => extract_and_cmp_int!(Int64Type),
        DataType::UInt8 => extract_and_cmp_int!(UInt8Type),
        DataType::UInt16 => extract_and_cmp_int!(UInt16Type),
        DataType::UInt32 => extract_and_cmp_int!(UInt32Type),
        DataType::UInt64 => extract_and_cmp_int!(UInt64Type),
        DataType::Decimal128(_, _) => extract_and_cmp_int!(Decimal128Type),
        DataType::Float16 => {
            // Promote to f32; f16 has no Sub in std.
            let a = extract_scalar!(a_arr, Float16Type, a_idx).to_f32();
            let b = extract_scalar!(b_arr, Float16Type, b_idx).to_f32();
            let p = extract_scalar!(pivot_arr, Float16Type, pivot_idx).to_f32();
            is_nearer_float!(a, b, p)
        }
        DataType::Float32 => extract_and_cmp_float!(Float32Type),
        DataType::Float64 => extract_and_cmp_float!(Float64Type),
        t => unreachable!("nearest join: unsupported on-key type {t:?}"),
    }
}
