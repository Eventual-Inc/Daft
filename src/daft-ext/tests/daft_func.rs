#![cfg(any(feature = "arrow-56", feature = "arrow-57", feature = "arrow-58"))]

use std::sync::Arc;

use daft_ext::{
    helpers::_codegen::{
        Array, ArrayRef, BooleanArray, FixedSizeListArray, FixedSizeListBuilder, Float32Array,
        Float64Array, Int32Array, Int32Type, Int64Array, LargeBinaryArray, LargeListArray,
        LargeStringArray, PrimitiveBuilder, UInt64Array,
    },
    prelude::*,
};

// ── Helpers ────────────────────────────────────────────────────────

fn call_unary(func: &dyn DaftScalarFunction, input: ArrayRef) -> DaftResult<ArrayRef> {
    let data = export_array(input, "input")?;
    let result = func.call(vec![data])?;
    import_array(result)
}

fn call_binary(func: &dyn DaftScalarFunction, a: ArrayRef, b: ArrayRef) -> DaftResult<ArrayRef> {
    let da = export_array(a, "a")?;
    let db = export_array(b, "b")?;
    let result = func.call(vec![da, db])?;
    import_array(result)
}

// ── Primitive types ────────────────────────────────────────────────

#[daft_func]
fn double_i32(x: i32) -> i32 {
    x * 2
}

#[test]
fn test_primitive_i32() {
    let input: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let result = call_unary(&DoubleI32, input).unwrap();
    let arr = result.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(arr.values().as_ref(), &[2, 4, 6]);
}

#[daft_func]
fn add_f64(a: f64, b: f64) -> f64 {
    a + b
}

#[test]
fn test_primitive_f64_binary() {
    let a: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.5, 3.0]));
    let b: ArrayRef = Arc::new(Float64Array::from(vec![0.5, 0.5, 0.5]));
    let result = call_binary(&AddF64, a, b).unwrap();
    let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(arr.values().as_ref(), &[1.5, 3.0, 3.5]);
}

// ── Boolean ────────────────────────────────────────────────────────

#[daft_func]
fn negate(x: bool) -> bool {
    !x
}

#[test]
fn test_boolean() {
    let input: ArrayRef = Arc::new(BooleanArray::from(vec![true, false, true]));
    let result = call_unary(&Negate, input).unwrap();
    let arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(!arr.value(0));
    assert!(arr.value(1));
    assert!(!arr.value(2));
}

// ── String / &str ──────────────────────────────────────────────────

#[daft_func]
fn shout(s: &str) -> String {
    s.to_uppercase()
}

#[test]
fn test_string() {
    let input: ArrayRef = Arc::new(LargeStringArray::from(vec!["hello", "world"]));
    let result = call_unary(&Shout, input).unwrap();
    let arr = result.as_any().downcast_ref::<LargeStringArray>().unwrap();
    assert_eq!(arr.value(0), "HELLO");
    assert_eq!(arr.value(1), "WORLD");
}

// ── Binary / &[u8] ────────────────────────────────────────────────

#[daft_func]
fn byte_len(data: &[u8]) -> i64 {
    data.len() as i64
}

#[test]
fn test_binary() {
    let input: ArrayRef = Arc::new(LargeBinaryArray::from_iter_values(vec![
        b"\x00\x01".as_slice(),
        b"\xff\xfe\xfd".as_slice(),
    ]));
    let result = call_unary(&ByteLen, input).unwrap();
    let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(arr.values().as_ref(), &[2, 3]);
}

// ── Null propagation (non-Option params) ───────────────────────────

#[test]
fn test_null_propagation() {
    let input: ArrayRef = Arc::new(Int32Array::from(vec![Some(10), None, Some(30)]));
    let result = call_unary(&DoubleI32, input).unwrap();
    let arr = result.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(arr.value(0), 20);
    assert!(arr.is_null(1));
    assert_eq!(arr.value(2), 60);
}

#[test]
fn test_null_propagation_binary_args() {
    let a: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.0), None, Some(3.0)]));
    let b: ArrayRef = Arc::new(Float64Array::from(vec![Some(0.5), Some(0.5), None]));
    let result = call_binary(&AddF64, a, b).unwrap();
    let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(arr.value(0), 1.5);
    assert!(arr.is_null(1));
    assert!(arr.is_null(2));
}

#[test]
fn test_null_propagation_string() {
    let input: ArrayRef = Arc::new(LargeStringArray::from(vec![
        Some("hello"),
        None,
        Some("world"),
    ]));
    let result = call_unary(&Shout, input).unwrap();
    let arr = result.as_any().downcast_ref::<LargeStringArray>().unwrap();
    assert_eq!(arr.value(0), "HELLO");
    assert!(arr.is_null(1));
    assert_eq!(arr.value(2), "WORLD");
}

// ── Option<T> params (user handles nulls) ──────────────────────────

#[daft_func]
fn coalesce_i32(x: Option<i32>) -> i32 {
    x.unwrap_or(-1)
}

#[test]
fn test_option_input() {
    let input: ArrayRef = Arc::new(Int32Array::from(vec![Some(42), None, Some(7)]));
    let result = call_unary(&CoalesceI32, input).unwrap();
    let arr = result.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(arr.values().as_ref(), &[42, -1, 7]);
    assert!(!arr.is_null(1));
}

// ── Option<T> return (nullable output) ─────────────────────────────

#[daft_func]
fn safe_div(a: f64, b: f64) -> Option<f64> {
    if b == 0.0 { None } else { Some(a / b) }
}

#[test]
fn test_nullable_return() {
    let a: ArrayRef = Arc::new(Float64Array::from(vec![10.0, 5.0, 1.0]));
    let b: ArrayRef = Arc::new(Float64Array::from(vec![2.0, 0.0, 4.0]));
    let result = call_binary(&SafeDiv, a, b).unwrap();
    let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(arr.value(0), 5.0);
    assert!(arr.is_null(1));
    assert_eq!(arr.value(2), 0.25);
}

// ── DaftResult<T> return (fallible) ────────────────────────────────

#[daft_func]
fn strict_div(a: f64, b: f64) -> DaftResult<f64> {
    if b == 0.0 {
        Err(DaftError::RuntimeError("division by zero".into()))
    } else {
        Ok(a / b)
    }
}

#[test]
fn test_fallible_success() {
    let a: ArrayRef = Arc::new(Float64Array::from(vec![10.0, 6.0]));
    let b: ArrayRef = Arc::new(Float64Array::from(vec![2.0, 3.0]));
    let result = call_binary(&StrictDiv, a, b).unwrap();
    let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(arr.values().as_ref(), &[5.0, 2.0]);
}

#[test]
fn test_fallible_error() {
    let a: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));
    let b: ArrayRef = Arc::new(Float64Array::from(vec![0.0]));
    let err = match call_binary(&StrictDiv, a, b) {
        Err(e) => e,
        Ok(_) => panic!("expected an error"),
    };
    assert!(err.to_string().contains("division by zero"), "got: {err}");
}

// ── DaftResult<Option<T>> return (fallible + nullable) ─────────────

#[daft_func]
fn parse_int(s: &str) -> DaftResult<Option<i64>> {
    if s.is_empty() {
        Ok(None)
    } else {
        s.parse::<i64>()
            .map(Some)
            .map_err(|e| DaftError::RuntimeError(e.to_string()))
    }
}

#[test]
fn test_fallible_nullable_success() {
    let input: ArrayRef = Arc::new(LargeStringArray::from(vec!["42", "", "7"]));
    let result = call_unary(&ParseInt, input).unwrap();
    let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(arr.value(0), 42);
    assert!(arr.is_null(1));
    assert_eq!(arr.value(2), 7);
}

#[test]
fn test_fallible_nullable_error() {
    let input: ArrayRef = Arc::new(LargeStringArray::from(vec!["not_a_number"]));
    let result = call_unary(&ParseInt, input);
    assert!(result.is_err());
}

// ── Mixed: Option input + Option return ────────────────────────────

#[allow(clippy::single_option_map)]
#[daft_func]
fn maybe_double(x: Option<i32>) -> Option<i32> {
    x.map(|v| v * 2)
}

#[test]
fn test_option_in_option_out() {
    let input: ArrayRef = Arc::new(Int32Array::from(vec![Some(5), None, Some(0)]));
    let result = call_unary(&MaybeDouble, input).unwrap();
    let arr = result.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(arr.value(0), 10);
    assert!(arr.is_null(1));
    assert_eq!(arr.value(2), 0);
}

// ── Multiple types: u64, f32 ───────────────────────────────────────

#[daft_func]
fn u64_to_f32(x: u64) -> f32 {
    x as f32
}

#[test]
fn test_cross_type() {
    let input: ArrayRef = Arc::new(UInt64Array::from(vec![100, 200, 300]));
    let result = call_unary(&U64ToF32, input).unwrap();
    let arr = result.as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(arr.values().as_ref(), &[100.0, 200.0, 300.0]);
}

// ── Arg count validation ───────────────────────────────────────────

#[test]
fn test_wrong_arg_count() {
    let a: ArrayRef = Arc::new(Int32Array::from(vec![1]));
    let b: ArrayRef = Arc::new(Int32Array::from(vec![2]));
    let da = export_array(a, "a").unwrap();
    let db = export_array(b, "b").unwrap();
    let err = match DoubleI32.call(vec![da, db]) {
        Err(e) => e,
        Ok(_) => panic!("expected an error"),
    };
    assert!(
        err.to_string().contains("expected 1 argument"),
        "got: {err}"
    );
}

// ── name = "..." attribute ─────────────────────────────────────────

#[daft_func(name = "my_custom_name")]
fn aliased_func(x: i32) -> i32 {
    x + 1
}

#[test]
fn test_name_override() {
    let name = DaftScalarFunction::name(&AliasedFunc);
    assert_eq!(name.to_str().unwrap(), "my_custom_name");
}

// ── Vec<T> (LargeList) input ───────────────────────────────────────

#[daft_func]
fn sum_list(xs: Vec<i32>) -> i64 {
    xs.iter().map(|&x| x as i64).sum()
}

#[test]
fn test_vec_input() {
    let input: ArrayRef = Arc::new(LargeListArray::from_iter_primitive::<Int32Type, _, _>(
        vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            Some(vec![Some(4), Some(5)]),
            Some(vec![]),
        ],
    ));
    let result = call_unary(&SumList, input).unwrap();
    let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(arr.values().as_ref(), &[6, 9, 0]);
}

#[test]
fn test_vec_input_null_propagation() {
    let input: ArrayRef = Arc::new(LargeListArray::from_iter_primitive::<Int32Type, _, _>(
        vec![Some(vec![Some(1), Some(2)]), None, Some(vec![Some(10)])],
    ));
    let result = call_unary(&SumList, input).unwrap();
    let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(arr.value(0), 3);
    assert!(arr.is_null(1));
    assert_eq!(arr.value(2), 10);
}

// ── Vec<T> (LargeList) return ──────────────────────────────────────

#[daft_func]
fn range_list(n: i32) -> Vec<i64> {
    (0..n as i64).collect()
}

#[test]
fn test_vec_return() {
    let input: ArrayRef = Arc::new(Int32Array::from(vec![3, 0, 2]));
    let result = call_unary(&RangeList, input).unwrap();
    let arr = result.as_any().downcast_ref::<LargeListArray>().unwrap();

    let row0 = arr.value(0);
    assert_eq!(
        row0.as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[0, 1, 2]
    );
    let row1 = arr.value(1);
    assert!(
        row1.as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .is_empty()
    );
    let row2 = arr.value(2);
    assert_eq!(
        row2.as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[0, 1]
    );
}

#[test]
fn test_vec_return_null_propagation() {
    let input: ArrayRef = Arc::new(Int32Array::from(vec![Some(2), None, Some(1)]));
    let result = call_unary(&RangeList, input).unwrap();
    let arr = result.as_any().downcast_ref::<LargeListArray>().unwrap();
    assert!(!arr.is_null(0));
    assert!(arr.is_null(1));
    assert!(!arr.is_null(2));
}

// ── [T; N] (FixedSizeList) input ───────────────────────────────────

#[daft_func]
fn fixed_sum(xs: [i32; 3]) -> i32 {
    xs.iter().sum()
}

fn fixed_i32_array(rows: Vec<Option<[i32; 3]>>) -> ArrayRef {
    let mut builder = FixedSizeListBuilder::new(PrimitiveBuilder::<Int32Type>::new(), 3);
    for row in rows {
        match row {
            Some(vals) => {
                for v in vals {
                    builder.values().append_value(v);
                }
                builder.append(true);
            }
            None => {
                for _ in 0..3 {
                    builder.values().append_null();
                }
                builder.append(false);
            }
        }
    }
    Arc::new(builder.finish())
}

#[test]
fn test_fixed_array_input() {
    let input = fixed_i32_array(vec![Some([1, 2, 3]), Some([10, 20, 30])]);
    let result = call_unary(&FixedSum, input).unwrap();
    let arr = result.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(arr.values().as_ref(), &[6, 60]);
}

#[test]
fn test_fixed_array_input_null_propagation() {
    let input = fixed_i32_array(vec![Some([1, 2, 3]), None, Some([4, 5, 6])]);
    let result = call_unary(&FixedSum, input).unwrap();
    let arr = result.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(arr.value(0), 6);
    assert!(arr.is_null(1));
    assert_eq!(arr.value(2), 15);
}

// ── [T; N] (FixedSizeList) return ──────────────────────────────────

#[daft_func]
fn pair(x: f64) -> [f64; 2] {
    [x, x * 2.0]
}

#[test]
fn test_fixed_array_return() {
    let input: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 5.0]));
    let result = call_unary(&Pair, input).unwrap();
    let arr = result
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .unwrap();
    assert_eq!(arr.value_length(), 2);

    let row0 = arr.value(0);
    assert_eq!(
        row0.as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[1.0, 2.0]
    );
    let row1 = arr.value(1);
    assert_eq!(
        row1.as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[5.0, 10.0]
    );
}
