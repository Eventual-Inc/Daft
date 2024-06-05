use arrow2::array::*;
use arrow2::compute::aggregate::{sum, sum_primitive};
use arrow2::compute::arithmetics;
use arrow2::datatypes::DataType;
use arrow2::scalar::{PrimitiveScalar, Scalar};

#[test]
fn test_primitive_array_sum() {
    let a = Int32Array::from_slice([1, 2, 3, 4, 5]);
    assert_eq!(
        &PrimitiveScalar::<i32>::from(Some(15)) as &dyn Scalar,
        sum(&a).unwrap().as_ref()
    );

    let a = a.to(DataType::Date32);
    assert_eq!(
        &PrimitiveScalar::<i32>::from(Some(15)).to(DataType::Date32) as &dyn Scalar,
        sum(&a).unwrap().as_ref()
    );
}

#[test]
fn test_primitive_array_float_sum() {
    let a = Float64Array::from_slice([1.1f64, 2.2, 3.3, 4.4, 5.5]);
    assert!((16.5 - sum_primitive(&a).unwrap()).abs() < f64::EPSILON);
}

#[test]
fn test_primitive_array_sum_with_nulls() {
    let a = Int32Array::from(&[None, Some(2), Some(3), None, Some(5)]);
    assert_eq!(10, sum_primitive(&a).unwrap());
}

#[test]
fn test_primitive_array_sum_all_nulls() {
    let a = Int32Array::from(&[None, None, None]);
    assert_eq!(None, sum_primitive(&a));
}

#[test]
fn test_primitive_array_sum_large_64() {
    let a: Int64Array = (1..=100)
        .map(|i| if i % 3 == 0 { Some(i) } else { None })
        .collect();
    let b: Int64Array = (1..=100)
        .map(|i| if i % 3 == 0 { Some(0) } else { Some(i) })
        .collect();
    // create an array that actually has non-zero values at the invalid indices
    let c = arithmetics::basic::add(&a, &b);
    assert_eq!(
        Some((1..=100).filter(|i| i % 3 == 0).sum()),
        sum_primitive(&c)
    );
}
