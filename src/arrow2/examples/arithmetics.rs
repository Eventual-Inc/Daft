use arrow2::array::{Array, PrimitiveArray};
use arrow2::compute::arithmetics::basic::*;
use arrow2::compute::arithmetics::{add as dyn_add, can_add};
use arrow2::compute::arity::{binary, unary};
use arrow2::datatypes::DataType;

fn main() {
    // say we have two arrays
    let array0 = PrimitiveArray::<i64>::from(&[Some(1), Some(2), Some(3)]);
    let array1 = PrimitiveArray::<i64>::from(&[Some(4), None, Some(6)]);

    // we can add them as follows:
    let added = add(&array0, &array1);
    assert_eq!(
        added,
        PrimitiveArray::<i64>::from(&[Some(5), None, Some(9)])
    );

    // subtract:
    let subtracted = sub(&array0, &array1);
    assert_eq!(
        subtracted,
        PrimitiveArray::<i64>::from(&[Some(-3), None, Some(-3)])
    );

    // add a scalar:
    let plus10 = add_scalar(&array0, &10);
    assert_eq!(
        plus10,
        PrimitiveArray::<i64>::from(&[Some(11), Some(12), Some(13)])
    );

    // a similar API for trait objects:
    let array0 = &array0 as &dyn Array;
    let array1 = &array1 as &dyn Array;

    // check whether the logical types support addition.
    assert!(can_add(array0.data_type(), array1.data_type()));

    // add them
    let added = dyn_add(array0, array1);
    assert_eq!(
        PrimitiveArray::<i64>::from(&[Some(5), None, Some(9)]),
        added.as_ref(),
    );

    // a more exotic implementation: arbitrary binary operations
    // this is compiled to SIMD when intrinsics exist.
    let array0 = PrimitiveArray::<i64>::from(&[Some(1), Some(2), Some(3)]);
    let array1 = PrimitiveArray::<i64>::from(&[Some(4), None, Some(6)]);

    let op = |x: i64, y: i64| x.pow(2) + y.pow(2);
    let r = binary(&array0, &array1, DataType::Int64, op);
    assert_eq!(
        r,
        PrimitiveArray::<i64>::from(&[Some(1 + 16), None, Some(9 + 36)])
    );

    // arbitrary unary operations
    // this is compiled to SIMD when intrinsics exist.
    let array0 = PrimitiveArray::<f64>::from(&[Some(4.0), None, Some(6.0)]);
    let r = unary(
        &array0,
        |x| x.cos().powi(2) + x.sin().powi(2),
        DataType::Float64,
    );
    assert!((r.values()[0] - 1.0).abs() < 0.0001);
    assert!(r.is_null(1));
    assert!((r.values()[2] - 1.0).abs() < 0.0001);

    // finally, a transformation that changes types:
    let array0 = PrimitiveArray::<f64>::from(&[Some(4.4), None, Some(4.6)]);
    let rounded = unary(&array0, |x| x.round() as i64, DataType::Int64);
    assert_eq!(
        rounded,
        PrimitiveArray::<i64>::from(&[Some(4), None, Some(5)])
    );
}
