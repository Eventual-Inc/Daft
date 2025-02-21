use daft_core::prelude::*;
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    lit, Expr, ExprRef,
};
use daft_functions::sequence::monotonically_increasing_id::{
    monotonically_increasing_id, MonotonicallyIncreasingId, SpecialFunction,
};

#[test]
fn test_monotonically_increasing_id_udf_marker() {
    let udf = MonotonicallyIncreasingId {};
    // Test that it implements SpecialFunction
    assert_eq!(
        std::any::type_name::<dyn SpecialFunction>(),
        "dyn daft_functions::sequence::monotonically_increasing_id::SpecialFunction"
    );
}

#[test]
fn test_monotonically_increasing_id_udf() {
    let udf = MonotonicallyIncreasingId {};
    let expr = monotonically_increasing_id();

    // Verify that the UDF is marked as special
    let scalar_fn = ScalarFunction::new(udf, vec![]);
    assert!(
        scalar_fn.is_special_function(),
        "MonotonicallyIncreasingId should be detected as a special function"
    );

    // Verify that the expression contains the special UDF
    if let Expr::ScalarFunction(func) = expr.as_ref() {
        assert!(
            func.is_special_function(),
            "Expression should contain a special function"
        );
    } else {
        panic!("Expected ScalarFunction expression");
    }
}

#[test]
fn test_monotonically_increasing_id_field() {
    let udf = MonotonicallyIncreasingId {};
    let schema = Schema::default();

    // Verify that the field type is UInt64
    let field = udf.to_field(&[], &schema).unwrap();
    assert_eq!(field.dtype, DataType::UInt64);

    // Verify that the UDF rejects inputs
    let literal_expr: ExprRef = lit(1i64).into();
    assert!(udf.to_field(&[literal_expr], &schema).is_err());
}

#[test]
fn test_monotonically_increasing_id_evaluate() {
    let udf = MonotonicallyIncreasingId {};

    // Verify that evaluate returns a NotImplemented error
    let result = udf.evaluate(&[]);
    assert!(matches!(
        result,
        Err(common_error::DaftError::NotImplemented(_))
    ));
}

#[test]
fn test_monotonically_increasing_id_expression() {
    let expr = monotonically_increasing_id();
    // Verify the expression contains our special UDF
    match expr.as_ref() {
        Expr::ScalarFunction(func) => {
            let udf = &func.udf;
            assert_eq!(udf.name(), "monotonically_increasing_id");
            assert!(udf.as_any().is::<MonotonicallyIncreasingId>());
        }
        _ => panic!("Expected ScalarFunction expression"),
    }
}
