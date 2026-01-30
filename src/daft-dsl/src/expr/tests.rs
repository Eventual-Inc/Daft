use super::*;

#[test]
fn check_comparison_type() -> DaftResult<()> {
    let x = lit(10.);
    let y = lit(12);
    let schema = Schema::empty();

    let z = Expr::BinaryOp {
        left: x,
        right: y,
        op: Operator::Lt,
    };
    assert_eq!(z.get_type(&schema)?, DataType::Boolean);
    Ok(())
}

#[test]
fn check_alias_type() -> DaftResult<()> {
    let a = resolved_col("a");
    let b = a.alias("b");
    match b.as_ref() {
        Expr::Alias(..) => Ok(()),
        other => Err(common_error::DaftError::ValueError(format!(
            "expected expression to be a alias, got {other:?}"
        ))),
    }
}

#[test]
fn check_arithmetic_type() -> DaftResult<()> {
    let x = lit(10.);
    let y = lit(12);
    let schema = Schema::empty();

    let z = Expr::BinaryOp {
        left: x,
        right: y,
        op: Operator::Plus,
    };
    assert_eq!(z.get_type(&schema)?, DataType::Float64);

    let x = lit(10.);
    let y = lit(12);

    let z = Expr::BinaryOp {
        left: y,
        right: x,
        op: Operator::Plus,
    };
    assert_eq!(z.get_type(&schema)?, DataType::Float64);

    Ok(())
}

#[test]
fn check_arithmetic_type_with_columns() -> DaftResult<()> {
    let x = resolved_col("x");
    let y = resolved_col("y");
    let schema = Schema::new(vec![
        Field::new("x", DataType::Float64),
        Field::new("y", DataType::Int64),
    ]);

    let z = Expr::BinaryOp {
        left: x,
        right: y,
        op: Operator::Plus,
    };
    assert_eq!(z.get_type(&schema)?, DataType::Float64);

    let x = resolved_col("x");
    let y = resolved_col("y");

    let z = Expr::BinaryOp {
        left: y,
        right: x,
        op: Operator::Plus,
    };
    assert_eq!(z.get_type(&schema)?, DataType::Float64);

    Ok(())
}

#[test]
fn partition_compatibility_resolved_vs_bound_order_insensitive() -> DaftResult<()> {
    // Schema with columns a, b
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int64),
        Field::new("b", DataType::Utf8),
    ]);

    // Resolved columns in one order
    let resolved_cols = vec![resolved_col("a"), resolved_col("b")];
    // Bound columns in reversed order
    let bound_cols = BoundExpr::bind_all(&vec![resolved_col("b"), resolved_col("a")], &schema)?;

    assert!(is_partition_compatible(
        &resolved_cols,
        bound_cols.iter().map(|e| e.inner()),
    ));

    // Differing names should be incompatible
    let differing_resolved = vec![resolved_col("a"), resolved_col("c")];
    assert!(!is_partition_compatible(
        &differing_resolved,
        bound_cols.iter().map(|e| e.inner()),
    ));

    Ok(())
}
