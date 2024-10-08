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
    let a = col("a");
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
    let x = col("x");
    let y = col("y");
    let schema = Schema::new(vec![
        Field::new("x", DataType::Float64),
        Field::new("y", DataType::Int64),
    ])?;

    let z = Expr::BinaryOp {
        left: x,
        right: y,
        op: Operator::Plus,
    };
    assert_eq!(z.get_type(&schema)?, DataType::Float64);

    let x = col("x");
    let y = col("y");

    let z = Expr::BinaryOp {
        left: y,
        right: x,
        op: Operator::Plus,
    };
    assert_eq!(z.get_type(&schema)?, DataType::Float64);

    Ok(())
}
