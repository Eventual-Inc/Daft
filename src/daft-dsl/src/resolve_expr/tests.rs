use super::*;

fn substitute_expr_getter_sugar(expr: ExprRef, schema: &Schema) -> DaftResult<ExprRef> {
    let struct_expr_map = calculate_struct_expr_map(schema);
    transform_struct_gets(expr, &struct_expr_map)
}

#[test]
fn test_substitute_expr_getter_sugar() -> DaftResult<()> {
    use crate::functions::struct_::get as struct_get;

    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)])?);

    assert_eq!(substitute_expr_getter_sugar(col("a"), &schema)?, col("a"));
    assert!(substitute_expr_getter_sugar(col("a.b"), &schema).is_err());
    assert!(matches!(
        substitute_expr_getter_sugar(col("a.b"), &schema).unwrap_err(),
        DaftError::ValueError(..)
    ));

    let schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        DataType::Struct(vec![Field::new("b", DataType::Int64)]),
    )])?);

    assert_eq!(substitute_expr_getter_sugar(col("a"), &schema)?, col("a"));
    assert_eq!(
        substitute_expr_getter_sugar(col("a.b"), &schema)?,
        struct_get(col("a"), "b")
    );
    assert_eq!(
        substitute_expr_getter_sugar(col("a.b").alias("c"), &schema)?,
        struct_get(col("a"), "b").alias("c")
    );

    let schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        DataType::Struct(vec![Field::new(
            "b",
            DataType::Struct(vec![Field::new("c", DataType::Int64)]),
        )]),
    )])?);

    assert_eq!(
        substitute_expr_getter_sugar(col("a.b"), &schema)?,
        struct_get(col("a"), "b")
    );
    assert_eq!(
        substitute_expr_getter_sugar(col("a.b.c"), &schema)?,
        struct_get(struct_get(col("a"), "b"), "c")
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "a",
            DataType::Struct(vec![Field::new(
                "b",
                DataType::Struct(vec![Field::new("c", DataType::Int64)]),
            )]),
        ),
        Field::new("a.b", DataType::Int64),
    ])?);

    assert_eq!(
        substitute_expr_getter_sugar(col("a.b"), &schema)?,
        col("a.b")
    );
    assert_eq!(
        substitute_expr_getter_sugar(col("a.b.c"), &schema)?,
        struct_get(struct_get(col("a"), "b"), "c")
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "a",
            DataType::Struct(vec![Field::new("b.c", DataType::Int64)]),
        ),
        Field::new(
            "a.b",
            DataType::Struct(vec![Field::new("c", DataType::Int64)]),
        ),
    ])?);

    assert_eq!(
        substitute_expr_getter_sugar(col("a.b.c"), &schema)?,
        struct_get(col("a.b"), "c")
    );

    Ok(())
}

#[test]
fn test_find_wildcards() -> DaftResult<()> {
    let schema = Schema::new(vec![
        Field::new(
            "a",
            DataType::Struct(vec![Field::new("b.*", DataType::Int64)]),
        ),
        Field::new("c.*", DataType::Int64),
    ])?;
    let struct_expr_map = calculate_struct_expr_map(&schema);

    let wildcards = find_wildcards(col("test"), &struct_expr_map);
    assert!(wildcards.is_empty());

    let wildcards = find_wildcards(col("*"), &struct_expr_map);
    assert!(wildcards.len() == 1 && wildcards.first().unwrap().as_ref() == "*");

    let wildcards = find_wildcards(col("t*"), &struct_expr_map);
    assert!(wildcards.len() == 1 && wildcards.first().unwrap().as_ref() == "t*");

    let wildcards = find_wildcards(col("a.*"), &struct_expr_map);
    assert!(wildcards.len() == 1 && wildcards.first().unwrap().as_ref() == "a.*");

    let wildcards = find_wildcards(col("c.*"), &struct_expr_map);
    assert!(wildcards.is_empty());

    let wildcards = find_wildcards(col("a.b.*"), &struct_expr_map);
    assert!(wildcards.is_empty());

    let wildcards = find_wildcards(col("a.b*"), &struct_expr_map);
    assert!(wildcards.len() == 1 && wildcards.first().unwrap().as_ref() == "a.b*");

    // nested expression
    let wildcards = find_wildcards(col("t*").add(col("a.*")), &struct_expr_map);
    assert!(wildcards.len() == 2);
    assert!(wildcards.iter().any(|s| s.as_ref() == "t*"));
    assert!(wildcards.iter().any(|s| s.as_ref() == "a.*"));

    let wildcards = find_wildcards(col("t*").add(col("a")), &struct_expr_map);
    assert!(wildcards.len() == 1 && wildcards.first().unwrap().as_ref() == "t*");

    // schema containing *
    let schema = Schema::new(vec![Field::new("*", DataType::Int64)])?;
    let struct_expr_map = calculate_struct_expr_map(&schema);

    let wildcards = find_wildcards(col("*"), &struct_expr_map);
    assert!(wildcards.is_empty());

    Ok(())
}
