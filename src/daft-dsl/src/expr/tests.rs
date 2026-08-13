use serde::{Deserialize, Serialize};

use super::*;
use crate::functions::{ScalarUDF, scalar::EvalContext};

/// A minimal builtin `ScalarFn` used to pin down `Not`/`IsNull`/`NotNull` field-name
/// resolution: its `get_return_field` deliberately returns a name that is unrelated to
/// (and thus differs from) its first argument's name, mirroring real builtins like
/// `st_intersects` whose return-field name ("st_intersects") differs from a first
/// argument aliased to something else (e.g. "g").
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct RenamingBoolFn;

#[typetag::serde]
impl ScalarUDF for RenamingBoolFn {
    fn name(&self) -> &'static str {
        "renaming_bool_fn"
    }

    fn call(&self, args: FunctionArgs<Series>, _ctx: &EvalContext) -> DaftResult<Series> {
        let input = args.first().expect("expected 1 input");
        Ok(input.rename(self.name()))
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let input = args.first().expect("expected 1 input");
        let input_field = input.to_field(schema)?;
        Ok(Field::new(self.name(), input_field.dtype))
    }
}

/// Root-cause regression test: `Not`/`IsNull`/`NotNull::to_field` must name their output
/// after the child's *actual evaluated* field name, not the child's first-argument name
/// (`Expr::name()`). For a builtin `ScalarFn` whose first argument is aliased, those two
/// names differ -- which is exactly the case that broke `~st_intersects(a, b)`.
#[test]
fn not_over_builtin_scalar_fn_uses_childs_evaluated_field_name() -> DaftResult<()> {
    let schema = Schema::new(vec![Field::new("x", DataType::Boolean)]);
    let aliased_arg = resolved_col("x").alias("g");
    let scalar_expr: ExprRef = ScalarFn::builtin(RenamingBoolFn, vec![aliased_arg]).into();

    // Sanity-check the premise: `Expr::name()` (first-arg name) differs from what the
    // scalar fn actually evaluates/`to_field`s to (its own return-field name).
    #[allow(deprecated)]
    let scalar_name = scalar_expr.name().to_string();
    assert_eq!(scalar_name, "g");
    let scalar_field = scalar_expr.to_field(&schema)?;
    assert_eq!(scalar_field.name.as_ref(), "renaming_bool_fn");
    assert_ne!(scalar_name, scalar_field.name.as_ref());

    // Not::to_field's expected name must match the child's evaluated name, i.e. what
    // `Not`'s evaluation (`!child_series`) actually produces.
    let not_field = scalar_expr.clone().not().to_field(&schema)?;
    assert_eq!(not_field.name, scalar_field.name);
    assert_eq!(not_field.dtype, DataType::Boolean);

    // Same property for IsNull/NotNull.
    let is_null_field = scalar_expr.clone().is_null().to_field(&schema)?;
    assert_eq!(is_null_field.name, scalar_field.name);
    assert_eq!(is_null_field.dtype, DataType::Boolean);

    let not_null_field = scalar_expr.not_null().to_field(&schema)?;
    assert_eq!(not_null_field.name, scalar_field.name);
    assert_eq!(not_null_field.dtype, DataType::Boolean);

    Ok(())
}

/// Root-cause regression test: `IfElse::to_field` with a constant-`false` predicate must
/// name its output after `if_true`'s *actual evaluated* field name, not `Expr::name()`
/// (the first-argument name). Both runtime eval paths in `daft-recordbatch` rename the
/// result via the schema-aware `if_true.get_name(schema)`, so a `to_field` that disagrees
/// trips the "Mismatch of expected expression name" consistency check -- which is exactly
/// what broke `when(lit(False), st_area(geom)).otherwise(None)`.
#[test]
fn if_else_with_literal_predicate_uses_if_trues_evaluated_field_name() -> DaftResult<()> {
    let schema = Schema::new(vec![Field::new("x", DataType::Boolean)]);
    let aliased_arg = resolved_col("x").alias("g");
    let if_true: ExprRef = ScalarFn::builtin(RenamingBoolFn, vec![aliased_arg]).into();

    // Sanity-check the premise: `Expr::name()` (first-arg name) differs from what the
    // scalar fn actually evaluates/`to_field`s to (its own return-field name).
    #[allow(deprecated)]
    let if_true_name = if_true.name().to_string();
    assert_eq!(if_true_name, "g");
    let if_true_field = if_true.to_field(&schema)?;
    assert_eq!(if_true_field.name.as_ref(), "renaming_bool_fn");
    assert_ne!(if_true_name, if_true_field.name.as_ref());

    // Constant-`false`: the result takes `if_false`'s *value* but `if_true`'s *name*.
    let false_field = lit(false)
        .if_else(if_true.clone(), null_lit())
        .to_field(&schema)?;
    assert_eq!(false_field.name, if_true_field.name);

    // Constant-`true` already resolves through `if_true.to_field`; pin it so the two
    // literal arms stay consistent with each other.
    let true_field = lit(true)
        .if_else(if_true.clone(), null_lit())
        .to_field(&schema)?;
    assert_eq!(true_field.name, if_true_field.name);

    // Non-constant predicates were never broken -- guard against a fix that regresses them.
    let dynamic_field = resolved_col("x")
        .if_else(if_true, null_lit())
        .to_field(&schema)?;
    assert_eq!(dynamic_field.name, if_true_field.name);

    Ok(())
}

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

// Partition-compatibility helpers operate on bound columns.
fn bound(index: usize, name: &str) -> crate::expr::bound_expr::BoundExpr {
    crate::expr::bound_expr::BoundExpr::new_unchecked(bound_col(
        index,
        Field::new(name, DataType::Int64),
    ))
}

#[test]
fn is_exact_partition_match_is_order_independent_set_equality() {
    let a = bound(0, "a");
    let b = bound(1, "b");
    let c = bound(2, "c");
    assert!(is_exact_partition_match(
        &[a.clone(), b.clone()],
        &[b.clone(), a.clone()]
    ));
    // A superset is NOT an exact match.
    assert!(!is_exact_partition_match(
        &[a.clone(), b.clone()],
        &[a, b, c]
    ));
}

#[test]
fn clustering_is_covered_by_equal_sets() {
    let a = bound(0, "a");
    let b = bound(1, "b");
    assert!(clustering_is_covered_by(&[a.clone(), b.clone()], &[b, a]));
}

#[test]
fn clustering_is_covered_by_op_is_strict_superset() {
    // op partitions by {a, b, c}, input is clustered by {a, b} => covered (refinement).
    let a = bound(0, "a");
    let b = bound(1, "b");
    let c = bound(2, "c");
    assert!(clustering_is_covered_by(
        &[a.clone(), b.clone()],
        &[a, b, c]
    ));
}

#[test]
fn clustering_is_covered_by_input_richer_is_not_covered() {
    // input clustered by {a, b, c}, op partitions by {a, b} => NOT covered (unsound to skip).
    let a = bound(0, "a");
    let b = bound(1, "b");
    let c = bound(2, "c");
    assert!(!clustering_is_covered_by(
        &[a.clone(), b.clone(), c],
        &[a, b]
    ));
}

#[test]
fn clustering_is_covered_by_empty_input_is_not_covered() {
    let a = bound(0, "a");
    let input: [crate::expr::bound_expr::BoundExpr; 0] = [];
    assert!(!clustering_is_covered_by(&input, &[a]));
}

#[test]
fn clustering_is_covered_by_expression_keys() {
    // Expression-valued keys must compare structurally, not just bare columns.
    let producer = bound(0, "producer");
    let hour = crate::expr::bound_expr::BoundExpr::new_unchecked(binary_op(
        Operator::FloorDivide,
        bound_col(1, Field::new("id", DataType::Int64)),
        lit(3_600_000i64),
    ));
    // op adds an extra sub-bucket key => still covered.
    assert!(clustering_is_covered_by(
        &[producer.clone(), hour.clone()],
        &[producer, hour, bound(2, "bucket")]
    ));
}
