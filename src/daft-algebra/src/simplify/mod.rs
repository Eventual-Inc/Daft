mod boolean;
mod null;
mod numeric;

use boolean::{simplify_binary_compare, simplify_boolean_expr};
use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_dsl::{lit, Expr, ExprRef, LiteralValue};
use daft_schema::schema::SchemaRef;
use null::simplify_expr_with_null;
use numeric::simplify_numeric_expr;

/// Recursively simplify expression.
pub fn simplify_expr(expr: ExprRef, schema: &SchemaRef) -> DaftResult<Transformed<ExprRef>> {
    let simplify_fns = [
        simplify_boolean_expr,
        simplify_binary_compare,
        simplify_expr_with_null,
        simplify_numeric_expr,
        simplify_misc_expr,
        simplify_is_in_expr,
    ];

    // Our simplify rules currently require bottom-up traversal to work
    // If we introduce top-down rules in the future, please add a separate pass
    // on the expression instead of changing this.
    expr.transform_up(|node| {
        let dtype = node.to_field(schema)?.dtype;

        let transformed = simplify_fns
            .into_iter()
            .try_fold(Transformed::no(node), |transformed, f| {
                transformed.transform_data(|e| f(e, schema))
            })?;

        // cast back to original dtype if necessary
        transformed.map_data(|new_node| {
            Ok(if new_node.to_field(schema)?.dtype == dtype {
                new_node
            } else {
                new_node.cast(&dtype)
            })
        })
    })
}

fn simplify_misc_expr(expr: ExprRef, schema: &SchemaRef) -> DaftResult<Transformed<ExprRef>> {
    Ok(match expr.as_ref() {
        // a BETWEEN low AND high --> a >= low AND e <= high
        Expr::Between(between, low, high) => Transformed::yes(
            between
                .clone()
                .lt_eq(high.clone())
                .and(between.clone().gt_eq(low.clone())),
        ),
        // CAST(e AS dtype) -> e if e.dtype == dtype
        Expr::Cast(e, dtype) if e.get_type(schema)? == *dtype => Transformed::yes(e.clone()),
        _ => Transformed::no(expr),
    })
}

const MAX_IS_IN_CHAIN_LENGTH: usize = 5;

fn simplify_is_in_expr(expr: ExprRef, _schema: &SchemaRef) -> DaftResult<Transformed<ExprRef>> {
    Ok(match expr.as_ref() {
        Expr::IsIn(_, list) if list.is_empty() => Transformed::yes(lit(false)),
        // If the list is a small literal list, we can just make it an OR chain of eqs, e.g.
        // e IN (1, 2, 3) -> e = 1 OR e = 2 OR e = 3
        Expr::IsIn(e, list)
            if list.len() <= MAX_IS_IN_CHAIN_LENGTH
                && list.iter().all(|e| matches!(e.as_ref(), Expr::Literal(l) if !matches!(l, LiteralValue::Series(_)))) =>
        {
            let chain_of_eqs = list
                .iter()
                .map(|item| e.clone().eq(item.clone()));
            Transformed::yes(chain_of_eqs.reduce(|a, b| a.or(b)).unwrap())
        }
        _ => Transformed::no(expr),
    })
}
#[cfg(test)]
mod test {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_dsl::{col, lit, null_lit, ExprRef};
    use daft_schema::{
        dtype::DataType,
        field::Field,
        schema::{Schema, SchemaRef},
    };
    use rstest::{fixture, rstest};

    use crate::simplify_expr;

    #[fixture]
    fn schema() -> SchemaRef {
        Arc::new(
            Schema::new(vec![
                Field::new("bool", DataType::Boolean),
                Field::new("int", DataType::Int32),
                Field::new("a", DataType::Boolean),
                Field::new("b", DataType::Boolean),
                Field::new("c", DataType::Boolean),
            ])
            .unwrap(),
        )
    }

    #[rstest]
    // true = A  --> A
    #[case(col("bool").eq(lit(true)), col("bool"))]
    // false = A --> !A
    #[case(col("bool").eq(lit(false)), col("bool").not())]
    // A == true ---> A
    #[case(col("bool").eq(lit(true)), col("bool"))]
    // null = A --> null
    #[case(null_lit().eq(col("bool")), null_lit().cast(&DataType::Boolean))]
    // A == false ---> !A
    #[case(col("bool").eq(lit(false)), col("bool").not())]
    // true != A  --> !A
    #[case(lit(true).not_eq(col("bool")), col("bool").not())]
    // false != A --> A
    #[case(lit(false).not_eq(col("bool")), col("bool"))]
    // true OR A  --> true
    #[case(lit(true).or(col("bool")), lit(true))]
    // false OR A  --> A
    #[case(lit(false).or(col("bool")), col("bool"))]
    // A OR true  --> true
    #[case(col("bool").or(lit(true)), lit(true))]
    // A OR false --> A
    #[case(col("bool").or(lit(false)), col("bool"))]
    // (A OR B) AND (A OR C) -> A OR (B AND C)
    #[case((col("a").or(col("b"))).and(col("a").or(col("c"))), col("a").or(col("b").and(col("c"))))]
    // (A AND B) OR (A AND C) -> A AND (B OR C)
    #[case((col("a").and(col("b"))).or(col("a").and(col("c"))), col("a").and(col("b").or(col("c"))))]
    fn test_simplify_bool_exprs(
        #[case] input: ExprRef,
        #[case] expected: ExprRef,
        schema: SchemaRef,
    ) -> DaftResult<()> {
        let optimized = simplify_expr(input, &schema)?;

        assert!(optimized.transformed);
        assert_eq!(optimized.data, expected);
        Ok(())
    }

    #[rstest]
    // A * 1 --> A
    #[case(col("int").mul(lit(1)), col("int"))]
    // 1 * A --> A
    #[case(lit(1).mul(col("int")), col("int"))]
    // A / 1 --> A
    #[case(col("int").div(lit(1)), col("int").cast(&DataType::Float64))]
    // A + 0 --> A
    #[case(col("int").add(lit(0)), col("int"))]
    // A - 0 --> A
    #[case(col("int").sub(lit(0)), col("int"))]
    fn test_math_exprs(
        #[case] input: ExprRef,
        #[case] expected: ExprRef,
        schema: SchemaRef,
    ) -> DaftResult<()> {
        let optimized = simplify_expr(input, &schema)?;

        assert!(optimized.transformed);
        assert_eq!(optimized.data, expected);
        Ok(())
    }

    #[rstest]
    fn test_between(schema: SchemaRef) -> DaftResult<()> {
        let input = col("int").between(lit(1), lit(10));
        let expected = col("int").lt_eq(lit(10)).and(col("int").gt_eq(lit(1)));

        let optimized = simplify_expr(input, &schema)?;

        assert!(optimized.transformed);
        assert_eq!(optimized.data, expected);
        Ok(())
    }

    #[rstest]
    // One element list, can transform to eq
    // e IN (1) --> e = 1
    #[case(col("int").is_in(vec![lit(1)]), col("int").eq(lit(1)))]
    // Small list, can transform to ORs
    // e IN (1, 2, 3, 4, 5) --> e = 1 OR e = 2 OR e = 3 OR e = 4 OR e = 5
    #[case(col("int").is_in(vec![lit(1), lit(2), lit(3), lit(4), lit(5)]), col("int").eq(lit(1)).or(col("int").eq(lit(2))).or(col("int").eq(lit(3))).or(col("int").eq(lit(4))).or(col("int").eq(lit(5))))]
    fn test_is_in_exprs_can_chain_or_clauses(
        #[case] input: ExprRef,
        #[case] expected: ExprRef,
        schema: SchemaRef,
    ) -> DaftResult<()> {
        let optimized = simplify_expr(input, &schema)?;

        assert!(optimized.transformed);
        assert_eq!(optimized.data, expected);
        Ok(())
    }

    #[rstest]
    // e IN (1, 2, 3, 4, 5, 6) --> e IN (1, 2, 3, 4, 5, 6)
    #[case(col("int").is_in(vec![lit(1), lit(2), lit(3), lit(4), lit(5), lit(6)]), col("int").is_in(vec![lit(1), lit(2), lit(3), lit(4), lit(5), lit(6)]))]
    fn test_is_in_exprs_more_than_max_chain_length(
        #[case] input: ExprRef,
        #[case] expected: ExprRef,
        schema: SchemaRef,
    ) -> DaftResult<()> {
        let optimized = simplify_expr(input, &schema)?;

        assert!(!optimized.transformed);
        assert_eq!(optimized.data, expected);
        Ok(())
    }
}
