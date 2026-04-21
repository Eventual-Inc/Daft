use common_error::DaftResult;
use daft_core::prelude::{CountMode, DataType, Literal, Operator, Schema};
use daft_dsl::{
    AggExpr, ApproxPercentileParams, Column, Expr, ExprRef, SketchType, bound_col,
    expr::bound_expr::{BoundAggExpr, BoundExpr},
    functions::agg::merge_mean,
    lit, null_lit,
};
use daft_functions::numeric::sqrt;
use daft_functions_list::{count_distinct, distinct};
use indexmap::IndexSet;

pub fn populate_aggregation_stages_bound(
    aggregations: &[BoundAggExpr],
    schema: &Schema,
    group_by: &[BoundExpr],
) -> DaftResult<(Vec<BoundAggExpr>, Vec<BoundAggExpr>, Vec<BoundExpr>)> {
    let (
        (first_stage_aggs, _first_stage_schema),
        (second_stage_aggs, _second_stage_schema),
        final_exprs,
    ) = populate_aggregation_stages_bound_with_schema(aggregations, schema, group_by)?;

    Ok((first_stage_aggs, second_stage_aggs, final_exprs))
}

#[allow(clippy::type_complexity)]
pub fn populate_aggregation_stages_bound_with_schema(
    aggregations: &[BoundAggExpr],
    schema: &Schema,
    group_by: &[BoundExpr],
) -> DaftResult<(
    (Vec<BoundAggExpr>, Schema),
    (Vec<BoundAggExpr>, Schema),
    Vec<BoundExpr>,
)> {
    let mut first_stage_aggs = IndexSet::new();
    let mut second_stage_aggs = IndexSet::new();

    #[derive(Clone, Copy)]
    enum SumLiteralRewriteKind {
        Add,
        SubtractLiteral,
        LiteralMinusInput,
    }

    struct SumLiteralRewrite {
        input_expr: ExprRef,
        literal_expr: ExprRef,
        kind: SumLiteralRewriteKind,
    }

    fn extract_rewrite_input(expr: &ExprRef, schema: &Schema) -> DaftResult<Option<ExprRef>> {
        let field = expr.to_field(schema)?;
        if !field.dtype.is_integer() {
            return Ok(None);
        }

        match expr.as_ref() {
            Expr::Column(Column::Bound(_)) => Ok(Some(expr.clone())),
            Expr::Alias(inner, _) => extract_rewrite_input(inner, schema),
            _ => Ok(None),
        }
    }

    fn extract_rewrite_literal(expr: &ExprRef) -> Option<ExprRef> {
        match expr.as_ref() {
            Expr::Literal(literal)
                if literal.get_type().is_integer() && !matches!(literal, Literal::Null) =>
            {
                Some(expr.clone())
            }
            _ => None,
        }
    }

    fn extract_sum_literal_rewrite(
        expr: &ExprRef,
        schema: &Schema,
        output_dtype: &DataType,
    ) -> DaftResult<Option<SumLiteralRewrite>> {
        if !output_dtype.is_integer() {
            return Ok(None);
        }

        let Expr::BinaryOp { op, left, right } = expr.as_ref() else {
            return Ok(None);
        };

        let left_input = extract_rewrite_input(left, schema)?;
        let right_input = extract_rewrite_input(right, schema)?;
        let left_literal = extract_rewrite_literal(left);
        let right_literal = extract_rewrite_literal(right);

        let rewrite = match op {
            Operator::Plus => match (left_input, right_input, left_literal, right_literal) {
                (Some(input_expr), None, None, Some(literal_expr))
                | (None, Some(input_expr), Some(literal_expr), None) => Some(SumLiteralRewrite {
                    input_expr,
                    literal_expr,
                    kind: SumLiteralRewriteKind::Add,
                }),
                _ => None,
            },
            Operator::Minus => match (left_input, right_input, left_literal, right_literal) {
                (Some(input_expr), None, None, Some(literal_expr)) => Some(SumLiteralRewrite {
                    input_expr,
                    literal_expr,
                    kind: SumLiteralRewriteKind::SubtractLiteral,
                }),
                (None, Some(input_expr), Some(literal_expr), None) => Some(SumLiteralRewrite {
                    input_expr,
                    literal_expr,
                    kind: SumLiteralRewriteKind::LiteralMinusInput,
                }),
                _ => None,
            },
            _ => None,
        };

        Ok(rewrite)
    }

    let group_by_fields = group_by
        .iter()
        .map(|expr| expr.inner().to_field(schema))
        .collect::<DaftResult<Vec<_>>>()?;
    let mut first_stage_schema = Schema::new(group_by_fields);
    let mut second_stage_schema = first_stage_schema.clone();

    let mut final_exprs = group_by
        .iter()
        .enumerate()
        .map(|(i, e)| {
            let field = e.inner().to_field(schema)?;

            Ok(BoundExpr::new_unchecked(bound_col(i, field)))
        })
        .collect::<DaftResult<Vec<_>>>()?;

    /// Add an agg expr to the specified stage, returning a column expression to reference it.
    ///
    /// If an equivalent expr is already in the stage, simply refer to that expr.
    fn add_to_stage(
        stage: &mut IndexSet<BoundAggExpr>,
        input_schema: &Schema,
        output_schema: &mut Schema,
        groupby_count: usize,
        expr: AggExpr,
    ) -> DaftResult<ExprRef> {
        let (index, is_new) = stage.insert_full(BoundAggExpr::new_unchecked(expr.clone()));
        let index = index + groupby_count;

        if is_new {
            let field = expr.to_field(input_schema)?;
            output_schema.append(field);
        }

        Ok(bound_col(index, output_schema[index].clone()))
    }

    // using macros here instead of closures because borrow checker complains
    // about simultaneous reference + mutable reference
    macro_rules! first_stage {
        ($expr:expr) => {
            add_to_stage(
                &mut first_stage_aggs,
                schema,
                &mut first_stage_schema,
                group_by.len(),
                $expr,
            )?
        };
    }

    macro_rules! second_stage {
        ($expr:expr) => {
            add_to_stage(
                &mut second_stage_aggs,
                &first_stage_schema,
                &mut second_stage_schema,
                group_by.len(),
                $expr,
            )?
        };
    }

    for agg_expr in aggregations {
        let output_field = agg_expr.as_ref().to_field(schema)?;
        let output_name = output_field.name.as_ref();

        let mut final_stage = |expr: ExprRef| {
            final_exprs.push(BoundExpr::new_unchecked(expr.alias(output_name)));
        };

        match agg_expr.as_ref() {
            AggExpr::Count(expr, count_mode) => {
                let count_col = first_stage!(AggExpr::Count(expr.clone(), *count_mode));
                let global_count_col = second_stage!(AggExpr::Sum(count_col));
                final_stage(global_count_col);
            }
            AggExpr::Product(expr) => {
                let product_col = first_stage!(AggExpr::Product(expr.clone()));
                let global_product_col = second_stage!(AggExpr::Product(product_col));
                final_stage(global_product_col);
            }
            AggExpr::CountDistinct(expr) => {
                let set_agg_col = first_stage!(AggExpr::Set(expr.clone()));
                let concat_col = second_stage!(AggExpr::Concat(set_agg_col, None));
                final_stage(count_distinct(concat_col));
            }
            AggExpr::Sum(expr) => {
                if let Some(rewrite) =
                    extract_sum_literal_rewrite(expr, schema, &output_field.dtype)?
                {
                    let sum_col = first_stage!(AggExpr::Sum(rewrite.input_expr.clone()));
                    let count_col =
                        first_stage!(AggExpr::Count(rewrite.input_expr, CountMode::Valid));

                    let global_sum_col =
                        second_stage!(AggExpr::Sum(sum_col)).cast(&output_field.dtype);
                    let global_count_col =
                        second_stage!(AggExpr::Sum(count_col)).cast(&output_field.dtype);
                    let literal_contribution = rewrite
                        .literal_expr
                        .cast(&output_field.dtype)
                        .mul(global_count_col);

                    let final_expr = match rewrite.kind {
                        SumLiteralRewriteKind::Add => global_sum_col.add(literal_contribution),
                        SumLiteralRewriteKind::SubtractLiteral => {
                            global_sum_col.sub(literal_contribution)
                        }
                        SumLiteralRewriteKind::LiteralMinusInput => {
                            literal_contribution.sub(global_sum_col)
                        }
                    };

                    final_stage(final_expr);
                } else {
                    let sum_col = first_stage!(AggExpr::Sum(expr.clone()));
                    let global_sum_col = second_stage!(AggExpr::Sum(sum_col));
                    final_stage(global_sum_col);
                }
            }
            AggExpr::ApproxPercentile(ApproxPercentileParams {
                child,
                percentiles,
                force_list_output,
            }) => {
                let percentiles = percentiles.iter().map(|p| p.0).collect::<Vec<f64>>();
                let approx_sketch_col =
                    first_stage!(AggExpr::ApproxSketch(child.clone(), SketchType::DDSketch));
                let merge_sketch_col = second_stage!(AggExpr::MergeSketch(
                    approx_sketch_col,
                    SketchType::DDSketch
                ));
                final_stage(merge_sketch_col.sketch_percentile(&percentiles, *force_list_output));
            }
            AggExpr::ApproxCountDistinct(expr) => {
                let approx_sketch_col =
                    first_stage!(AggExpr::ApproxSketch(expr.clone(), SketchType::HyperLogLog));
                let merge_sketch_col = second_stage!(AggExpr::MergeSketch(
                    approx_sketch_col,
                    SketchType::HyperLogLog
                ));
                final_stage(merge_sketch_col);
            }
            AggExpr::Mean(expr) => {
                let sum_col = first_stage!(AggExpr::Sum(expr.clone()));
                let count_col = first_stage!(AggExpr::Count(expr.clone(), CountMode::Valid));

                let global_sum_col = second_stage!(AggExpr::Sum(sum_col));
                let global_count_col = second_stage!(AggExpr::Sum(count_col));

                final_stage(merge_mean(global_sum_col, global_count_col));
            }
            AggExpr::Stddev(expr, ddof) => {
                // The stddev calculation we're performing here is:
                // stddev(X, ddof) = sqrt((E(X^2) - E(X)^2) * n / (n - ddof))
                // where X is the sub_expr.
                //
                // First stage, we compute `sum(X^2)`, `sum(X)` and `count(X)`.
                // Second stage, we `global_sqsum := sum(sum(X^2))`, `global_sum := sum(sum(X))` and `global_count := sum(count(X))` in order to get the global versions of the first stage.
                // In the final projection, we then compute:
                // `sqrt(((global_sqsum / global_count) - (global_sum / global_count) ^ 2) * global_count / (global_count - ddof))`.

                // This is a workaround since we have different code paths for single stage and two stage aggregations.
                // Currently all Std Dev types will be computed using floats.
                let expr = expr.clone().cast(&DataType::Float64);

                let sum_col = first_stage!(AggExpr::Sum(expr.clone()));
                let sq_sum_col = first_stage!(AggExpr::Sum(expr.clone().mul(expr.clone())));
                let count_col = first_stage!(AggExpr::Count(expr, CountMode::Valid));

                let global_sum_col = second_stage!(AggExpr::Sum(sum_col));
                let global_sq_sum_col = second_stage!(AggExpr::Sum(sq_sum_col));
                let global_count_col = second_stage!(AggExpr::Sum(count_col));

                let n = global_count_col.clone().cast(&DataType::Float64);
                let sq_mean = global_sq_sum_col.div(n.clone());
                let mean = global_sum_col.clone().div(n.clone());
                let mean_sq = mean.clone().mul(mean);
                let pop_var = sq_mean.sub(mean_sq);

                let ddof_expr = lit(*ddof as f64);
                let adjusted = pop_var.mul(n.clone()).div(n.clone().sub(ddof_expr.clone()));
                let result = n
                    .clone()
                    .lt_eq(ddof_expr)
                    .if_else(null_lit(), sqrt::sqrt(adjusted));

                final_stage(result);
            }
            AggExpr::Var(expr, ddof) => {
                // The variance calculation we're performing here is:
                // var(X, ddof) = (E(X^2) - E(X)^2) * n / (n - ddof)
                // where X is the sub_expr.
                //
                // First stage, we compute `sum(X^2)`, `sum(X)` and `count(X)`.
                // Second stage, we get global versions: `global_sqsum`, `global_sum`, `global_count`.
                // In the final projection, we compute:
                // ((global_sqsum / global_count) - (global_sum / global_count) ^ 2) * global_count / (global_count - ddof)

                let expr = expr.clone().cast(&DataType::Float64);

                let sum_col = first_stage!(AggExpr::Sum(expr.clone()));
                let sq_sum_col = first_stage!(AggExpr::Sum(expr.clone().mul(expr.clone())));
                let count_col = first_stage!(AggExpr::Count(expr, CountMode::Valid));

                let global_sum_col = second_stage!(AggExpr::Sum(sum_col));
                let global_sq_sum_col = second_stage!(AggExpr::Sum(sq_sum_col));
                let global_count_col = second_stage!(AggExpr::Sum(count_col));

                // Population variance = (sqsum/n - (sum/n)^2)
                let n = global_count_col.clone().cast(&DataType::Float64);
                let sq_mean = global_sq_sum_col.div(n.clone());
                let mean = global_sum_col.clone().div(n.clone());
                let mean_sq = mean.clone().mul(mean);
                let pop_var = sq_mean.sub(mean_sq);

                // Adjust for ddof: sample_var = pop_var * n / (n - ddof)
                let ddof_expr = lit(*ddof as f64);
                let adjusted = pop_var.mul(n.clone()).div(n.clone().sub(ddof_expr.clone()));
                let result = n.clone().lt_eq(ddof_expr).if_else(null_lit(), adjusted);

                final_stage(result);
            }
            AggExpr::Min(expr) => {
                let min_col = first_stage!(AggExpr::Min(expr.clone()));
                let global_min_col = second_stage!(AggExpr::Min(min_col));
                final_stage(global_min_col);
            }
            AggExpr::Max(expr) => {
                let max_col = first_stage!(AggExpr::Max(expr.clone()));
                let global_max_col = second_stage!(AggExpr::Max(max_col));
                final_stage(global_max_col);
            }
            AggExpr::BoolAnd(expr) => {
                let bool_and_col = first_stage!(AggExpr::BoolAnd(expr.clone()));
                let global_bool_and_col = second_stage!(AggExpr::BoolAnd(bool_and_col.clone()));
                final_stage(global_bool_and_col);
            }
            AggExpr::BoolOr(expr) => {
                let bool_or_col = first_stage!(AggExpr::BoolOr(expr.clone()));
                let global_bool_or_col = second_stage!(AggExpr::BoolOr(bool_or_col.clone()));
                final_stage(global_bool_or_col);
            }
            AggExpr::AnyValue(expr, ignore_nulls) => {
                let any_col = first_stage!(AggExpr::AnyValue(expr.clone(), *ignore_nulls));
                let global_any_col = second_stage!(AggExpr::AnyValue(any_col, *ignore_nulls));
                final_stage(global_any_col);
            }
            AggExpr::List(expr) => {
                let list_col = first_stage!(AggExpr::List(expr.clone()));
                let concat_col = second_stage!(AggExpr::Concat(list_col, None));
                final_stage(concat_col);
            }
            AggExpr::Set(expr) => {
                let set_col = first_stage!(AggExpr::Set(expr.clone()));
                let concat_col = second_stage!(AggExpr::Concat(set_col, None));
                final_stage(distinct(concat_col));
            }
            AggExpr::Concat(expr, delimiter) => {
                let concat_col = first_stage!(AggExpr::Concat(expr.clone(), delimiter.clone()));
                let global_concat_col =
                    second_stage!(AggExpr::Concat(concat_col, delimiter.clone()));
                final_stage(global_concat_col);
            }
            AggExpr::Skew(expr) => {
                // See https://github.com/duckdb/duckdb/blob/93fda3591f4298414fa362c59219c09e03f718ab/extension/core_functions/aggregate/distributive/skew.cpp#L16
                // Not exactly the same since they normalize by N - 1
                let expr = expr.clone().cast(&DataType::Float64);

                // Global count, sum, squared_sum, and cubed_sum are required for final expr

                // First stage: count, sum, squared_sum, cubed_sum
                let count_col = first_stage!(AggExpr::Count(expr.clone(), CountMode::Valid));
                let sum_col = first_stage!(AggExpr::Sum(expr.clone()));
                let squared_sum_col = first_stage!(AggExpr::Sum(expr.clone().mul(expr.clone())));
                let cubed_sum_col =
                    first_stage!(AggExpr::Sum(expr.clone().mul(expr.clone()).mul(expr)));

                // Second stage: sum(count), sum(sum), sum(squared_sum), sum(cubed_sum)
                let global_count_col = second_stage!(AggExpr::Sum(count_col));
                let global_sum_col = second_stage!(AggExpr::Sum(sum_col));
                let global_squared_sum_col = second_stage!(AggExpr::Sum(squared_sum_col));
                let global_cubed_sum_col = second_stage!(AggExpr::Sum(cubed_sum_col));

                // Final projection: Given
                // - sum(count) = N
                // - sum(sum) = S
                // - sum(squared_sum) = S2
                // - sum(cubed_sum) = S3
                //         S3 - 3 * S2 * S / N + 2 * S^3 / N^2
                // Skew = -------------------------------------
                //          N * ((S2 - S^2 / N) / N) ^ (3/2)

                let n = global_count_col;
                let s = global_sum_col;
                let s2 = global_squared_sum_col;
                let s3 = global_cubed_sum_col;

                let denom_base = s2
                    .clone()
                    .sub(s.clone().mul(s.clone()).div(n.clone()))
                    .div(n.clone());
                let denom = sqrt::sqrt(denom_base.clone().mul(denom_base.clone()).mul(denom_base));

                let numerator = s3.sub(lit(3).mul(s2).mul(s.clone()).div(n.clone())).add(
                    lit(2)
                        .mul(s.clone())
                        .mul(s.clone())
                        .mul(s)
                        .div(n.clone())
                        .div(n.clone()),
                );

                let result = numerator.div(denom).div(n);
                final_stage(result);
            }
            AggExpr::MapGroups { .. } => {
                // MapGroups UDFs cannot be decomposed into partial / final stages.
                // We rely on evaluating the original MapGroups aggregation in a single
                // pass during grouped aggregation, so we intentionally do not add
                // any MapGroups expressions to the intermediate aggregation stages
                // or to the final projection list here. The grouped aggregate sinks
                // will call `agg()` with the original MapGroups expression when
                // `partial_agg_exprs` is empty.
            }
            // Only necessary for Flotilla
            AggExpr::ApproxSketch(expr, sketch_type) => {
                let approx_sketch_col =
                    first_stage!(AggExpr::ApproxSketch(expr.clone(), *sketch_type));
                let merged_sketch_col =
                    second_stage!(AggExpr::MergeSketch(approx_sketch_col, *sketch_type));
                final_stage(merged_sketch_col);
            }
            AggExpr::MergeSketch(expr, sketch_type) => {
                // Merging is commutative and associative, so just keep doing it
                let merge_sketch_col =
                    first_stage!(AggExpr::MergeSketch(expr.clone(), *sketch_type));
                let merged_sketch_col =
                    second_stage!(AggExpr::MergeSketch(merge_sketch_col, *sketch_type));
                final_stage(merged_sketch_col);
            }
        }
    }

    Ok((
        (first_stage_aggs.into_iter().collect(), first_stage_schema),
        (second_stage_aggs.into_iter().collect(), second_stage_schema),
        final_exprs,
    ))
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;
    use daft_core::prelude::{DataType, Field, Schema};
    use daft_dsl::{
        Expr,
        expr::bound_expr::{BoundAggExpr, BoundExpr},
        lit, resolved_col,
    };

    use super::populate_aggregation_stages_bound_with_schema;

    fn bound_sum(expr: daft_dsl::ExprRef, schema: &Schema) -> DaftResult<BoundAggExpr> {
        let expr = expr.sum();
        let Expr::Agg(agg) = expr.as_ref() else {
            panic!("expected aggregate expression");
        };
        BoundAggExpr::try_new(agg.clone(), schema)
    }

    #[test]
    fn rewrites_integer_sum_with_literals_into_shared_sum_and_count() -> DaftResult<()> {
        let schema = Schema::new(vec![Field::new("x", DataType::Int16)]);
        let aggregations = vec![
            bound_sum(resolved_col("x").add(lit(1i64)), &schema)?,
            bound_sum(resolved_col("x").add(lit(2i64)), &schema)?,
        ];

        let ((first_stage_aggs, _), (second_stage_aggs, _), final_exprs) =
            populate_aggregation_stages_bound_with_schema(&aggregations, &schema, &[])?;

        assert_eq!(
            first_stage_aggs
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec!["sum(col(0: x))", "count(col(0: x), Valid)"]
        );
        assert_eq!(
            second_stage_aggs
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec!["sum(col(0: x))", "sum(col(1: x))"]
        );
        assert_eq!(
            final_exprs
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec![
                "cast(col(0: x) as Int64) + [cast(lit(1) as Int64) * cast(col(1: x) as Int64)] as x",
                "cast(col(0: x) as Int64) + [cast(lit(2) as Int64) * cast(col(1: x) as Int64)] as x",
            ]
        );

        Ok(())
    }

    #[test]
    fn rewrites_grouped_integer_sum_minus_literal() -> DaftResult<()> {
        let schema = Schema::new(vec![
            Field::new("g", DataType::Utf8),
            Field::new("x", DataType::Int16),
        ]);
        let aggregations = vec![bound_sum(resolved_col("x").sub(lit(3i64)), &schema)?];
        let group_by = vec![BoundExpr::try_new(resolved_col("g"), &schema)?];

        let ((first_stage_aggs, _), (second_stage_aggs, _), final_exprs) =
            populate_aggregation_stages_bound_with_schema(&aggregations, &schema, &group_by)?;

        assert_eq!(
            first_stage_aggs
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec!["sum(col(1: x))", "count(col(1: x), Valid)"]
        );
        assert_eq!(
            second_stage_aggs
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec!["sum(col(1: x))", "sum(col(2: x))"]
        );
        assert_eq!(
            final_exprs
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec![
                "col(0: g)",
                "cast(col(1: x) as Int64) - [cast(lit(3) as Int64) * cast(col(2: x) as Int64)] as x",
            ]
        );

        Ok(())
    }

    #[test]
    fn rewrites_integer_sum_literal_minus_input() -> DaftResult<()> {
        let schema = Schema::new(vec![Field::new("x", DataType::Int16)]);
        let aggregations = vec![bound_sum(lit(3i64).sub(resolved_col("x")), &schema)?];

        let ((first_stage_aggs, _), (second_stage_aggs, _), final_exprs) =
            populate_aggregation_stages_bound_with_schema(&aggregations, &schema, &[])?;

        assert_eq!(
            first_stage_aggs
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec!["sum(col(0: x))", "count(col(0: x), Valid)"]
        );
        assert_eq!(
            second_stage_aggs
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec!["sum(col(0: x))", "sum(col(1: x))"]
        );
        assert_eq!(
            final_exprs
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec![
                "[cast(lit(3) as Int64) * cast(col(1: x) as Int64)] - cast(col(0: x) as Int64) as literal",
            ]
        );

        Ok(())
    }

    #[test]
    fn leaves_float_sum_with_literal_on_original_path() -> DaftResult<()> {
        let schema = Schema::new(vec![Field::new("x", DataType::Float64)]);
        let aggregations = vec![bound_sum(resolved_col("x").add(lit(1.5)), &schema)?];

        let ((first_stage_aggs, _), (second_stage_aggs, _), final_exprs) =
            populate_aggregation_stages_bound_with_schema(&aggregations, &schema, &[])?;

        assert_eq!(
            first_stage_aggs
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec!["sum(col(0: x) + lit(1.5))"]
        );
        assert_eq!(
            second_stage_aggs
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec!["sum(col(0: x))"]
        );
        assert_eq!(
            final_exprs
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec!["col(0: x) as x"]
        );

        Ok(())
    }
}
