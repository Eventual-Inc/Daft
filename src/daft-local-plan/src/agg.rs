use common_error::DaftResult;
use daft_core::prelude::{CountMode, DataType, Schema};
use daft_dsl::{
    AggExpr, ApproxPercentileParams, ExprRef, SketchType, bound_col,
    expr::bound_expr::{BoundAggExpr, BoundExpr},
    functions::agg::merge_mean,
    lit,
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
        let output_name = output_field.name.as_str();

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
                let concat_col = second_stage!(AggExpr::Concat(set_agg_col));
                final_stage(count_distinct(concat_col));
            }
            AggExpr::Sum(expr) => {
                let sum_col = first_stage!(AggExpr::Sum(expr.clone()));
                let global_sum_col = second_stage!(AggExpr::Sum(sum_col));
                final_stage(global_sum_col);
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
            AggExpr::Stddev(expr) => {
                // The stddev calculation we're performing here is:
                // stddev(X) = sqrt(E(X^2) - E(X)^2)
                // where X is the sub_expr.
                //
                // First stage, we compute `sum(X^2)`, `sum(X)` and `count(X)`.
                // Second stage, we `global_sqsum := sum(sum(X^2))`, `global_sum := sum(sum(X))` and `global_count := sum(count(X))` in order to get the global versions of the first stage.
                // In the final projection, we then compute `sqrt((global_sqsum / global_count) - (global_sum / global_count) ^ 2)`.

                // This is a workaround since we have different code paths for single stage and two stage aggregations.
                // Currently all Std Dev types will be computed using floats.
                let expr = expr.clone().cast(&DataType::Float64);

                let sum_col = first_stage!(AggExpr::Sum(expr.clone()));
                let sq_sum_col = first_stage!(AggExpr::Sum(expr.clone().mul(expr.clone())));
                let count_col = first_stage!(AggExpr::Count(expr, CountMode::Valid));

                let global_sum_col = second_stage!(AggExpr::Sum(sum_col));
                let global_sq_sum_col = second_stage!(AggExpr::Sum(sq_sum_col));
                let global_count_col = second_stage!(AggExpr::Sum(count_col));

                let left = global_sq_sum_col.div(global_count_col.clone());
                let right = global_sum_col.div(global_count_col);
                let right = right.clone().mul(right);
                let result = sqrt::sqrt(left.sub(right));

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
                let concat_col = second_stage!(AggExpr::Concat(list_col));
                final_stage(concat_col);
            }
            AggExpr::Set(expr) => {
                let set_col = first_stage!(AggExpr::Set(expr.clone()));
                let concat_col = second_stage!(AggExpr::Concat(set_col));
                final_stage(distinct(concat_col));
            }
            AggExpr::Concat(expr) => {
                let concat_col = first_stage!(AggExpr::Concat(expr.clone()));
                let global_concat_col = second_stage!(AggExpr::Concat(concat_col));
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
            AggExpr::MapGroups { func, inputs } => {
                // No first stage aggregation for MapGroups, do all the work in the second stage.
                let map_groups_col = second_stage!(AggExpr::MapGroups {
                    func: func.clone(),
                    inputs: inputs.clone()
                });
                final_stage(map_groups_col);
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
