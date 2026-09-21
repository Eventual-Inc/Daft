use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_core::lit::Literal;
use daft_dsl::{Expr, ExprRef, functions::scalar::ScalarFn, lit};

use super::OptimizerRule;
use crate::{LogicalPlan, ops::Filter};

/// Optimization rule that rewrites `starts_with(col, literal_prefix)` predicates
/// inside Filter nodes into the equivalent half-open lexicographic range
/// `col >= prefix AND col < increment(prefix)`.
///
/// The rewrite runs before `PushDownFilter`, so the rewritten predicate is made
/// of plain comparisons and conjunctions. That has two benefits over keeping
/// the `starts_with` scalar function:
///
/// 1. `daft-scan`'s expression rewriter classifies any `ScalarFn` as a UDF and
///    routes it to a residual Filter op, so a lone `starts_with` filter never
///    reaches `pushdowns.filters`. Comparisons do, which lets every scan source
///    (Parquet, CSV, Lance, Iceberg, ...) push the predicate down natively.
/// 2. Once pushed down, the existing Utf8 min/max statistics pruning in
///    `daft-stats` (`TableStatistics::eval_expression` handles `>=`/`<`/`AND`)
///    skips row groups and scan tasks whose range cannot contain the prefix,
///    mirroring how other engines (Spark's `StringStartsWith`, Lance's
///    `SargableQuery::LikePrefix`) turn prefix predicates into range bounds.
///
/// Only Filter predicates are rewritten: elsewhere (e.g. a projection) the
/// `starts_with` kernel stays a single, cheaper call and no pushdown applies.
///
/// The rewrite is semantics-preserving, including for nulls: `starts_with`
/// returns null on a null input, and so do the comparison operators. When the
/// pattern is not a non-empty Utf8 literal, or the prefix has no lexicographic
/// successor (every scalar is at its maximum), we either leave the expression
/// untouched or fall back to the lower-bound-only rewrite, which stays sound.
#[derive(Default, Debug)]
pub struct RewriteStartsWith {}

impl RewriteStartsWith {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for RewriteStartsWith {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform(|node| {
            if let LogicalPlan::Filter(filter) = node.as_ref() {
                let rewritten = filter
                    .predicate
                    .clone()
                    .transform_up(rewrite_starts_with_expr)?;
                if rewritten.transformed {
                    let new_filter = Filter {
                        predicate: rewritten.data,
                        ..filter.clone()
                    };
                    return Ok(Transformed::yes(Arc::new(LogicalPlan::Filter(new_filter))));
                }
            }
            Ok(Transformed::no(node))
        })
    }
}

fn rewrite_starts_with_expr(expr: ExprRef) -> DaftResult<Transformed<ExprRef>> {
    let Expr::ScalarFn(ScalarFn::Builtin(func)) = expr.as_ref() else {
        return Ok(Transformed::no(expr));
    };
    if func.name() != "starts_with" {
        return Ok(Transformed::no(expr));
    }

    // Bind by name, not position: both arguments are named (`input`, `pattern`)
    // and callers may pass them as keyword args in either order, so positional
    // indexing would be unsound. This mirrors the binding used by the
    // `starts_with` kernel itself. If either argument is absent the expression
    // is malformed; leave it alone and let normal validation report the error.
    let (Ok(input), Ok(pattern)) = (
        func.inputs.required((0, "input")),
        func.inputs.required((1, "pattern")),
    ) else {
        return Ok(Transformed::no(expr));
    };

    // The prefix must be a non-empty Utf8 literal; anything else (column,
    // non-string literal, empty prefix) yields no useful bound, so leave the
    // call untouched.
    let Expr::Literal(Literal::Utf8(prefix)) = pattern.as_ref() else {
        return Ok(Transformed::no(expr));
    };
    if prefix.is_empty() {
        return Ok(Transformed::no(expr));
    }

    let input = (*input).clone();
    let lower_bound = input.clone().gt_eq(lit(prefix.clone()));
    Ok(Transformed::yes(match increment_utf8_prefix(prefix) {
        Some(upper) => lower_bound.and(input.lt(lit(upper))),
        // The prefix is all max-value characters: no valid exclusive upper
        // bound exists, so fall back to the lower-bound-only rewrite. This is
        // still sound (it just prunes less).
        None => lower_bound,
    }))
}

/// Compute the lexicographically-next string prefix, used as the exclusive upper
/// bound when rewriting `starts_with(col, prefix)`:
/// `prefix <= col < increment_utf8_prefix(prefix)`.
///
/// It increments the right-most Unicode scalar value that can be incremented
/// (walking right to left), dropping any trailing characters already at their
/// maximum. Returns `None` when every character is at the maximum scalar value,
/// in which case the caller should fall back to a lower-bound-only range.
///
/// # UTF-8 / Unicode ordering
///
/// This operates on Unicode scalar values (chars), not bytes. Because UTF-8 byte
/// ordering matches Unicode code point ordering, incrementing a char's code point
/// produces the correct lexicographic successor for byte-wise string comparison.
///
/// Examples:
/// - `"foo"`  -> `Some("fop")`
/// - `"café"` -> `Some("cafê")`  (é U+00E9 -> ê U+00EA)
fn increment_utf8_prefix(prefix: &str) -> Option<String> {
    let chars: Vec<char> = prefix.chars().collect();
    for i in (0..chars.len()).rev() {
        if let Some(next) = next_unicode_scalar(chars[i]) {
            let mut result: String = chars[..i].iter().collect();
            result.push(next);
            return Some(result);
        }
        // chars[i] is at the maximum scalar value; drop it and carry left.
    }
    // Every character was at the maximum scalar value.
    None
}

/// Return the next valid Unicode scalar value after `c`, skipping the surrogate
/// range (U+D800..=U+DFFF) which is not valid in UTF-8. Returns `None` when `c`
/// is already the maximum scalar value (U+10FFFF).
fn next_unicode_scalar(c: char) -> Option<char> {
    let next = (c as u32).checked_add(1)?;
    // Skip the surrogate range, which does not contain valid `char`s.
    let next = if (0xD800..=0xDFFF).contains(&next) {
        0xE000
    } else {
        next
    };
    char::from_u32(next)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_core::prelude::*;
    use daft_dsl::{lit, resolved_col};
    use daft_functions_utf8::startswith;

    use super::{RewriteStartsWith, increment_utf8_prefix, next_unicode_scalar};
    use crate::{
        LogicalPlan,
        optimization::{
            optimizer::{RuleBatch, RuleExecutionStrategy},
            test::assert_optimized_plan_with_rules_eq,
        },
        test::{dummy_scan_node, dummy_scan_operator},
    };

    fn assert_optimized_plan_eq(
        plan: Arc<LogicalPlan>,
        expected: Arc<LogicalPlan>,
    ) -> common_error::DaftResult<()> {
        assert_optimized_plan_with_rules_eq(
            plan,
            expected,
            vec![RuleBatch::new(
                vec![Box::new(RewriteStartsWith::new())],
                RuleExecutionStrategy::Once,
            )],
        )
    }

    fn source_with_utf8_col() -> crate::LogicalPlanBuilder {
        dummy_scan_node(dummy_scan_operator(vec![Field::new("s", DataType::Utf8)]))
    }

    // Build the plan and the expected plan from the *same* scan node instance:
    // scan operator equality is by Arc identity, so two separately constructed
    // dummy operators never compare equal.
    fn optimize_and_expect(
        scan: &crate::LogicalPlanBuilder,
        predicate: daft_dsl::ExprRef,
        expected_predicate: daft_dsl::ExprRef,
    ) -> common_error::DaftResult<()> {
        let plan = scan.clone().filter(predicate)?.build();
        let expected = scan.clone().filter(expected_predicate)?.build();
        assert_optimized_plan_eq(plan, expected)
    }

    #[test]
    fn rewrites_starts_with_to_range() -> common_error::DaftResult<()> {
        let scan = source_with_utf8_col();
        optimize_and_expect(
            &scan,
            startswith(resolved_col("s"), lit("abc")),
            resolved_col("s")
                .gt_eq(lit("abc"))
                .and(resolved_col("s").lt(lit("abd"))),
        )
    }

    #[test]
    fn rewrites_inside_conjunctions_and_disjunctions() -> common_error::DaftResult<()> {
        let scan = source_with_utf8_col();
        optimize_and_expect(
            &scan,
            startswith(resolved_col("s"), lit("a"))
                .and(resolved_col("s").gt(lit("b")))
                .or(startswith(resolved_col("s"), lit("zz"))),
            resolved_col("s")
                .gt_eq(lit("a"))
                .and(resolved_col("s").lt(lit("b")))
                .and(resolved_col("s").gt(lit("b")))
                .or(resolved_col("s")
                    .gt_eq(lit("zz"))
                    .and(resolved_col("s").lt(lit("z{")))),
        )
    }

    #[test]
    fn keeps_empty_prefix_untouched() -> common_error::DaftResult<()> {
        let scan = source_with_utf8_col();
        let predicate = startswith(resolved_col("s"), lit(""));
        optimize_and_expect(&scan, predicate.clone(), predicate)
    }

    #[test]
    fn keeps_non_literal_pattern_untouched() -> common_error::DaftResult<()> {
        let scan = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("s", DataType::Utf8),
            Field::new("other", DataType::Utf8),
        ]));
        let predicate = startswith(resolved_col("s"), resolved_col("other"));
        optimize_and_expect(&scan, predicate.clone(), predicate)
    }

    #[test]
    fn all_max_prefix_falls_back_to_lower_bound_only() -> common_error::DaftResult<()> {
        // U+10FFFF is the maximum Unicode scalar value; when *every* character
        // is at the maximum there is no successor, so only the lower bound is
        // emitted.
        let max = char::from_u32(0x10FFFF).unwrap();
        let prefix = format!("{max}{max}");
        let scan = source_with_utf8_col();
        optimize_and_expect(
            &scan,
            startswith(resolved_col("s"), lit(prefix.clone())),
            resolved_col("s").gt_eq(lit(prefix)),
        )
    }

    #[test]
    fn increment_carries_past_trailing_max_chars() -> common_error::DaftResult<()> {
        // Trailing max-value characters are dropped and the carry lands on the
        // last incrementable char: "a\u{10FFFF}\u{10FFFF}" -> "b".
        let max = char::from_u32(0x10FFFF).unwrap();
        let prefix = format!("a{max}{max}");
        let scan = source_with_utf8_col();
        optimize_and_expect(
            &scan,
            startswith(resolved_col("s"), lit(prefix)),
            resolved_col("s")
                .gt_eq(lit(format!("a{max}{max}")))
                .and(resolved_col("s").lt(lit("b"))),
        )
    }

    #[test]
    fn rewrites_named_argument_binding_in_any_order() -> common_error::DaftResult<()> {
        // `starts_with(pattern => "ab", input => col)` must bind by name: the
        // column is the input and the literal is the prefix, regardless of the
        // order the keyword args are passed in.
        use std::sync::Arc;

        use daft_dsl::{
            Expr,
            functions::{FunctionArg, FunctionArgs, scalar::BuiltinScalarFnVariant},
        };
        let inputs = FunctionArgs::new_unchecked(vec![
            FunctionArg::named("pattern", lit("ab")),
            FunctionArg::named("input", resolved_col("s")),
        ]);
        let expr = Expr::ScalarFn(daft_dsl::functions::scalar::ScalarFn::Builtin(
            daft_dsl::functions::scalar::BuiltinScalarFn {
                func: BuiltinScalarFnVariant::Sync(Arc::new(daft_functions_utf8::StartsWith)),
                inputs,
            },
        ));
        let scan = source_with_utf8_col();
        optimize_and_expect(
            &scan,
            expr.into(),
            resolved_col("s")
                .gt_eq(lit("ab"))
                .and(resolved_col("s").lt(lit("ac"))),
        )
    }

    #[test]
    fn does_not_rewrite_outside_filter() -> common_error::DaftResult<()> {
        // In a projection the kernel call is cheaper than two comparisons plus
        // an AND, and no pushdown applies, so it must stay untouched.
        let scan = source_with_utf8_col();
        let projection = vec![startswith(resolved_col("s"), lit("abc"))];
        let plan = scan.select(projection.clone())?.build();
        let expected = scan.select(projection)?.build();
        assert_optimized_plan_eq(plan, expected)
    }

    #[test]
    fn increment_prefix_basic() {
        assert_eq!(increment_utf8_prefix("foo").as_deref(), Some("fop"));
        assert_eq!(increment_utf8_prefix("a").as_deref(), Some("b"));
        assert_eq!(increment_utf8_prefix("z").as_deref(), Some("{"));
        // é (U+00E9) -> ê (U+00EA)
        assert_eq!(increment_utf8_prefix("café").as_deref(), Some("cafê"));
    }

    #[test]
    fn increment_prefix_skips_surrogate_range() {
        // U+D7FF's successor is U+D800, which is a surrogate and not a valid
        // char; it must skip to U+E000.
        let c = char::from_u32(0xD7FF).unwrap();
        assert_eq!(next_unicode_scalar(c), char::from_u32(0xE000));
        // A prefix ending just below the surrogate range carries correctly.
        let prefix = format!("a{c}");
        let expected = format!("a{}", char::from_u32(0xE000).unwrap());
        assert_eq!(
            increment_utf8_prefix(&prefix).as_deref(),
            Some(expected.as_str())
        );
    }

    #[test]
    fn increment_prefix_all_max_returns_none() {
        let max = char::from_u32(0x10FFFF).unwrap();
        assert_eq!(increment_utf8_prefix(&format!("{max}{max}")), None);
        assert_eq!(next_unicode_scalar(max), None);
    }

    #[test]
    fn pushdown_integration_reaches_scan_filters() -> common_error::DaftResult<()> {
        // Run the full default optimizer: the rewritten comparisons must land
        // in the scan's pushdown filters (a lone `starts_with` ScalarFn would
        // be classified as a UDF and stranded in a residual Filter op).
        use common_treenode::{TreeNode, TreeNodeRecursion};

        use crate::{SourceInfo, ops::Source, optimization::optimizer::OptimizerBuilder};

        let plan = source_with_utf8_col()
            .filter(startswith(resolved_col("s"), lit("abc")))?
            .build();
        let optimizer = OptimizerBuilder::default()
            .with_default_optimizations()
            .build();
        let optimized = optimizer.optimize(plan, |_, _, _, _, _| {})?;

        let mut found_pushed_range = false;
        optimized.apply(
            |node| -> common_error::DaftResult<common_treenode::TreeNodeRecursion> {
                if let LogicalPlan::Source(Source { source_info, .. }) = node.as_ref()
                    && let SourceInfo::Physical(info) = source_info.as_ref()
                    && let Some(filters) = &info.pushdowns.filters
                {
                    let rendered = filters.to_string();
                    assert!(
                        !rendered.contains("starts_with"),
                        "starts_with should have been rewritten, got: {rendered}"
                    );
                    assert!(
                        rendered.contains("abc") && rendered.contains("abd"),
                        "expected range bounds in pushdowns, got: {rendered}"
                    );
                    found_pushed_range = true;
                }
                Ok(TreeNodeRecursion::Continue)
            },
        )?;
        assert!(
            found_pushed_range,
            "expected the predicate to be pushed into the scan"
        );
        Ok(())
    }
}
