//! Conservative extraction of MCAP reader constraints from Daft predicates.
//!
//! Extraction only tightens the [`McapReadOptions`] used for index-based I/O
//! pruning; the complete predicate is always re-applied to decoded batches,
//! so a skipped expression costs performance, never correctness. Only
//! top-level conjuncts of the forms `topic == <literal>`,
//! `topic IS IN (<literals>)`, and `log_time <cmp> <integer literal>` are
//! recognized.

use std::collections::BTreeSet;

use daft_algebra::boolean::split_conjunction;
use daft_core::{lit::Literal, prelude::Operator};
use daft_dsl::{Column, Expr, ExprRef, ResolvedColumn};

use crate::McapReadOptions;

const TOPIC_COLUMN: &str = "topic";
const LOG_TIME_COLUMN: &str = "log_time";

/// Tightens reader constraints with topic/log-time bounds recovered from the
/// scan predicate. Recovered bounds intersect any explicit constraints.
pub(crate) fn apply_predicate_pushdown(
    mut read_options: McapReadOptions,
    predicate: Option<&ExprRef>,
) -> McapReadOptions {
    let Some(predicate) = predicate else {
        return read_options;
    };
    for conjunct in split_conjunction(predicate) {
        match conjunct.as_ref() {
            Expr::BinaryOp { op, left, right } => {
                apply_comparison(&mut read_options, *op, left, right);
            }
            Expr::IsIn(expr, items) => {
                if column_name(expr) == Some(TOPIC_COLUMN)
                    && let Some(topics) = utf8_literals(items)
                {
                    intersect_topics(&mut read_options, topics);
                }
            }
            _ => {}
        }
    }
    read_options
}

fn apply_comparison(
    read_options: &mut McapReadOptions,
    op: Operator,
    left: &ExprRef,
    right: &ExprRef,
) {
    // Normalize to `column <op> literal`.
    let (column, op, literal) = if let Some(column) = column_name(left) {
        (column, op, right)
    } else if let Some(column) = column_name(right) {
        let Some(op) = flip_comparison(op) else {
            return;
        };
        (column, op, left)
    } else {
        return;
    };

    match column {
        TOPIC_COLUMN => {
            if op == Operator::Eq
                && let Expr::Literal(Literal::Utf8(topic)) = literal.as_ref()
            {
                intersect_topics(read_options, BTreeSet::from([topic.clone()]));
            }
        }
        LOG_TIME_COLUMN => {
            let Some(value) = u64_literal(literal) else {
                return;
            };
            // The reader's time range is half-open: [start_time, end_time).
            // Bounds that cannot be represented in u64 are skipped, which
            // simply leaves them to the residual predicate.
            match op {
                Operator::GtEq => raise_start(read_options, value),
                Operator::Gt => {
                    if let Some(bound) = value.checked_add(1) {
                        raise_start(read_options, bound);
                    }
                }
                Operator::Lt => lower_end(read_options, value),
                Operator::LtEq => {
                    if let Some(bound) = value.checked_add(1) {
                        lower_end(read_options, bound);
                    }
                }
                _ => {}
            }
        }
        _ => {}
    }
}

fn flip_comparison(op: Operator) -> Option<Operator> {
    match op {
        Operator::Eq => Some(Operator::Eq),
        Operator::Lt => Some(Operator::Gt),
        Operator::LtEq => Some(Operator::GtEq),
        Operator::Gt => Some(Operator::Lt),
        Operator::GtEq => Some(Operator::LtEq),
        _ => None,
    }
}

fn column_name(expr: &ExprRef) -> Option<&str> {
    match expr.as_ref() {
        Expr::Column(Column::Resolved(ResolvedColumn::Basic(name))) => Some(name.as_ref()),
        _ => None,
    }
}

fn utf8_literals(items: &[ExprRef]) -> Option<BTreeSet<String>> {
    items
        .iter()
        .map(|item| match item.as_ref() {
            Expr::Literal(Literal::Utf8(value)) => Some(value.clone()),
            _ => None,
        })
        .collect()
}

fn u64_literal(expr: &ExprRef) -> Option<u64> {
    let Expr::Literal(literal) = expr.as_ref() else {
        return None;
    };
    match *literal {
        Literal::UInt8(value) => Some(u64::from(value)),
        Literal::UInt16(value) => Some(u64::from(value)),
        Literal::UInt32(value) => Some(u64::from(value)),
        Literal::UInt64(value) => Some(value),
        Literal::Int8(value) => u64::try_from(value).ok(),
        Literal::Int16(value) => u64::try_from(value).ok(),
        Literal::Int32(value) => u64::try_from(value).ok(),
        Literal::Int64(value) => u64::try_from(value).ok(),
        _ => None,
    }
}

fn intersect_topics(read_options: &mut McapReadOptions, topics: BTreeSet<String>) {
    let merged = match read_options.topics.take() {
        Some(existing) => existing
            .into_iter()
            .filter(|topic| topics.contains(topic))
            .collect(),
        None => topics.into_iter().collect(),
    };
    read_options.topics = Some(merged);
}

fn raise_start(read_options: &mut McapReadOptions, bound: u64) {
    read_options.start_time = Some(
        read_options
            .start_time
            .map_or(bound, |start| start.max(bound)),
    );
}

fn lower_end(read_options: &mut McapReadOptions, bound: u64) {
    read_options.end_time = Some(read_options.end_time.map_or(bound, |end| end.min(bound)));
}

#[cfg(test)]
mod tests {
    use daft_dsl::{lit, resolved_col};

    use super::apply_predicate_pushdown;
    use crate::McapReadOptions;

    fn extract(predicate: daft_dsl::ExprRef) -> McapReadOptions {
        apply_predicate_pushdown(McapReadOptions::default(), Some(&predicate))
    }

    #[test]
    fn no_predicate_is_a_no_op() {
        let options = apply_predicate_pushdown(McapReadOptions::default(), None);
        assert_eq!(options.topics, None);
        assert_eq!(options.start_time, None);
        assert_eq!(options.end_time, None);
    }

    #[test]
    fn topic_equality_becomes_topic_set() {
        let options = extract(resolved_col("topic").eq(lit("/camera")));
        assert_eq!(options.topics, Some(vec!["/camera".to_string()]));
    }

    #[test]
    fn flipped_topic_equality_becomes_topic_set() {
        let options = extract(lit("/camera").eq(resolved_col("topic")));
        assert_eq!(options.topics, Some(vec!["/camera".to_string()]));
    }

    #[test]
    fn topic_is_in_becomes_topic_set() {
        let options = extract(resolved_col("topic").is_in(vec![lit("/camera"), lit("/imu")]));
        assert_eq!(
            options.topics,
            Some(vec!["/camera".to_string(), "/imu".to_string()])
        );
    }

    #[test]
    fn topic_constraints_intersect_explicit_topics() {
        let explicit = McapReadOptions {
            topics: Some(vec!["/camera".to_string(), "/imu".to_string()]),
            ..Default::default()
        };
        let predicate = resolved_col("topic").eq(lit("/imu"));
        let options = apply_predicate_pushdown(explicit, Some(&predicate));
        assert_eq!(options.topics, Some(vec!["/imu".to_string()]));
    }

    #[test]
    fn contradictory_topics_intersect_to_empty() {
        let predicate = resolved_col("topic")
            .eq(lit("/camera"))
            .and(resolved_col("topic").eq(lit("/imu")));
        let options = extract(predicate);
        assert_eq!(options.topics, Some(vec![]));
    }

    #[test]
    fn log_time_bounds_map_to_half_open_range() {
        let options = extract(
            resolved_col("log_time")
                .gt_eq(lit(4_u64))
                .and(resolved_col("log_time").lt(lit(10_u64))),
        );
        assert_eq!(options.start_time, Some(4));
        assert_eq!(options.end_time, Some(10));

        let options = extract(
            resolved_col("log_time")
                .gt(lit(4_u64))
                .and(resolved_col("log_time").lt_eq(lit(10_u64))),
        );
        assert_eq!(options.start_time, Some(5));
        assert_eq!(options.end_time, Some(11));
    }

    #[test]
    fn flipped_log_time_bounds_are_normalized() {
        let options = extract(lit(4_u64).lt_eq(resolved_col("log_time")));
        assert_eq!(options.start_time, Some(4));

        let options = extract(lit(10_u64).gt(resolved_col("log_time")));
        assert_eq!(options.end_time, Some(10));
    }

    #[test]
    fn log_time_bounds_tighten_explicit_range() {
        let explicit = McapReadOptions {
            start_time: Some(10),
            end_time: Some(100),
            ..Default::default()
        };
        let predicate = resolved_col("log_time")
            .gt_eq(lit(5_u64))
            .and(resolved_col("log_time").lt(lit(50_u64)));
        let options = apply_predicate_pushdown(explicit, Some(&predicate));
        assert_eq!(options.start_time, Some(10));
        assert_eq!(options.end_time, Some(50));
    }

    #[test]
    fn unrepresentable_bounds_are_skipped() {
        let options = extract(resolved_col("log_time").gt(lit(u64::MAX)));
        assert_eq!(options.start_time, None);

        let options = extract(resolved_col("log_time").lt_eq(lit(u64::MAX)));
        assert_eq!(options.end_time, None);

        let options = extract(resolved_col("log_time").gt_eq(lit(-5_i64)));
        assert_eq!(options.start_time, None);
    }

    #[test]
    fn disjunctions_are_not_extracted() {
        let predicate = resolved_col("topic")
            .eq(lit("/camera"))
            .or(resolved_col("topic").eq(lit("/imu")));
        let options = extract(predicate);
        assert_eq!(options.topics, None);
    }

    #[test]
    fn unrelated_expressions_are_skipped() {
        let options = extract(resolved_col("publish_time").gt_eq(lit(5_u64)));
        assert_eq!(options.start_time, None);

        let options = extract(resolved_col("log_time").gt_eq(resolved_col("publish_time")));
        assert_eq!(options.start_time, None);

        let options = extract(resolved_col("topic").eq(resolved_col("source_path")));
        assert_eq!(options.topics, None);
    }

    #[test]
    fn conjunction_combines_topic_and_time_constraints() {
        let predicate = resolved_col("topic")
            .eq(lit("/camera"))
            .and(resolved_col("log_time").gt_eq(lit(4_u64)))
            .and(resolved_col("log_time").lt(lit(10_u64)))
            .and(resolved_col("sequence").gt(lit(1_u32)));
        let options = extract(predicate);
        assert_eq!(options.topics, Some(vec!["/camera".to_string()]));
        assert_eq!(options.start_time, Some(4));
        assert_eq!(options.end_time, Some(10));
    }
}
