/// Rust-level aggregation benchmark — bypasses query planning entirely.
///
/// Run with:
///   cargo test -p daft-recordbatch --release -- bench_agg --nocapture --ignored
///
/// Each test prints timing results to stderr.
#[cfg(test)]
mod bench {
    use std::time::Instant;

    use daft_core::{count_mode::CountMode, datatypes::*, prelude::*, series::IntoSeries};
    use daft_dsl::{
        AggExpr,
        expr::bound_expr::{BoundAggExpr, BoundExpr},
        resolved_col,
    };

    use crate::RecordBatch;

    const WARMUP: usize = 3;
    const ITERS: usize = 10;

    fn make_large_batch(
        num_rows: usize,
        num_groups: usize,
    ) -> (RecordBatch, Vec<BoundExpr>, Schema) {
        let keys: Vec<i64> = (0..num_rows).map(|i| (i % num_groups) as i64).collect();
        let vals: Vec<i64> = (0..num_rows).map(|i| (i % 1000) as i64).collect();

        let key_series = Int64Array::from_vec("key", keys).into_series();
        let val_series = Int64Array::from_vec("val", vals).into_series();

        let schema = Schema::new(vec![
            Field::new("key", DataType::Int64),
            Field::new("val", DataType::Int64),
        ]);
        let rb = RecordBatch::from_nonempty_columns(vec![key_series, val_series]).unwrap();
        let group_by = vec![BoundExpr::try_new(resolved_col("key"), &schema).unwrap()];
        (rb, group_by, schema)
    }

    fn bench_fn(_label: &str, warmup: usize, iters: usize, f: impl Fn()) -> f64 {
        for _ in 0..warmup {
            f();
        }
        let mut best = f64::INFINITY;
        for _ in 0..iters {
            let t0 = Instant::now();
            f();
            let elapsed = t0.elapsed().as_secs_f64() * 1000.0;
            best = best.min(elapsed);
        }
        best
    }

    fn run_bench(num_rows: usize, num_groups: usize) {
        let (rb, group_by, schema) = make_large_batch(num_rows, num_groups);

        let count_agg = vec![
            BoundAggExpr::try_new(AggExpr::Count(resolved_col("val"), CountMode::All), &schema)
                .unwrap(),
        ];
        let sum_agg =
            vec![BoundAggExpr::try_new(AggExpr::Sum(resolved_col("val")), &schema).unwrap()];
        let count_sum_agg = vec![
            BoundAggExpr::try_new(AggExpr::Count(resolved_col("val"), CountMode::All), &schema)
                .unwrap(),
            BoundAggExpr::try_new(AggExpr::Sum(resolved_col("val")), &schema).unwrap(),
        ];

        eprintln!("\n  rows={num_rows:>10}  groups={num_groups:>10}");
        eprintln!(
            "  {:>14} {:>14} {:>14} {:>10}",
            "agg", "inline (ms)", "fallback (ms)", "speedup"
        );
        eprintln!("  {}", "-".repeat(58));

        for (label, agg) in [
            ("count", &count_agg),
            ("sum", &sum_agg),
            ("count+sum", &count_sum_agg),
        ] {
            let inline_ms = bench_fn(label, WARMUP, ITERS, || {
                rb.agg_groupby_inline(agg, &group_by).unwrap();
            });
            let fallback_ms = bench_fn(label, WARMUP, ITERS, || {
                rb.agg_groupby_fallback(agg, &group_by).unwrap();
            });
            let speedup = fallback_ms / inline_ms;
            eprintln!("  {label:>14} {inline_ms:>14.2} {fallback_ms:>14.2} {speedup:>9.2}x");
        }
    }

    #[test]
    #[ignore] // Run explicitly with --ignored
    fn bench_agg_5m_rows() {
        eprintln!("\nRust-level aggregation benchmark (no query planning overhead)");
        eprintln!("warmup={WARMUP} iters={ITERS}");

        for &num_groups in &[10, 1_000, 100_000, 5_000_000] {
            run_bench(5_000_000, num_groups);
        }
    }

    /// Q1-like benchmark: 2 string groupby columns, float64 sum + count aggs.
    /// This exercises the generic hash path (multi-column, non-integer keys).
    fn make_q1_like_batch(num_rows: usize) -> (RecordBatch, Vec<BoundExpr>, Vec<BoundAggExpr>) {
        use std::sync::Arc;

        // 2 low-cardinality string columns → ~6 groups
        let flags: Vec<&str> = (0..num_rows)
            .map(|i| match i % 3 {
                0 => "A",
                1 => "N",
                _ => "R",
            })
            .collect();
        let statuses: Vec<&str> = (0..num_rows)
            .map(|i| match i % 2 {
                0 => "F",
                _ => "O",
            })
            .collect();
        let quantities: Vec<f64> = (0..num_rows).map(|i| (i % 50) as f64 + 1.0).collect();
        let prices: Vec<f64> = (0..num_rows).map(|i| (i % 10000) as f64 + 100.0).collect();

        let flag_series = Series::from_arrow(
            Arc::new(Field::new("flag", DataType::Utf8)),
            Arc::new(arrow::array::LargeStringArray::from(flags)),
        )
        .unwrap();
        let status_series = Series::from_arrow(
            Arc::new(Field::new("status", DataType::Utf8)),
            Arc::new(arrow::array::LargeStringArray::from(statuses)),
        )
        .unwrap();
        let qty_series = Float64Array::from_vec("qty", quantities).into_series();
        let price_series = Float64Array::from_vec("price", prices).into_series();

        let schema = Schema::new(vec![
            Field::new("flag", DataType::Utf8),
            Field::new("status", DataType::Utf8),
            Field::new("qty", DataType::Float64),
            Field::new("price", DataType::Float64),
        ]);
        let rb = RecordBatch::from_nonempty_columns(vec![
            flag_series,
            status_series,
            qty_series,
            price_series,
        ])
        .unwrap();

        let group_by = vec![
            BoundExpr::try_new(resolved_col("flag"), &schema).unwrap(),
            BoundExpr::try_new(resolved_col("status"), &schema).unwrap(),
        ];

        // Mimic Q1's decomposed aggs: sum(qty), sum(price), count(qty), count(price)
        let aggs = vec![
            BoundAggExpr::try_new(AggExpr::Sum(resolved_col("qty")), &schema).unwrap(),
            BoundAggExpr::try_new(AggExpr::Sum(resolved_col("price")), &schema).unwrap(),
            BoundAggExpr::try_new(
                AggExpr::Count(resolved_col("qty"), CountMode::Valid),
                &schema,
            )
            .unwrap(),
            BoundAggExpr::try_new(
                AggExpr::Count(resolved_col("price"), CountMode::Valid),
                &schema,
            )
            .unwrap(),
        ];

        (rb, group_by, aggs)
    }

    #[test]
    #[ignore]
    fn bench_q1_like() {
        eprintln!("\nQ1-like benchmark: 2 string keys, 6 groups, float64 sum+count");
        eprintln!("warmup={WARMUP} iters={ITERS}");

        for &num_rows in &[1_200_000, 5_000_000] {
            let (rb, group_by, aggs) = make_q1_like_batch(num_rows);
            eprintln!("\n  rows={num_rows:>10}  groups=6 (2 string cols)");

            let inline_ms = bench_fn("inline", WARMUP, ITERS, || {
                rb.agg_groupby_inline(&aggs, &group_by).unwrap();
            });
            let fallback_ms = bench_fn("fallback", WARMUP, ITERS, || {
                rb.agg_groupby_fallback(&aggs, &group_by).unwrap();
            });
            let speedup = fallback_ms / inline_ms;
            eprintln!(
                "  inline: {inline_ms:.2}ms  fallback: {fallback_ms:.2}ms  speedup: {speedup:.2}x"
            );
        }
    }
}
