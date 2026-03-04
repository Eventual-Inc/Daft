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
}
