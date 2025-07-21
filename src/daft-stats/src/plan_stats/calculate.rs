use common_scan_info::{Pushdowns, ScanTaskLikeRef};
use daft_core::prelude::SchemaRef;
use daft_dsl::{estimated_selectivity, Expr};

use crate::plan_stats::{ApproxStats, PlanStats};

pub fn calculate_scan_stats(
    scan_tasks: &Vec<ScanTaskLikeRef>,
    pushdowns: &Pushdowns,
    output_schema: &SchemaRef,
) -> PlanStats {
    let mut approx_stats = ApproxStats::empty();
    for st in scan_tasks {
        if let Some(num_rows) = st.num_rows() {
            approx_stats.num_rows += num_rows;
        } else if let Some(approx_num_rows) = st.approx_num_rows(None) {
            approx_stats.num_rows += approx_num_rows as usize;
        }
        approx_stats.size_bytes += st.estimate_in_memory_size_bytes(None).unwrap_or(0);
    }
    approx_stats.acc_selectivity = pushdowns.estimated_selectivity(output_schema.as_ref());
    PlanStats::new(approx_stats)
}

pub fn calculate_filter_stats(
    input_stats: &PlanStats,
    predicate: &Expr,
    schema: &SchemaRef,
) -> PlanStats {
    let estimated_selectivity = estimated_selectivity(predicate, schema.as_ref());
    let approx_stats = ApproxStats {
        num_rows: (input_stats.approx_stats.num_rows as f64 * estimated_selectivity).ceil()
            as usize,
        size_bytes: (input_stats.approx_stats.size_bytes as f64 * estimated_selectivity).ceil()
            as usize,
        acc_selectivity: input_stats.approx_stats.acc_selectivity * estimated_selectivity,
    };
    PlanStats::new(approx_stats)
}

pub fn calculate_limit_stats(input_stats: &PlanStats, limit: u64) -> PlanStats {
    let limit = limit as usize;
    let limit_selectivity = if input_stats.approx_stats.num_rows > limit {
        if input_stats.approx_stats.num_rows == 0 {
            0.0
        } else {
            limit as f64 / input_stats.approx_stats.num_rows as f64
        }
    } else {
        1.0
    };
    let approx_stats = ApproxStats {
        num_rows: limit.min(input_stats.approx_stats.num_rows),
        size_bytes: if input_stats.approx_stats.num_rows > limit {
            let est_bytes_per_row =
                input_stats.approx_stats.size_bytes / input_stats.approx_stats.num_rows.max(1);
            limit * est_bytes_per_row
        } else {
            input_stats.approx_stats.size_bytes
        },
        acc_selectivity: input_stats.approx_stats.acc_selectivity * limit_selectivity,
    };
    PlanStats::new(approx_stats)
}

pub fn calculate_explode_stats(input_stats: &PlanStats) -> PlanStats {
    let est_num_exploded_rows = input_stats.approx_stats.num_rows * 4;
    let acc_selectivity = if input_stats.approx_stats.num_rows == 0 {
        0.0
    } else {
        input_stats.approx_stats.acc_selectivity * est_num_exploded_rows as f64
            / input_stats.approx_stats.num_rows as f64
    };
    let approx_stats = ApproxStats {
        num_rows: est_num_exploded_rows,
        size_bytes: input_stats.approx_stats.size_bytes,
        acc_selectivity,
    };
    PlanStats::new(approx_stats)
}

pub fn calculate_project_stats(input_stats: &PlanStats) -> PlanStats {
    // TODO(desmond): We can do better estimations with the projection schema. For now, reuse the old logic.
    input_stats.clone()
}

pub fn calculate_actor_pool_project_stats(input_stats: &PlanStats) -> PlanStats {
    // TODO(desmond): We can do better estimations here. For now, reuse the old logic.
    input_stats.clone()
}

pub fn calculate_ungrouped_aggregate_stats(input_stats: &PlanStats) -> PlanStats {
    let est_bytes_per_row =
        input_stats.approx_stats.size_bytes / (input_stats.approx_stats.num_rows.max(1));
    let acc_selectivity = if input_stats.approx_stats.num_rows == 0 {
        0.0
    } else {
        input_stats.approx_stats.acc_selectivity / input_stats.approx_stats.num_rows as f64
    };
    let approx_stats = ApproxStats {
        num_rows: 1,
        size_bytes: est_bytes_per_row,
        acc_selectivity,
    };
    PlanStats::new(approx_stats)
}

pub fn calculate_hash_aggregate_stats(input_stats: &PlanStats, group_by: &[&Expr]) -> PlanStats {
    let est_bytes_per_row =
        input_stats.approx_stats.size_bytes / (input_stats.approx_stats.num_rows.max(1));
    let acc_selectivity = if input_stats.approx_stats.num_rows == 0 {
        0.0
    } else {
        input_stats.approx_stats.acc_selectivity / input_stats.approx_stats.num_rows as f64
    };

    let approx_stats = if group_by.is_empty() {
        ApproxStats {
            num_rows: 1,
            size_bytes: est_bytes_per_row,
            acc_selectivity,
        }
    } else {
        // Assume high cardinality for group by columns, and 80% of rows are unique.
        let est_num_groups = input_stats.approx_stats.num_rows * 4 / 5;
        ApproxStats {
            num_rows: est_num_groups,
            size_bytes: est_bytes_per_row * est_num_groups,
            acc_selectivity: input_stats.approx_stats.acc_selectivity * est_num_groups as f64
                / input_stats.approx_stats.num_rows as f64,
        }
    };
    PlanStats::new(approx_stats)
}

pub fn calculate_dedup_stats(input_stats: &PlanStats) -> PlanStats {
    // TODO(desmond): We can do better estimations here. For now, reuse the old logic.
    input_stats.clone()
}

pub fn calculate_distinct_stats(input_stats: &PlanStats) -> PlanStats {
    // TODO(desmond): We can simply use NDVs here. For now, do a naive estimation.
    let est_bytes_per_row =
        input_stats.approx_stats.size_bytes / (input_stats.approx_stats.num_rows.max(1));
    // Assume high cardinality, 80% of rows are distinct.
    let est_distinct_values = input_stats.approx_stats.num_rows * 4 / 5;
    let acc_selectivity = if input_stats.approx_stats.num_rows == 0 {
        0.0
    } else {
        input_stats.approx_stats.acc_selectivity * est_distinct_values as f64
            / input_stats.approx_stats.num_rows as f64
    };
    let approx_stats = ApproxStats {
        num_rows: est_distinct_values,
        size_bytes: est_distinct_values * est_bytes_per_row,
        acc_selectivity,
    };
    PlanStats::new(approx_stats)
}

pub fn calculate_pivot_stats(input_stats: &PlanStats) -> PlanStats {
    // TODO(desmond): We can do better estimations here. For now, reuse the old logic.
    input_stats.clone()
}

pub fn calculate_sort_stats(input_stats: &PlanStats) -> PlanStats {
    // Sorting does not affect cardinality
    input_stats.clone()
}

pub fn calculate_top_n_stats(input_stats: &PlanStats, limit: u64) -> PlanStats {
    let limit = limit as usize;
    let limit_selectivity = if input_stats.approx_stats.num_rows > limit {
        if input_stats.approx_stats.num_rows == 0 {
            0.0
        } else {
            limit as f64 / input_stats.approx_stats.num_rows as f64
        }
    } else {
        1.0
    };

    let approx_stats = ApproxStats {
        num_rows: limit.min(input_stats.approx_stats.num_rows),
        size_bytes: if input_stats.approx_stats.num_rows > limit {
            let est_bytes_per_row =
                input_stats.approx_stats.size_bytes / input_stats.approx_stats.num_rows.max(1);
            limit * est_bytes_per_row
        } else {
            input_stats.approx_stats.size_bytes
        },
        acc_selectivity: input_stats.approx_stats.acc_selectivity * limit_selectivity,
    };
    PlanStats::new(approx_stats)
}

pub fn calculate_sample_stats(input_stats: &PlanStats, fraction: f64) -> PlanStats {
    let approx_stats = input_stats
        .approx_stats
        .apply(|v| ((v as f64) * fraction) as usize);
    PlanStats::new(approx_stats)
}

pub fn calculate_monotonically_increasing_id_stats(input_stats: &PlanStats) -> PlanStats {
    // TODO(desmond): We can do better estimations with the projection schema. For now, reuse the old logic.
    input_stats.clone()
}

pub fn calculate_hash_join_stats(left_stats: &PlanStats, right_stats: &PlanStats) -> PlanStats {
    // Assume a Primary-key + Foreign-Key join which would yield the max of the two tables.
    // TODO(desmond): We can do better estimations here. For now, use the old logic.
    // We assume that if one side of a join had its cardinality reduced by some operations
    // (e.g. filters, limits, aggregations), then assuming a pk-fk join, the total number of
    // rows output from the join will be reduced proportionally. Hence, apply the right side's
    // selectivity to the number of rows/size in bytes on the left and vice versa.
    let left_num_rows =
        left_stats.approx_stats.num_rows as f64 * right_stats.approx_stats.acc_selectivity;
    let right_num_rows =
        right_stats.approx_stats.num_rows as f64 * left_stats.approx_stats.acc_selectivity;
    let left_size =
        left_stats.approx_stats.size_bytes as f64 * right_stats.approx_stats.acc_selectivity;
    let right_size =
        right_stats.approx_stats.size_bytes as f64 * left_stats.approx_stats.acc_selectivity;
    let approx_stats = ApproxStats {
        num_rows: left_num_rows.max(right_num_rows).ceil() as usize,
        size_bytes: left_size.max(right_size).ceil() as usize,
        acc_selectivity: left_stats.approx_stats.acc_selectivity
            * right_stats.approx_stats.acc_selectivity,
    };
    PlanStats::new(approx_stats)
}

pub fn calculate_cross_join_stats(left_stats: &PlanStats, right_stats: &PlanStats) -> PlanStats {
    // Cross join multiplies the cardinalities
    let approx_stats = ApproxStats {
        num_rows: left_stats.approx_stats.num_rows * right_stats.approx_stats.num_rows,
        size_bytes: left_stats.approx_stats.size_bytes + right_stats.approx_stats.size_bytes,
        acc_selectivity: left_stats.approx_stats.acc_selectivity
            * right_stats.approx_stats.acc_selectivity,
    };
    PlanStats::new(approx_stats)
}

pub fn calculate_concat_stats(input_stats: &PlanStats, other_stats: &PlanStats) -> PlanStats {
    // TODO(desmond): We can do better estimations with the projection schema. For now, reuse the old logic.
    let approx_stats = &input_stats.approx_stats + &other_stats.approx_stats;
    PlanStats::new(approx_stats)
}

pub fn calculate_unpivot_stats(input_stats: &PlanStats, num_values: usize) -> PlanStats {
    let approx_stats = ApproxStats {
        num_rows: input_stats.approx_stats.num_rows * num_values,
        size_bytes: input_stats.approx_stats.size_bytes,
        acc_selectivity: input_stats.approx_stats.acc_selectivity * num_values as f64,
    };
    PlanStats::new(approx_stats)
}

pub fn calculate_window_stats(input_stats: &PlanStats) -> PlanStats {
    // For now, just use the input's stats as an approximation
    input_stats.clone()
}

pub fn calculate_repartition_stats(input_stats: &PlanStats) -> PlanStats {
    // Repartitioning does not affect cardinality.
    input_stats.clone()
}

pub fn calculate_write_stats(input_stats: &PlanStats) -> PlanStats {
    // Writing does not affect cardinality.
    input_stats.clone()
}
