use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::UInt64Array;
use daft_micropartition::MicroPartition;
use daft_recordbatch::{GrowableRecordBatch, ProbeState};

use crate::join::hash_join::HashJoinParams;

const DEFAULT_GROWABLE_SIZE: usize = 20;
const MIN_MATCHES_FOR_VECTORIZED_TAKE: usize = 1024;
const MIN_FANOUT_FOR_VECTORIZED_TAKE: usize = 4;
const MIN_AVG_RUN_LEN_FOR_VECTORIZED_TAKE: usize = 8;

type BuildMatch = (u64, usize, u64);

struct MatchStats {
    matched_probe_rows: usize,
    build_table_runs: usize,
}

pub(crate) fn probe_inner(
    input: &MicroPartition,
    probe_state: &ProbeState,
    params: &HashJoinParams,
) -> DaftResult<MicroPartition> {
    let build_side_tables = probe_state.get_record_batches().iter().collect::<Vec<_>>();

    let input_tables = input.record_batches();
    let result_tables = input_tables
        .iter()
        .map(|input_table| {
            let join_keys = input_table.eval_expression_list(&params.probe_on)?;
            let idx_iter = probe_state.probe_indices(join_keys)?;
            let mut matches = Vec::new();
            let mut matched_probe_rows = 0;
            let mut build_table_runs = 0;
            let mut previous_build_table = None;
            for (probe_row_idx, inner_iter) in idx_iter.enumerate() {
                let probe_matches_start = matches.len();
                if let Some(inner_iter) = inner_iter {
                    for (build_rb_idx, build_row_idx) in inner_iter {
                        let build_rb_idx = build_rb_idx as usize;
                        if previous_build_table != Some(build_rb_idx) {
                            build_table_runs += 1;
                            previous_build_table = Some(build_rb_idx);
                        }
                        matches.push((probe_row_idx as u64, build_rb_idx, build_row_idx));
                    }
                }
                if matches.len() > probe_matches_start {
                    matched_probe_rows += 1;
                }
            }
            let match_stats = MatchStats {
                matched_probe_rows,
                build_table_runs,
            };

            let build_side_table =
                build_side_for_inner_matches(&build_side_tables, &matches, &match_stats)?;
            let probe_side_table = {
                let indices_arr = UInt64Array::from_vec(
                    "",
                    matches
                        .iter()
                        .map(|(probe_row_idx, _, _)| *probe_row_idx)
                        .collect::<Vec<_>>(),
                );
                input_table.take(&indices_arr)?
            };

            let (left_table, right_table) = if params.build_on_left {
                (build_side_table, probe_side_table)
            } else {
                (probe_side_table, build_side_table)
            };

            let common_join_keys: Vec<String> = params.common_join_cols.iter().cloned().collect();
            let left_non_join_columns: Vec<String> = params
                .left_schema
                .field_names()
                .filter(|c| !params.common_join_cols.contains(*c))
                .map(ToString::to_string)
                .collect();
            let right_non_join_columns: Vec<String> = params
                .right_schema
                .field_names()
                .filter(|c| !params.common_join_cols.contains(*c))
                .map(ToString::to_string)
                .collect();

            let join_keys_table =
                daft_recordbatch::get_columns_by_name(&left_table, &common_join_keys)?;
            let left_non_join_columns =
                daft_recordbatch::get_columns_by_name(&left_table, &left_non_join_columns)?;
            let right_non_join_columns =
                daft_recordbatch::get_columns_by_name(&right_table, &right_non_join_columns)?;
            let final_table = join_keys_table
                .union(&left_non_join_columns)?
                .union(&right_non_join_columns)?;
            Ok(final_table)
        })
        .collect::<DaftResult<Vec<_>>>()?;

    Ok(MicroPartition::new_loaded(
        params.output_schema.clone(),
        Arc::new(result_tables),
        None,
    ))
}

fn build_side_for_inner_matches(
    build_side_tables: &[&daft_recordbatch::RecordBatch],
    matches: &[BuildMatch],
    match_stats: &MatchStats,
) -> DaftResult<daft_recordbatch::RecordBatch> {
    if should_use_vectorized_take(matches, match_stats) {
        build_side_with_vectorized_take(build_side_tables, matches, match_stats)
    } else {
        build_side_with_growable(build_side_tables, matches)
    }
}

fn should_use_vectorized_take(matches: &[BuildMatch], match_stats: &MatchStats) -> bool {
    if matches.len() < MIN_MATCHES_FOR_VECTORIZED_TAKE || match_stats.matched_probe_rows == 0 {
        return false;
    }

    if matches.len()
        < match_stats
            .matched_probe_rows
            .saturating_mul(MIN_FANOUT_FOR_VECTORIZED_TAKE)
    {
        return false;
    }

    matches.len()
        >= match_stats
            .build_table_runs
            .saturating_mul(MIN_AVG_RUN_LEN_FOR_VECTORIZED_TAKE)
}

fn build_side_with_growable(
    build_side_tables: &[&daft_recordbatch::RecordBatch],
    matches: &[BuildMatch],
) -> DaftResult<daft_recordbatch::RecordBatch> {
    let mut build_side_growable =
        GrowableRecordBatch::new(build_side_tables, false, DEFAULT_GROWABLE_SIZE)?;

    for (_, build_rb_idx, build_row_idx) in matches {
        build_side_growable.extend(*build_rb_idx, *build_row_idx as usize, 1);
    }

    build_side_growable.build()
}

fn build_side_with_vectorized_take(
    build_side_tables: &[&daft_recordbatch::RecordBatch],
    matches: &[BuildMatch],
    match_stats: &MatchStats,
) -> DaftResult<daft_recordbatch::RecordBatch> {
    if matches.is_empty() {
        return build_side_with_growable(build_side_tables, matches);
    }

    let mut taken_tables = Vec::with_capacity(match_stats.build_table_runs);
    let mut current_build_table = matches[0].1;
    let mut run_row_idxs = Vec::new();

    for (_, build_rb_idx, build_row_idx) in matches {
        if *build_rb_idx != current_build_table {
            push_taken_run(
                build_side_tables,
                &mut taken_tables,
                current_build_table,
                &mut run_row_idxs,
            )?;
            current_build_table = *build_rb_idx;
        }
        run_row_idxs.push(*build_row_idx);
    }
    push_taken_run(
        build_side_tables,
        &mut taken_tables,
        current_build_table,
        &mut run_row_idxs,
    )?;

    daft_recordbatch::RecordBatch::concat(taken_tables)
}

fn push_taken_run(
    build_side_tables: &[&daft_recordbatch::RecordBatch],
    taken_tables: &mut Vec<daft_recordbatch::RecordBatch>,
    build_table_idx: usize,
    run_row_idxs: &mut Vec<u64>,
) -> DaftResult<()> {
    let indices_arr = UInt64Array::from_vec("", std::mem::take(run_row_idxs));
    taken_tables.push(build_side_tables[build_table_idx].take(&indices_arr)?);
    Ok(())
}
