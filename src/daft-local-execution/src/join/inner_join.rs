use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::UInt64Array;
use daft_micropartition::MicroPartition;
use daft_recordbatch::{GrowableRecordBatch, ProbeState};

use crate::join::hash_join::HashJoinParams;

pub(crate) fn probe_inner(
    input: &Arc<MicroPartition>,
    probe_state: &ProbeState,
    params: &HashJoinParams,
) -> DaftResult<Arc<MicroPartition>> {
    let build_side_tables = probe_state.get_record_batches().iter().collect::<Vec<_>>();
    const DEFAULT_GROWABLE_SIZE: usize = 20;

    let input_tables = input.record_batches();
    let result_tables = input_tables
        .iter()
        .map(|input_table| {
            let mut build_side_growable =
                GrowableRecordBatch::new(&build_side_tables, false, DEFAULT_GROWABLE_SIZE)?;
            let mut probe_side_idxs = Vec::new();

            let join_keys = input_table.eval_expression_list(&params.probe_on)?;
            let idx_iter = probe_state.probe_indices(&join_keys)?;
            for (probe_row_idx, inner_iter) in idx_iter.enumerate() {
                if let Some(inner_iter) = inner_iter {
                    for (build_rb_idx, build_row_idx) in inner_iter {
                        build_side_growable.extend(
                            build_rb_idx as usize,
                            build_row_idx as usize,
                            1,
                        );
                        probe_side_idxs.push(probe_row_idx as u64);
                    }
                }
            }

            let build_side_table = build_side_growable.build()?;
            let probe_side_table = {
                let indices_arr = UInt64Array::from(("", probe_side_idxs));
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

    Ok(Arc::new(MicroPartition::new_loaded(
        params.output_schema.clone(),
        Arc::new(result_tables),
        None,
    )))
}
