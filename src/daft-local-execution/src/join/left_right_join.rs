use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::{Schema, Series, UInt64Array};
use daft_logical_plan::JoinType;
use daft_micropartition::MicroPartition;
use daft_recordbatch::{GrowableRecordBatch, ProbeState, RecordBatch, get_columns_by_name};

use crate::join::{
    hash_join::{HashJoinParams, HashJoinProbeState},
    index_bitmap::IndexBitmapBuilder,
    outer_join::merge_bitmaps_and_construct_null_table,
};

pub(crate) fn probe_left_right_with_bitmap(
    input: &Arc<MicroPartition>,
    bitmap_builder: &mut IndexBitmapBuilder,
    probe_state: &ProbeState,
    params: &HashJoinParams,
) -> DaftResult<Arc<MicroPartition>> {
    let build_side_tables = probe_state.get_record_batches().iter().collect::<Vec<_>>();

    let final_tables = input
        .record_batches()
        .iter()
        .map(|input_table| {
            let mut build_side_growable = GrowableRecordBatch::new(
                &build_side_tables,
                false,
                build_side_tables.iter().map(|table| table.len()).sum(),
            )?;
            let mut probe_side_idxs = Vec::new();

            let join_keys = input_table.eval_expression_list(&params.probe_on)?;
            let idx_iter = probe_state.probe_indices(&join_keys)?;

            for (probe_row_idx, inner_iter) in idx_iter.enumerate() {
                if let Some(inner_iter) = inner_iter {
                    for (build_table_idx, build_row_idx) in inner_iter {
                        bitmap_builder.mark_used(build_table_idx as usize, build_row_idx as usize);
                        build_side_growable.extend(
                            build_table_idx as usize,
                            build_row_idx as usize,
                            1,
                        );
                        probe_side_idxs.push(probe_row_idx as u64);
                    }
                }
            }

            let build_side_table = build_side_growable.build()?;
            let probe_side_table = {
                let indices_arr = UInt64Array::from_vec("", probe_side_idxs);
                input_table.take(&indices_arr)?
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

            let final_table = if params.join_type == JoinType::Left {
                let join_table = get_columns_by_name(&build_side_table, &common_join_keys)?;
                let left = get_columns_by_name(&build_side_table, &left_non_join_columns)?;
                let right = get_columns_by_name(&probe_side_table, &right_non_join_columns)?;
                join_table.union(&left)?.union(&right)?
            } else {
                let join_table = get_columns_by_name(&build_side_table, &common_join_keys)?;
                let left = get_columns_by_name(&probe_side_table, &left_non_join_columns)?;
                let right = get_columns_by_name(&build_side_table, &right_non_join_columns)?;
                join_table.union(&left)?.union(&right)?
            };
            Ok(final_table)
        })
        .collect::<DaftResult<Vec<_>>>()?;

    Ok(Arc::new(MicroPartition::new_loaded(
        params.output_schema.clone(),
        Arc::new(final_tables),
        None,
    )))
}

pub(crate) fn probe_left_right(
    input: &Arc<MicroPartition>,
    probe_state: &ProbeState,
    params: &HashJoinParams,
) -> DaftResult<Arc<MicroPartition>> {
    let build_side_tables = probe_state.get_record_batches().iter().collect::<Vec<_>>();

    let final_tables = input
        .record_batches()
        .iter()
        .map(|input_table| {
            let mut build_side_growable = GrowableRecordBatch::new(
                &build_side_tables,
                true,
                build_side_tables.iter().map(|table| table.len()).sum(),
            )?;
            let mut probe_side_idxs = Vec::with_capacity(input_table.len());

            let join_keys = input_table.eval_expression_list(&params.probe_on)?;
            let idx_iter = probe_state.probe_indices(&join_keys)?;
            for (probe_row_idx, inner_iter) in idx_iter.enumerate() {
                if let Some(inner_iter) = inner_iter {
                    for (build_table_idx, build_row_idx) in inner_iter {
                        build_side_growable.extend(
                            build_table_idx as usize,
                            build_row_idx as usize,
                            1,
                        );
                        probe_side_idxs.push(probe_row_idx as u64);
                    }
                } else {
                    // if there's no match, we should still emit the probe side and fill the build side with nulls
                    build_side_growable.add_nulls(1);
                    probe_side_idxs.push(probe_row_idx as u64);
                }
            }

            let build_side_table = build_side_growable.build()?;

            let probe_side_table = {
                let indices_arr = UInt64Array::from_vec("", probe_side_idxs);
                input_table.take(&indices_arr)?
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

            let final_table = if params.join_type == JoinType::Left {
                let join_table = get_columns_by_name(&probe_side_table, &common_join_keys)?;
                let left = get_columns_by_name(&probe_side_table, &left_non_join_columns)?;
                let right = get_columns_by_name(&build_side_table, &right_non_join_columns)?;
                join_table.union(&left)?.union(&right)?
            } else {
                let join_table = get_columns_by_name(&probe_side_table, &common_join_keys)?;
                let left = get_columns_by_name(&build_side_table, &left_non_join_columns)?;
                let right = get_columns_by_name(&probe_side_table, &right_non_join_columns)?;
                join_table.union(&left)?.union(&right)?
            };
            Ok(final_table)
        })
        .collect::<DaftResult<Vec<_>>>()?;

    Ok(Arc::new(MicroPartition::new_loaded(
        params.output_schema.clone(),
        Arc::new(final_tables),
        None,
    )))
}

pub(crate) async fn finalize_left(
    states: Vec<HashJoinProbeState>,
    params: &HashJoinParams,
) -> DaftResult<Option<Arc<MicroPartition>>> {
    let build_side_table = merge_bitmaps_and_construct_null_table(states).await?;

    // If build_side_table is empty, return empty result with correct schema
    if build_side_table.is_empty() {
        return Ok(Some(Arc::new(MicroPartition::empty(Some(
            params.output_schema.clone(),
        )))));
    }

    let common_join_cols: Vec<String> = params.common_join_cols.iter().cloned().collect();
    let left_non_join_columns: Vec<String> = params
        .left_schema
        .field_names()
        .filter(|c| !params.common_join_cols.contains(*c))
        .map(ToString::to_string)
        .collect();
    let right_non_join_schema = Arc::new(Schema::new(
        params
            .right_schema
            .fields()
            .iter()
            .filter(|f| !params.common_join_cols.contains(&f.name))
            .cloned(),
    ));

    // For left join, we only finalize when build_on_left is true (needs_bitmap check ensures this)
    // So build_side_table has left columns
    let join_table = get_columns_by_name(&build_side_table, &common_join_cols)?;
    let left = get_columns_by_name(&build_side_table, &left_non_join_columns)?;
    let right = {
        let columns = right_non_join_schema
            .fields()
            .iter()
            .map(|field| Series::full_null(&field.name, &field.dtype, left.len()))
            .collect::<Vec<_>>();
        RecordBatch::new_unchecked(right_non_join_schema, columns, left.len())
    };
    let final_table = join_table.union(&left)?.union(&right)?;
    Ok(Some(Arc::new(MicroPartition::new_loaded(
        final_table.schema.clone(),
        Arc::new(vec![final_table]),
        None,
    ))))
}

pub(crate) async fn finalize_right(
    states: Vec<HashJoinProbeState>,
    params: &HashJoinParams,
) -> DaftResult<Option<Arc<MicroPartition>>> {
    let build_side_table = merge_bitmaps_and_construct_null_table(states).await?;

    // If build_side_table is empty, return empty result with correct schema
    if build_side_table.is_empty() {
        return Ok(Some(Arc::new(MicroPartition::empty(Some(
            params.output_schema.clone(),
        )))));
    }

    let common_join_cols: Vec<String> = params.common_join_cols.iter().cloned().collect();
    let right_non_join_columns: Vec<String> = params
        .right_schema
        .field_names()
        .filter(|c| !params.common_join_cols.contains(*c))
        .map(ToString::to_string)
        .collect();
    let left_non_join_schema = Arc::new(Schema::new(
        params
            .left_schema
            .fields()
            .iter()
            .filter(|f| !params.common_join_cols.contains(&f.name))
            .cloned(),
    ));

    // For right join, we only finalize when build_on_left is false (needs_bitmap check ensures this)
    // So build_side_table has right columns
    let join_table = get_columns_by_name(&build_side_table, &common_join_cols)?;
    let left = {
        let columns = left_non_join_schema
            .fields()
            .iter()
            .map(|field| Series::full_null(&field.name, &field.dtype, build_side_table.len()))
            .collect::<Vec<_>>();
        RecordBatch::new_unchecked(left_non_join_schema, columns, build_side_table.len())
    };
    let right = get_columns_by_name(&build_side_table, &right_non_join_columns)?;
    let final_table = join_table.union(&left)?.union(&right)?;
    Ok(Some(Arc::new(MicroPartition::new_loaded(
        final_table.schema.clone(),
        Arc::new(vec![final_table]),
        None,
    ))))
}
